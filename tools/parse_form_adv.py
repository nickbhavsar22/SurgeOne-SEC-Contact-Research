"""
Tool: EDGAR CCO Extractor

Extracts Chief Compliance Officer name and phone from SEC EDGAR filings.
Form ADV PDFs at reports.adviserinfo.sec.gov are template-only (filled values
are not in the PDF text). Instead, this tool queries EDGAR full-text search
for structured filings (13F-HR, Form D) that list the CCO in their signature
blocks.

Approach:
  1. Search EDGAR EFTS for the firm name + "compliance officer"
  2. Prefer 13F-HR and Form D filings (structured XML with signature blocks)
  3. Extract CCO name, title, and phone from XML tags
  4. Store in form_adv_details via upsert_form_adv()

Expected hit rate: ~35% of firms (those with 13F-HR or Form D filings).
Rate-limited to ~2 requests/second per SEC EDGAR fair-use policy.
"""

import re
import time

import requests

from tools.cache_db import (
    init_db, upsert_form_adv, get_form_adv, get_stale_form_adv_crds,
    log_enrichment,
)

EDGAR_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"

HEADERS = {
    'User-Agent': 'Bhavsar-Growth/1.0 sec-research@bhavsargrowth.com',
    'Accept': 'application/json, application/xml, */*',
}

# SEC EDGAR fair-use: max 10 req/s. We stay well below that.
REQUEST_DELAY = 0.5

# Filing types to try, in order of reliability for CCO extraction
PREFERRED_FORMS = ['13F-HR', 'D']

# Words that are titles, not names — used to filter bad extractions
_TITLE_WORDS = frozenset([
    'vice', 'president', 'director', 'officer', 'manager', 'counsel',
    'secretary', 'treasurer', 'partner', 'principal', 'general',
    'assistant', 'senior', 'junior', 'executive', 'managing',
])


def _clean_company_name(company):
    """Strip corporate suffixes for better EDGAR search matching."""
    name = company.split(',')[0].strip()
    name = re.sub(
        r'\b(LLC|INC|LP|LLP|CORP|LTD|CO|COMPANY|ASSOCIATES|ADVISORS|'
        r'ADVISERS|MANAGEMENT|PARTNERS|WEALTH|CAPITAL|FINANCIAL|'
        r'INVESTMENT|INVESTMENTS|GROUP|SERVICES|CONSULTING)\b',
        '', name, flags=re.IGNORECASE,
    ).strip()
    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:40]


def _is_valid_person_name(name):
    """Check if extracted text looks like a real person name."""
    if not name:
        return False
    words = name.split()
    if len(words) < 2 or len(words) > 5:
        return False
    # At least one word should be capitalized and not a title word
    has_name_word = any(
        w[0].isupper() and w.lower() not in _TITLE_WORDS
        for w in words if len(w) > 1
    )
    if not has_name_word:
        return False
    # Should not be all-caps corporate names
    if name == name.upper() and len(name) > 10:
        return False
    return True


def _extract_cco_from_13f_xml(text):
    """Extract CCO from 13F-HR XML signature block."""
    sig_match = re.search(
        r'<signatureBlock>\s*'
        r'<name>([^<]+)</name>\s*'
        r'<title>([^<]+)</title>\s*'
        r'(?:<phone>([^<]*)</phone>)?',
        text, re.DOTALL,
    )
    if not sig_match:
        return None
    title = sig_match.group(2).strip()
    if 'compliance' not in title.lower():
        return None
    name = sig_match.group(1).strip()
    if not _is_valid_person_name(name):
        return None
    phone = sig_match.group(3).strip() if sig_match.group(3) else None
    return {
        'cco_name': name,
        'cco_title': title,
        'cco_phone': _format_phone(phone),
    }


def _extract_cco_from_form_d_xml(text):
    """Extract CCO from Form D XML signature block."""
    sigs = re.findall(
        r'<nameOfSigner>([^<]+)</nameOfSigner>\s*'
        r'.*?'
        r'<signatureTitle>([^<]+)</signatureTitle>',
        text, re.DOTALL,
    )
    for raw_name, title in sigs:
        if 'compliance' not in title.lower():
            continue
        name = raw_name.replace('/s/', '').strip().strip('/')
        if _is_valid_person_name(name):
            return {
                'cco_name': name,
                'cco_title': title.strip(),
                'cco_phone': None,
            }
    return None


def _format_phone(phone):
    """Format a raw phone string into standard format."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == '1':
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return phone.strip() if phone.strip() else None


def _fetch_filing(filing_id, ciks):
    """Fetch a filing from EDGAR Archives by its EFTS ID."""
    if ':' not in filing_id or not ciks:
        return None
    accession, filename = filing_id.split(':', 1)
    acc_clean = accession.replace('-', '')
    cik = ciks[0]
    url = f"{EDGAR_ARCHIVES_URL}/{cik}/{acc_clean}/{filename}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.text
    except requests.RequestException:
        pass
    return None


def search_edgar_cco(company_name):
    """Search EDGAR for a firm's CCO using EFTS full-text search.

    Searches for the company name + "compliance officer" in 13F-HR and
    Form D filings. Returns dict with cco_name/cco_title/cco_phone or None.
    """
    clean_name = _clean_company_name(company_name)
    if not clean_name or len(clean_name) < 3:
        return None

    for form_type in PREFERRED_FORMS:
        query = f'"{clean_name}" "compliance officer"'
        params = {
            'q': query,
            'forms': form_type,
            'dateRange': 'custom',
            'startdt': '2020-01-01',
            'enddt': '2026-12-31',
            '_source': 'ciks,root_forms,file_date',
            'sort': 'file_date:desc',
        }
        try:
            resp = requests.get(
                EDGAR_EFTS_URL, params=params,
                headers=HEADERS, timeout=15,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])
            if not hits:
                continue

            # Try the most recent filing first
            for hit in hits[:3]:
                filing_id = hit['_id']
                ciks = hit['_source'].get('ciks', [])
                filing_text = _fetch_filing(filing_id, ciks)
                if not filing_text:
                    time.sleep(REQUEST_DELAY)
                    continue

                if form_type == '13F-HR':
                    result = _extract_cco_from_13f_xml(filing_text)
                elif form_type == 'D':
                    result = _extract_cco_from_form_d_xml(filing_text)
                else:
                    result = None

                if result:
                    result['source'] = f'edgar_{form_type}'
                    result['filing_id'] = filing_id
                    return result

                time.sleep(REQUEST_DELAY)

        except (requests.RequestException, ValueError, KeyError):
            continue

        time.sleep(REQUEST_DELAY)

    return None


def extract_cco(crd, company_name, db_path=None):
    """Extract CCO for a single firm and store in database.

    Preserves existing state_registrations data from prior IAPD queries.
    Returns the CCO result dict or None.
    """
    init_db(db_path)

    result = search_edgar_cco(company_name)
    if not result:
        log_enrichment(
            crd, 'edgar', 'efts_search', 0, 'no_result',
            db_path=db_path,
        )
        return None

    # Merge with existing form_adv_details to preserve state registrations
    existing = get_form_adv(crd, db_path=db_path)
    details = {
        'cco_name': result['cco_name'],
        'cco_email': None,  # EDGAR doesn't have email
        'cco_phone': result.get('cco_phone'),
        'state_registrations': None,
        'state_count': 0,
        'aum_breakdown': None,
    }
    if existing:
        details['state_registrations'] = existing.get('state_registrations')
        details['state_count'] = existing.get('state_count', 0)
        details['aum_breakdown'] = existing.get('aum_breakdown')

    upsert_form_adv(crd, details, db_path=db_path)
    log_enrichment(
        crd, 'edgar', f'efts/{result.get("source", "")}',
        200, 'success', db_path=db_path,
    )
    return result


def extract_cco_batch(crd_company_list, max_age_days=30, db_path=None,
                      progress_callback=None):
    """Extract CCO for a batch of firms, skipping recently extracted ones.

    Args:
        crd_company_list: list of (crd, company_name) tuples
        max_age_days: skip firms with CCO data fresher than this
        db_path: database path
        progress_callback: callable(current, total, results_dict)

    Returns dict: {extracted, cached, no_result, errors}
    """
    init_db(db_path)

    all_crds = [c[0] for c in crd_company_list]
    stale_crds = set(_get_stale_cco_crds(all_crds, max_age_days, db_path))
    crd_to_company = {c[0]: c[1] for c in crd_company_list}

    results = {'extracted': 0, 'cached': 0, 'no_result': 0, 'errors': 0}
    total = len(crd_company_list)

    for i, (crd, company) in enumerate(crd_company_list):
        if crd not in stale_crds:
            results['cached'] += 1
        else:
            try:
                cco = extract_cco(crd, company, db_path=db_path)
                if cco:
                    results['extracted'] += 1
                else:
                    results['no_result'] += 1
            except Exception:
                results['errors'] += 1

        if progress_callback:
            progress_callback(i + 1, total, results)

    return results


def _get_stale_cco_crds(crd_list, max_age_days, db_path):
    """Return CRDs that have no CCO data or stale CCO data."""
    from datetime import datetime, timedelta
    if not crd_list:
        return []

    from tools.cache_db import get_connection
    conn = get_connection(db_path)
    cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
    try:
        stale = []
        for i in range(0, len(crd_list), 500):
            chunk = crd_list[i:i + 500]
            placeholders = ','.join('?' * len(chunk))
            rows = conn.execute(f"""
                SELECT crd, cco_name, scraped_at FROM form_adv_details
                WHERE crd IN ({placeholders})
            """, chunk).fetchall()
            fresh = set()
            for row in rows:
                # Fresh only if CCO name is populated AND recent
                if row['cco_name'] and row['scraped_at'] and row['scraped_at'] > cutoff:
                    fresh.add(row['crd'])
            stale.extend(c for c in chunk if c not in fresh)
        return stale
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        crd = int(sys.argv[1])
        company = sys.argv[2]
        result = search_edgar_cco(company)
        if result:
            print(f"CRD {crd}: CCO={result['cco_name']} "
                  f"Title={result.get('cco_title', '')} "
                  f"Phone={result.get('cco_phone', '')} "
                  f"Source={result.get('source', '')}")
        else:
            print(f"CRD {crd}: No CCO found in EDGAR for '{company}'")
    else:
        print("Usage: python tools/parse_form_adv.py <CRD> <COMPANY_NAME>")
