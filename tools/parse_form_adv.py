"""
Tool: Form ADV PDF Contact Extractor

Downloads Form ADV PDFs from IAPD and extracts ALL contact names and titles
using pdfplumber text extraction + regex patterns.

Approach:
  1. Download PDF from reports.adviserinfo.sec.gov/reports/ADV/{CRD}/PDF/{CRD}.pdf
  2. Extract text from first 15 pages using pdfplumber
  3. Apply regex patterns for Principal/Owner, CCO, and other contacts
  4. Return all contacts found (not just the best one)

Rate-limited to 1 request/second to be respectful to SEC servers.
"""

import io
import re
import time
import logging

import requests
import pdfplumber

from tools.cache_db import (
    init_db, upsert_form_adv, insert_contact, delete_contacts_for_firm,
    get_unprocessed_crds, log_enrichment,
)

logger = logging.getLogger(__name__)

PDF_URL_TEMPLATE = "https://reports.adviserinfo.sec.gov/reports/ADV/{crd}/PDF/{crd}.pdf"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

# Rate limit: 1 request/second for SEC servers
REQUEST_DELAY = 1.0

# Corporate suffixes that indicate a company name, not a person
_CORP_SUFFIXES = ['LLC', 'INC', 'LTD', 'CORP', 'LP', 'LLP', 'FORTITUDE',
                  'WEALTH', 'CAPITAL', 'MANAGEMENT', 'ADVISORS', 'GROUP',
                  'PARTNERS', 'HOLDINGS', 'FINANCIAL', 'CONSULTING']

# Words that are titles, not names — used to filter bad extractions
_TITLE_WORDS = frozenset([
    'vice', 'president', 'director', 'officer', 'manager', 'counsel',
    'secretary', 'treasurer', 'partner', 'principal', 'general',
    'assistant', 'senior', 'junior', 'executive', 'managing',
])

# Generic email domains/prefixes to filter
_GENERIC_DOMAINS = {'sec.gov', 'finra.org', 'example.com'}
_GENERIC_PREFIXES = {'info', 'support', 'admin', 'contact', 'compliance',
                     'noreply', 'no-reply', 'reporting'}


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
    # Should not contain corporate suffixes
    if any(corp in name.upper() for corp in _CORP_SUFFIXES):
        return False
    return True


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


def _is_generic_email(email):
    """Check if an email is generic/role-based."""
    if not email:
        return True
    local = email.split('@')[0].lower()
    domain = email.split('@')[-1].lower()
    if domain in _GENERIC_DOMAINS:
        return True
    if local in _GENERIC_PREFIXES:
        return True
    return False


def _extract_phone_near_name(text, name):
    """Try to find a phone number near a person's name in the text."""
    if not name or not text:
        return None
    # Find the name position and look for phone numbers nearby
    name_pos = text.find(name)
    if name_pos == -1:
        return None
    # Search within 500 chars after the name
    nearby = text[name_pos:name_pos + 500]
    phone_match = re.search(
        r'(?:Telephone|Phone|Tel)[:\s]*(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})',
        nearby, re.IGNORECASE
    )
    if phone_match:
        return _format_phone(phone_match.group(1))
    return None


def extract_contacts_from_pdf(crd):
    """Download and parse Form ADV PDF. Returns list of contacts found.

    Each contact is a dict with: {name, title, email, phone, source}
    Returns empty list if PDF not accessible or no contacts found.
    """
    pdf_url = PDF_URL_TEMPLATE.format(crd=crd)

    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=45)
        if resp.status_code != 200:
            logger.warning('PDF download failed for CRD %s: HTTP %s', crd, resp.status_code)
            return []

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            text = ''
            for page in pdf.pages[:15]:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + '\n'

        if not text.strip():
            return []

        contacts = []
        seen_names = set()

        # Pattern 1: Principal/Owner — after "your last, first, and middle names"
        legal_match = re.search(
            r'your last, first, and middle names\):\s*'
            r'([A-Z][A-Za-z.\s,\'-]+?)'
            r'(?:\s*B\.\s|\nB\.|\n[A-Z])',
            text
        )
        if legal_match:
            name = legal_match.group(1).strip()
            if _is_valid_person_name(name):
                seen_names.add(name.lower())
                contacts.append({
                    'name': name,
                    'title': 'Principal/Owner',
                    'email': None,
                    'phone': _extract_phone_near_name(text, name),
                    'source': 'pdf_principal',
                })

        # Pattern 2: Chief Compliance Officer — Section J
        cco_match = re.search(
            r'J\.?\s*Chief Compliance Officer[\s\S]*?'
            r'Name:\s*([A-Z][A-Za-z.\s,\'-]+?)'
            r'(?:\s+Other titles|\s+Telephone|\n)',
            text, re.IGNORECASE
        )
        if cco_match:
            cco_name = cco_match.group(1).strip()
            if _is_valid_person_name(cco_name) and cco_name.lower() not in seen_names:
                seen_names.add(cco_name.lower())
                contacts.append({
                    'name': cco_name,
                    'title': 'Chief Compliance Officer',
                    'email': None,
                    'phone': _extract_phone_near_name(text, cco_name),
                    'source': 'pdf_cco',
                })

        # Pattern 3: Other officers/directors — Schedule A items
        # Look for "Name:" followed by a person name and then a title
        schedule_matches = re.findall(
            r'(?:Name|Full Legal Name):\s*([A-Z][A-Za-z.\s,\'-]+?)\s*'
            r'(?:Title|Position):\s*([A-Za-z\s/,]+?)(?:\n|$)',
            text
        )
        for name, title in schedule_matches:
            name = name.strip()
            title = title.strip()
            if (name.lower() not in seen_names
                    and _is_valid_person_name(name)
                    and len(title) < 80):
                seen_names.add(name.lower())
                contacts.append({
                    'name': name,
                    'title': title,
                    'email': None,
                    'phone': None,
                    'source': 'pdf_schedule',
                })

        # Extract all emails from the PDF text
        all_emails = re.findall(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            text
        )
        valid_emails = [e for e in all_emails if not _is_generic_email(e)]

        # Try to assign emails to contacts (first valid email to first contact)
        for i, contact in enumerate(contacts):
            if i < len(valid_emails):
                contact['email'] = valid_emails[i]

        # If there are extra emails not assigned to contacts, they may belong
        # to the already-found contacts. Don't create phantom contacts from
        # emails alone — the user wants names+titles from the PDF.

        return contacts

    except Exception as e:
        logger.error('Failed to extract contacts for CRD %s: %s', crd, e)
        return []


def extract_contacts_batch(crd_list, max_age_days=30, db_path=None,
                           progress_callback=None):
    """Extract contacts from Form ADV PDFs for a batch of firms.

    Skips firms that were processed within max_age_days.
    Stores contacts in the database and marks firms as processed.

    Args:
        crd_list: list of CRD numbers to process
        max_age_days: skip firms processed within this many days
        db_path: database path
        progress_callback: callable(current, total, results_dict)

    Returns dict: {processed, cached, no_contacts, errors, contacts_found}
    """
    init_db(db_path)

    unprocessed = set(get_unprocessed_crds(crd_list, max_age_days, db_path))

    results = {
        'processed': 0, 'cached': 0, 'no_contacts': 0,
        'errors': 0, 'contacts_found': 0,
    }
    total = len(crd_list)

    for i, crd in enumerate(crd_list):
        if crd not in unprocessed:
            results['cached'] += 1
        else:
            try:
                contacts = extract_contacts_from_pdf(crd)

                # Clear old contacts and insert new ones
                delete_contacts_for_firm(crd, db_path=db_path)
                for contact in contacts:
                    insert_contact(crd, {
                        'contact_name': contact['name'],
                        'contact_title': contact.get('title'),
                        'contact_email': contact.get('email'),
                        'contact_phone': contact.get('phone'),
                        'source': contact.get('source', 'pdf'),
                        'confidence': 80.0 if contact.get('email') else 50.0,
                    }, db_path=db_path)

                # Mark firm as processed
                upsert_form_adv(crd, {
                    'cco_name': next(
                        (c['name'] for c in contacts if 'cco' in (c.get('source') or '')),
                        None
                    ),
                    'cco_email': None,
                    'cco_phone': next(
                        (c.get('phone') for c in contacts if 'cco' in (c.get('source') or '')),
                        None
                    ),
                    'state_registrations': None,
                    'state_count': 0,
                    'aum_breakdown': None,
                }, db_path=db_path)

                log_enrichment(
                    crd, 'pdf_extraction', 'form_adv_pdf', 200,
                    'success' if contacts else 'no_result',
                    db_path=db_path,
                )

                if contacts:
                    results['processed'] += 1
                    results['contacts_found'] += len(contacts)
                else:
                    results['no_contacts'] += 1

            except Exception as e:
                logger.error('Error processing CRD %s: %s', crd, e)
                results['errors'] += 1

            time.sleep(REQUEST_DELAY)

        if progress_callback:
            progress_callback(i + 1, total, results)

    return results


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        contacts = extract_contacts_from_pdf(crd)
        if contacts:
            for c in contacts:
                print(f"  {c['title']}: {c['name']} "
                      f"({c.get('email', 'no email')}) "
                      f"[{c.get('source', '')}]")
        else:
            print(f"CRD {crd}: No contacts found in Form ADV PDF")
    else:
        print("Usage: python tools/parse_form_adv.py <CRD>")
