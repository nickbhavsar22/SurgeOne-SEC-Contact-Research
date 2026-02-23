"""
Tool 2: IAPD Form ADV Scraper

Queries adviserinfo.sec.gov for each firm's Form ADV details:
  - CCO name, email, phone (Item 1.J) — rarely available via API
  - State registrations (from registrationStatus)
  - AUM breakdown
Rate-limited to 1 request/second.

Note: IAPD is a JavaScript SPA; HTML scraping returns an empty shell.
The working endpoint is the search API at api.adviserinfo.sec.gov/search/firm/{crd}.
CCO data is not exposed by this API — contact enrichment relies on the
waterfall in enrich_contacts.py (website scraping, Hunter.io).
"""

import json
import re
import time

import requests

from tools.cache_db import (
    init_db, upsert_form_adv, get_stale_form_adv_crds, get_form_adv,
    log_enrichment,
)

# Working IAPD search API endpoint (returns JSON with firm details)
IAPD_SEARCH_API = "https://api.adviserinfo.sec.gov/search/firm/{crd}"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://adviserinfo.sec.gov/',
}

REQUEST_DELAY = 1.0  # seconds between requests


def query_firm_adv(crd):
    """Query IAPD for a firm's Form ADV details.

    Uses the IAPD search API to extract registration status and state data.
    CCO info is rarely available from this endpoint.
    Returns a dict with CCO info, state registrations, and AUM breakdown.
    """
    result = {
        'cco_name': None,
        'cco_email': None,
        'cco_phone': None,
        'state_registrations': None,
        'state_count': 0,
        'aum_breakdown': None,
    }

    api_result = _try_iapd_search_api(crd)
    if api_result:
        result.update(api_result)

    return result


def _try_iapd_search_api(crd):
    """Get firm data from IAPD's search API (the only working endpoint)."""
    try:
        url = IAPD_SEARCH_API.format(crd=crd)
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        result = {}

        hits = data.get('hits', {}).get('hits', [])
        if not hits:
            return None

        # Parse the nested iacontent JSON string
        iacontent_str = hits[0].get('_source', {}).get('iacontent', '{}')
        iacontent = json.loads(iacontent_str)

        # Extract state registrations from registrationStatus
        reg_statuses = iacontent.get('registrationStatus', [])
        notice_filings = iacontent.get('noticeFilings', [])

        # Collect active state registrations (notice filings = active states)
        active_states = []
        for nf in notice_filings:
            jurisdiction = nf.get('jurisdiction', '')
            status = nf.get('status', '')
            if status == 'Notice Filed' and jurisdiction:
                state_abbr = _state_to_abbr(jurisdiction)
                if state_abbr:
                    active_states.append(state_abbr)

        # Also check registrationStatus for state registrations
        for rs in reg_statuses:
            jurisdiction = rs.get('secJurisdiction', '')
            status = rs.get('status', '')
            if jurisdiction != 'SEC' and status in ('Approved', 'Notice Filed'):
                state_abbr = _state_to_abbr(jurisdiction)
                if state_abbr and state_abbr not in active_states:
                    active_states.append(state_abbr)

        if active_states:
            result['state_registrations'] = ','.join(sorted(active_states))
            result['state_count'] = len(active_states)

        return result if result else None
    except (requests.RequestException, json.JSONDecodeError, KeyError):
        return None


STATE_NAME_TO_ABBR = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'district of columbia': 'DC', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI',
    'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
    'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME',
    'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
    'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
    'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM',
    'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH',
    'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI',
    'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX',
    'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
    'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
}

# Also map 2-letter abbreviations to themselves
_ABBR_SET = set(STATE_NAME_TO_ABBR.values())


def _state_to_abbr(name):
    """Convert a state name or abbreviation to a 2-letter abbreviation."""
    if not name:
        return None
    name_upper = name.strip().upper()
    if name_upper in _ABBR_SET:
        return name_upper
    return STATE_NAME_TO_ABBR.get(name.strip().lower())


def query_firms_batch(crd_list, max_age_days=30, db_path=None):
    """Query IAPD for a batch of firms, skipping recently scraped ones.

    Returns dict with counts: {queried, cached, errors}.
    """
    init_db(db_path)

    stale = get_stale_form_adv_crds(crd_list, max_age_days=max_age_days, db_path=db_path)
    cached = len(crd_list) - len(stale)
    queried = 0
    errors = 0

    for crd in stale:
        try:
            details = query_firm_adv(crd)
            upsert_form_adv(crd, details, db_path=db_path)
            log_enrichment(crd, 'iapd', f'/search/firm/{crd}', 200, 'success',
                           db_path=db_path)
            queried += 1
        except Exception as e:
            log_enrichment(crd, 'iapd', f'/search/firm/{crd}', 0,
                           'error', db_path=db_path)
            errors += 1

        time.sleep(REQUEST_DELAY)

    return {'queried': queried, 'cached': cached, 'errors': errors}


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        result = query_firm_adv(crd)
        print(f"CRD {crd}: {result}")
    else:
        print("Usage: python tools/query_iapd.py <CRD_NUMBER>")
