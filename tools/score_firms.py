"""
Tool 4: ICP Fit Scoring for SurgeOne.ai

Scores each firm on how well they match SurgeOne.ai's ideal customer profile:
  - AUM tier, employee count, client count
  - Multi-state registration (4+ states = strong signal)
  - CCO is "in name only" (managing principal as CCO)
  - Website compliance/advisory keywords
"""

import re
import requests
from bs4 import BeautifulSoup

from tools.cache_db import (
    init_db, get_firm_by_crd, get_form_adv, update_firm_score,
)

# Top financial states
TOP_STATES = {'NY', 'CA', 'TX', 'FL', 'CT', 'MA', 'IL', 'NJ', 'PA', 'CO'}

# Advisory/wealth name keywords
NAME_ADVISORY_KW = [
    'advisory', 'advisors', 'wealth', 'financial', 'capital',
    'investment', 'asset', 'fiduciary',
]
NAME_SCALE_KW = [
    'group', 'partners', 'associates', 'management', 'global',
]

# Website keyword categories and their point values
WEBSITE_KEYWORDS = {
    'compliance': {
        'keywords': ['compliance', 'regulatory', 'fiduciary', 'sec registered',
                     'form adv', 'disclosure', 'audit', 'examination'],
        'points': 14,
    },
    'advisory': {
        'keywords': ['wealth management', 'financial planning', 'investment advisory',
                     'portfolio management', 'asset management', 'retirement planning',
                     'estate planning', 'tax planning'],
        'points': 12,
    },
    'cybersecurity': {
        'keywords': ['cybersecurity', 'data protection', 'privacy',
                     'information security', 'data management', 'secure', 'encryption'],
        'points': 11,
    },
    'team': {
        'keywords': ['our team', 'meet the team', 'our advisors', 'leadership',
                     'managing director', 'vice president', 'partner', 'staff'],
        'points': 10,
    },
    'clients': {
        'keywords': ['assets under management', 'aum', 'clients', 'high net worth',
                     'institutional', 'individuals', 'families'],
        'points': 10,
    },
    'technology': {
        'keywords': ['technology', 'digital', 'platform', 'portal',
                     'fintech', 'innovation', 'automated', 'online'],
        'points': 8,
    },
}

SCRAPE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}


def score_firm(crd, deep=False, db_path=None):
    """Score a firm against SurgeOne.ai's ICP.

    Args:
        crd: Firm CRD number
        deep: If True, fetch and analyze the firm's website (slower, uses HTTP)
        db_path: Optional database path

    Returns: (score: float 0-100, reasons: list of strings)
    """
    init_db(db_path)
    firm = get_firm_by_crd(crd, db_path=db_path)
    if not firm:
        return 0, ['Firm not found']

    form_adv = get_form_adv(crd, db_path=db_path)

    data_score, data_reasons = _score_data(firm, form_adv)
    web_score, web_reasons = 0, []

    if deep and firm.get('website'):
        web_score, web_reasons = _score_website(firm['website'])

    # Combine: data max 50, website max 75 → normalize to 0-100
    max_possible = 50 + (75 if deep else 0)
    raw = data_score + web_score
    normalized = min(100, round((raw / max_possible) * 100, 1)) if max_possible > 0 else 0

    all_reasons = data_reasons + web_reasons
    reasons_str = '; '.join(all_reasons)

    update_firm_score(crd, normalized, reasons_str, db_path=db_path)

    return normalized, all_reasons


def score_batch(crd_list, deep=False, db_path=None):
    """Score a batch of firms. Returns summary stats."""
    init_db(db_path)
    results = {'scored': 0, 'errors': 0}

    for crd in crd_list:
        try:
            score_firm(crd, deep=deep, db_path=db_path)
            results['scored'] += 1
        except Exception:
            results['errors'] += 1

    return results


def _score_data(firm, form_adv):
    """Score based on SEC data fields. Max 50 points."""
    score = 0
    reasons = []

    # Website presence (8 pts)
    if firm.get('website'):
        score += 8
        reasons.append('Has website (+8)')

    # Phone presence (3 pts)
    if firm.get('phone'):
        score += 3
        reasons.append('Has phone (+3)')

    # Company name keywords
    name = (firm.get('company') or '').lower()
    if any(kw in name for kw in NAME_ADVISORY_KW):
        score += 6
        reasons.append('Advisory/wealth name (+6)')
    if any(kw in name for kw in NAME_SCALE_KW):
        score += 4
        reasons.append('Scale/team name (+4)')

    # Top financial state (4 pts)
    state = (firm.get('state') or '').upper().strip()
    if state in TOP_STATES:
        score += 4
        reasons.append(f'Top financial state: {state} (+4)')

    # Employee tiers
    employees = firm.get('employees') or 0
    if employees >= 10:
        score += 10
        reasons.append(f'Employees: {employees} (+10)')
    elif employees >= 3:
        score += 6
        reasons.append(f'Employees: {employees} (+6)')
    elif employees >= 1:
        score += 2
        reasons.append(f'Employees: {employees} (+2)')

    # AUM tiers
    aum = firm.get('aum') or 0
    if aum >= 1_000_000_000:
        score += 10
        reasons.append(f'AUM ≥$1B (+10)')
    elif aum >= 100_000_000:
        score += 8
        reasons.append(f'AUM ≥$100M (+8)')
    elif aum >= 10_000_000:
        score += 5
        reasons.append(f'AUM ≥$10M (+5)')
    elif aum > 0:
        score += 2
        reasons.append(f'AUM >$0 (+2)')

    # Client tiers
    clients = firm.get('clients') or 0
    if clients >= 100:
        score += 5
        reasons.append(f'Clients ≥100 (+5)')
    elif clients >= 10:
        score += 3
        reasons.append(f'Clients ≥10 (+3)')
    elif clients > 0:
        score += 1
        reasons.append(f'Clients >0 (+1)')

    # Multi-state registration bonus (from Form ADV)
    if form_adv:
        state_count = form_adv.get('state_count') or 0
        if state_count >= 4:
            score += 5  # Bonus: exceeds data max slightly for strong signal
            reasons.append(f'Multi-state registration: {state_count} states (+5)')

    return min(score, 50), reasons


def _score_website(url):
    """Score based on website content analysis. Max 75 points."""
    score = 0
    reasons = []

    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15,
                            allow_redirects=True)
        if resp.status_code != 200:
            return 0, [f'Website unreachable (HTTP {resp.status_code})']

        # Site reachable
        score += 5
        reasons.append('Website reachable (+5)')

        soup = BeautifulSoup(resp.text, 'lxml')
        # Remove scripts and styles
        for tag in soup(['script', 'style']):
            tag.decompose()
        text = soup.get_text(separator=' ').lower()

        # Check each keyword category
        for category, config in WEBSITE_KEYWORDS.items():
            matches = [kw for kw in config['keywords'] if kw in text]
            if matches:
                score += config['points']
                reasons.append(f'{category.title()} keywords: {", ".join(matches[:3])} (+{config["points"]})')

    except requests.RequestException as e:
        return 0, [f'Website error: {str(e)[:50]}']

    return min(score, 75), reasons


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        deep = '--deep' in sys.argv
        score, reasons = score_firm(crd, deep=deep)
        print(f"CRD {crd}: Score={score}, Reasons={reasons}")
    else:
        print("Usage: python tools/score_firms.py <CRD_NUMBER> [--deep]")
