"""
Tool 3: Contact Discovery Waterfall

Multi-source contact enrichment for RIA firms:
  1. Form ADV CCO data (from cache_db)
  2. Hunter.io Domain Search (paid plan)
  3. Website scraping (homepage + subpages)
  4. Hunter.io Email Finder (targeted, if name found but no email)

Every API call is logged for credit tracking.
"""

import os
import re
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from tools.cache_db import (
    init_db, get_form_adv, get_firm_by_crd, upsert_contact, get_contact,
    log_enrichment,
)

load_dotenv()

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY', '')
HUNTER_DOMAIN_SEARCH = 'https://api.hunter.io/v2/domain-search'
HUNTER_EMAIL_FINDER = 'https://api.hunter.io/v2/email-finder'

SCRAPE_SUBPAGES = [
    '/contact', '/contact-us', '/about', '/about-us', '/team',
    '/our-team', '/people', '/leadership', '/staff', '/our-firm',
    '/bio', '/advisors',
]

SCRAPE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

GENERIC_EMAIL_PREFIXES = {
    'info', 'support', 'admin', 'contact', 'sales', 'ir', 'help',
    'office', 'team', 'hello', 'general', 'mail', 'noreply',
    'no-reply', 'webmaster', 'postmaster',
    'reporting', 'information', 'compliance', 'inquiry', 'inquiries',
    'service', 'services', 'billing', 'accounts', 'hr', 'jobs',
    'careers', 'media', 'press', 'marketing', 'newsletter',
    'subscribe', 'feedback', 'operations', 'reception', 'frontdesk',
    'welcome',
}

GENERIC_EMAIL_DOMAINS = {
    'sec.gov', 'finra.org', 'example.com', 'gmail.com', 'yahoo.com',
    'hotmail.com', 'outlook.com', 'aol.com',
}

# Words that appear in scraped text but are NOT person names
NON_PERSON_WORDS = {
    'new york', 'los angeles', 'san francisco', 'san diego', 'san jose',
    'las vegas', 'new jersey', 'new hampshire', 'new mexico',
    'north carolina', 'south carolina', 'north dakota', 'south dakota',
    'west virginia', 'rhode island', 'puerto rico',
    'llc', 'inc', 'corp', 'corporation', 'limited', 'lp', 'llp',
    'management', 'capital', 'advisors', 'advisory', 'wealth',
    'financial', 'investment', 'consulting', 'services', 'partners',
    'associates', 'holdings', 'group', 'solutions',
}

# CCO and senior title keywords for prioritizing contacts
TITLE_PRIORITY = [
    'chief compliance officer', 'cco',
    'principal', 'managing member', 'managing partner', 'founder', 'co-founder',
    'ceo', 'president', 'owner', 'coo', 'cfo',
    'director', 'managing director', 'partner',
    'advisor', 'adviser',
]

SCRAPE_DELAY = 0.3  # seconds between scrape requests

DEFAULT_BATCH_CREDIT_LIMIT = 50  # Max Hunter.io credits per batch run


def enrich_firm(crd, force=False, db_path=None, credit_budget=None):
    """Run the full contact discovery waterfall for a single firm.

    Returns the best contact dict found, or None.
    """
    init_db(db_path)

    # Check cache first (unless forcing refresh)
    if not force:
        existing = get_contact(crd, db_path=db_path)
        if existing and existing.get('contact_email'):
            return existing

    firm = get_firm_by_crd(crd, db_path=db_path)
    if not firm:
        return None

    best_contact = None

    # Step 1: Form ADV CCO data
    adv = get_form_adv(crd, db_path=db_path)
    if adv and adv.get('cco_name'):
        email = adv.get('cco_email')
        if email and _is_generic_email(email):
            email = None
        contact = {
            'contact_name': adv['cco_name'],
            'contact_email': email,
            'contact_title': 'Chief Compliance Officer',
            'contact_phone': adv.get('cco_phone'),
            'source': 'form_adv',
            'confidence': 95.0 if email else 50.0,
        }
        if contact['contact_email']:
            best_contact = contact
        elif not best_contact:
            best_contact = contact

    # Step 2: Hunter.io Domain Search
    website = firm.get('website')
    hunter_allowed = (credit_budget is None or credit_budget['used'] < credit_budget['limit'])
    if HUNTER_API_KEY and website and hunter_allowed:
        domain = _extract_domain(website)
        if domain:
            hunter_result = _hunter_domain_search(crd, domain, db_path=db_path)
            if credit_budget is not None:
                credit_budget['used'] += 1
            if (hunter_result and hunter_result.get('contact_email')
                    and not _is_generic_email(hunter_result['contact_email'])):
                if not best_contact or not best_contact.get('contact_email'):
                    best_contact = hunter_result
                elif hunter_result.get('confidence', 0) > best_contact.get('confidence', 0):
                    best_contact = hunter_result

    # Step 3: Website scraping
    if website and (not best_contact or not best_contact.get('contact_email')):
        scrape_result = _scrape_website(crd, website, db_path=db_path)
        if scrape_result and scrape_result.get('contact_email'):
            if not best_contact or not best_contact.get('contact_email'):
                best_contact = scrape_result

    # Step 4: Hunter.io Email Finder (if we have a name but no email)
    hunter_allowed = (credit_budget is None or credit_budget['used'] < credit_budget['limit'])
    if (HUNTER_API_KEY and website and hunter_allowed and best_contact
            and best_contact.get('contact_name') and not best_contact.get('contact_email')):
        domain = _extract_domain(website)
        if domain:
            email = _hunter_email_finder(
                crd, domain, best_contact['contact_name'], db_path=db_path
            )
            if credit_budget is not None:
                credit_budget['used'] += 1
            if email and not _is_generic_email(email):
                best_contact['contact_email'] = email
                best_contact['confidence'] = max(best_contact.get('confidence', 0), 70.0)

    # Store the best contact
    if best_contact:
        upsert_contact(crd, best_contact, db_path=db_path)

    return best_contact


def enrich_batch(crd_list, force=False, db_path=None, credit_limit=None,
                 progress_callback=None):
    """Enrich a batch of firms. Returns summary stats.

    Args:
        crd_list: List of CRD numbers to enrich.
        force: If True, re-enrich even if cached.
        db_path: Optional database path.
        credit_limit: Max Hunter.io credits for this batch.
            None = use DEFAULT_BATCH_CREDIT_LIMIT.
            0 = no limit.
        progress_callback: Optional callable(current, total, results) called
            after each firm is processed.
    """
    init_db(db_path)

    if credit_limit is None:
        effective_limit = DEFAULT_BATCH_CREDIT_LIMIT
    elif credit_limit == 0:
        effective_limit = float('inf')
    else:
        effective_limit = credit_limit

    credit_budget = {'used': 0, 'limit': effective_limit}
    total = len(crd_list)
    results = {
        'enriched': 0, 'cached': 0, 'no_result': 0, 'errors': 0,
        'skipped_credit_limit': 0,
        'credits_used': 0,
        'credit_limit': credit_limit if credit_limit is not None else DEFAULT_BATCH_CREDIT_LIMIT,
        'credit_limit_hit': False,
    }

    for i, crd in enumerate(crd_list):
        try:
            if not force:
                existing = get_contact(crd, db_path=db_path)
                if existing and existing.get('contact_email'):
                    results['cached'] += 1
                    results['credits_used'] = credit_budget['used']
                    if progress_callback:
                        progress_callback(i + 1, total, results)
                    continue

            # Skip remaining firms if credit limit reached
            if credit_budget['used'] >= credit_budget['limit']:
                results['skipped_credit_limit'] += 1
                results['credit_limit_hit'] = True
                results['credits_used'] = credit_budget['used']
                if progress_callback:
                    progress_callback(i + 1, total, results)
                continue

            contact = enrich_firm(crd, force=force, db_path=db_path,
                                  credit_budget=credit_budget)
            if contact and contact.get('contact_email'):
                results['enriched'] += 1
            else:
                results['no_result'] += 1
        except Exception:
            results['errors'] += 1

        results['credits_used'] = credit_budget['used']
        if progress_callback:
            progress_callback(i + 1, total, results)

    results['credits_used'] = credit_budget['used']
    return results


# --- Hunter.io ---

def _hunter_domain_search(crd, domain, db_path=None):
    """Search Hunter.io for contacts at a domain."""
    try:
        resp = requests.get(HUNTER_DOMAIN_SEARCH, params={
            'domain': domain,
            'api_key': HUNTER_API_KEY,
            'limit': 10,
        }, timeout=15)

        log_enrichment(crd, 'hunter_io', '/domain-search', resp.status_code,
                       'success' if resp.status_code == 200 else 'error',
                       credits_used=1, db_path=db_path)

        if resp.status_code != 200:
            return None

        data = resp.json().get('data', {})
        emails = data.get('emails', [])
        if not emails:
            return None

        # Pick the best contact by title priority
        best = _pick_best_hunter_contact(emails)
        if best:
            return {
                'contact_name': f"{best.get('first_name', '')} {best.get('last_name', '')}".strip(),
                'first_name': best.get('first_name'),
                'last_name': best.get('last_name'),
                'contact_email': best.get('value'),
                'contact_title': best.get('position'),
                'contact_phone': best.get('phone_number'),
                'contact_linkedin': best.get('linkedin'),
                'source': 'hunter_io',
                'confidence': best.get('confidence', 0),
            }
    except requests.RequestException:
        log_enrichment(crd, 'hunter_io', '/domain-search', 0, 'error',
                       db_path=db_path)
    return None


def _hunter_email_finder(crd, domain, full_name, db_path=None):
    """Use Hunter.io Email Finder to get an email for a specific person."""
    parts = full_name.strip().split()
    if len(parts) < 2:
        return None
    first_name = parts[0]
    last_name = parts[-1]

    try:
        resp = requests.get(HUNTER_EMAIL_FINDER, params={
            'domain': domain,
            'first_name': first_name,
            'last_name': last_name,
            'api_key': HUNTER_API_KEY,
        }, timeout=15)

        log_enrichment(crd, 'hunter_io', '/email-finder', resp.status_code,
                       'success' if resp.status_code == 200 else 'not_found',
                       credits_used=1, db_path=db_path)

        if resp.status_code == 200:
            data = resp.json().get('data', {})
            email = data.get('email')
            if email and data.get('score', 0) > 30:
                return email
    except requests.RequestException:
        log_enrichment(crd, 'hunter_io', '/email-finder', 0, 'error',
                       db_path=db_path)
    return None


def _pick_best_hunter_contact(emails):
    """Pick the best contact from Hunter.io results by title priority."""
    scored = []
    for e in emails:
        position = (e.get('position') or '').lower()
        priority = len(TITLE_PRIORITY)  # default: lowest
        for i, title_kw in enumerate(TITLE_PRIORITY):
            if title_kw in position:
                priority = i
                break
        scored.append((priority, e.get('confidence', 0), e))

    scored.sort(key=lambda x: (x[0], -x[1]))
    return scored[0][2] if scored else None


# --- Website Scraping ---

def _scrape_website(crd, website, db_path=None):
    """Scrape a firm's website for contact information."""
    all_contacts = []

    # Scrape homepage
    homepage_contacts = _scrape_page(website)
    all_contacts.extend(homepage_contacts)

    # Scrape subpages
    base_url = website.rstrip('/')
    for subpage in SCRAPE_SUBPAGES:
        url = f"{base_url}{subpage}"
        try:
            contacts = _scrape_page(url)
            all_contacts.extend(contacts)
        except Exception:
            pass
        time.sleep(SCRAPE_DELAY)

    if not all_contacts:
        log_enrichment(crd, 'website_scrape', website, 0, 'not_found',
                       db_path=db_path)
        return None

    # Pick the best contact
    best = _pick_best_scraped_contact(all_contacts)
    if best:
        log_enrichment(crd, 'website_scrape', website, 200, 'success',
                       db_path=db_path)
        best['source'] = 'website_scrape'
        best['confidence'] = 60.0
        return best

    log_enrichment(crd, 'website_scrape', website, 200, 'not_found',
                   db_path=db_path)
    return None


def _scrape_page(url):
    """Scrape a single page for emails, phones, and names."""
    contacts = []
    try:
        resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=15,
                            allow_redirects=True)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, 'lxml')
        text = soup.get_text(separator='\n')

        # Extract emails
        emails = set(re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text))
        emails = {e for e in emails if not _is_generic_email(e)}

        # Extract phone numbers
        phones = re.findall(
            r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text
        )

        # Try to find name-title pairs
        name_title_pairs = _extract_name_title_pairs(soup)

        # Build contact records
        for name, title in name_title_pairs:
            contact = {
                'contact_name': name,
                'contact_title': title,
                'contact_email': _match_email_to_name(name, emails),
                'contact_phone': phones[0] if phones else None,
            }
            contacts.append(contact)

        # If we found emails but no names, still record them
        if emails and not name_title_pairs:
            for email in emails:
                contacts.append({
                    'contact_name': None,
                    'contact_email': email,
                    'contact_title': None,
                    'contact_phone': phones[0] if phones else None,
                })

    except requests.RequestException:
        pass
    return contacts


def _extract_name_title_pairs(soup):
    """Extract name-title pairs from HTML using common patterns."""
    pairs = []

    # Strategy 1: Look for team/bio sections with structured HTML
    team_selectors = [
        '[class*="team"]', '[class*="bio"]', '[class*="staff"]',
        '[class*="leadership"]', '[class*="advisor"]', '[class*="people"]',
    ]
    for selector in team_selectors:
        for el in soup.select(selector):
            names = el.select('h2, h3, h4, [class*="name"]')
            titles = el.select('[class*="title"], [class*="position"], [class*="role"]')
            if names and titles:
                for name_el, title_el in zip(names, titles):
                    name = name_el.get_text(strip=True)
                    title = title_el.get_text(strip=True)
                    if name and title and _is_valid_person_name(name) and len(title) <= 100:
                        pairs.append((name, title))

    # Strategy 2: Regex for "Name, Title" or "Name - Title" patterns
    text = soup.get_text(separator='\n')
    pattern = r'^([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*[,\-–]\s*(.+?)$'
    for match in re.finditer(pattern, text, re.MULTILINE):
        name = match.group(1).strip()
        title = match.group(2).strip()
        if _is_valid_person_name(name) and len(title) > 3 and len(title) <= 100:
            pairs.append((name, title))

    return pairs


def _match_email_to_name(name, emails):
    """Try to match an email address to a person's name."""
    if not name or not emails:
        return None
    name_parts = name.lower().split()
    for email in emails:
        local = email.split('@')[0].lower()
        # Check if any name part appears in the email local part
        if any(part in local for part in name_parts if len(part) > 2):
            return email
    # Return the first non-generic email as fallback
    return next(iter(emails), None)


def _pick_best_scraped_contact(contacts):
    """Pick the best contact from scraped results."""
    # Filter out contacts with non-person names (cities, companies, etc.)
    contacts = [
        c for c in contacts
        if not c.get('contact_name') or _is_valid_person_name(c['contact_name'])
    ]
    if not contacts:
        return None
    # Prioritize by: has email + has title + title priority
    scored = []
    for c in contacts:
        has_email = 1 if c.get('contact_email') else 0
        title = (c.get('contact_title') or '').lower()
        title_rank = len(TITLE_PRIORITY)
        for i, kw in enumerate(TITLE_PRIORITY):
            if kw in title:
                title_rank = i
                break
        has_name = 1 if c.get('contact_name') else 0
        scored.append((-has_email, title_rank, -has_name, c))

    scored.sort()
    return scored[0][3] if scored else None


def _is_valid_person_name(name):
    """Check if a string is plausibly a person's name, not a city/company/junk."""
    if not name:
        return False
    name_clean = name.strip()
    parts = name_clean.split()
    if len(parts) < 2:
        return False
    if len(name_clean) > 50:
        return False
    name_lower = name_clean.lower()
    if name_lower in NON_PERSON_WORDS:
        return False
    for part in parts:
        if part.lower() in NON_PERSON_WORDS:
            return False
    return True


def _is_generic_email(email):
    """Check if an email is generic or from a blocked domain."""
    local, _, domain = email.partition('@')
    if domain.lower() in GENERIC_EMAIL_DOMAINS:
        return True
    if local.lower() in GENERIC_EMAIL_PREFIXES:
        return True
    return False


def _extract_domain(url):
    """Extract domain from a URL."""
    if not url:
        return None
    url = url.strip().lower()
    if not url.startswith('http'):
        url = 'https://' + url
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.split(':')[0]  # Remove port
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        result = enrich_firm(crd)
        print(f"CRD {crd}: {result}")
    else:
        print("Usage: python tools/enrich_contacts.py <CRD_NUMBER>")
