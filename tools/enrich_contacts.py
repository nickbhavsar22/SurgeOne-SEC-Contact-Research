"""
Tool: Hunter.io Contact Research

For each firm, uses Hunter.io Domain Search to find ALL people at the firm's
website domain — returning names, titles, emails, and phone numbers in one call.

This replaces the previous two-step approach (PDF extraction + Email Finder).
Each Domain Search uses 1 Hunter.io credit regardless of how many contacts
are returned.

Every API call is logged for credit tracking.
"""

import os
import logging

import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from tools.cache_db import (
    init_db, get_firm_by_crd, insert_contact, delete_contacts_for_firm,
    get_unprocessed_crds, upsert_form_adv, log_enrichment,
)

# Bridge Streamlit Cloud secrets into env vars for os.getenv() compatibility
try:
    import streamlit as st
    for key in st.secrets:
        if isinstance(st.secrets[key], str):
            os.environ.setdefault(key, st.secrets[key])
except Exception:
    pass

logger = logging.getLogger(__name__)

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY', '')
HUNTER_DOMAIN_SEARCH = 'https://api.hunter.io/v2/domain-search'
HUNTER_EMAIL_FINDER = 'https://api.hunter.io/v2/email-finder'

# Domains that are social media / not useful for Hunter.io
SOCIAL_MEDIA_DOMAINS = {
    'linkedin.com', 'facebook.com', 'twitter.com', 'x.com',
    'instagram.com', 'youtube.com', 'tiktok.com',
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

DEFAULT_BATCH_CREDIT_LIMIT = 100  # Max Hunter.io credits per batch run


def _is_generic_email(email):
    """Check if an email is generic or from a blocked domain."""
    local, _, domain = email.partition('@')
    if domain.lower() in GENERIC_EMAIL_DOMAINS:
        return True
    if local.lower() in GENERIC_EMAIL_PREFIXES:
        return True
    return False


def _extract_domain(url):
    """Extract domain from a URL. Returns None for social media domains."""
    if not url:
        return None
    url = url.strip().lower()
    if not url.startswith('http'):
        url = 'https://' + url
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.split(':')[0]
        if domain.startswith('www.'):
            domain = domain[4:]
        if not domain:
            return None
        # Skip social media domains
        for social in SOCIAL_MEDIA_DOMAINS:
            if domain == social or domain.endswith('.' + social):
                return None
        return domain
    except Exception:
        return None


def domain_search(domain=None, company=None, crd=None, db_path=None):
    """Search Hunter.io for all people at a domain or company.

    Args:
        domain: website domain to search (e.g., 'acmecapital.com')
        company: company name for fallback lookup (e.g., 'Acme Capital LLC')
        crd: firm CRD number (for logging)
        db_path: database path

    Returns list of contacts: [{first_name, last_name, position, email,
                                phone, confidence, source}]
    Uses 1 Hunter.io credit. At least one of domain or company is required.
    """
    if not HUNTER_API_KEY or (not domain and not company):
        return []

    search_label = domain or company

    try:
        params = {
            'api_key': HUNTER_API_KEY,
            'limit': 20,
            'type': 'personal',
        }
        if domain:
            params['domain'] = domain
        if company:
            params['company'] = company

        resp = requests.get(HUNTER_DOMAIN_SEARCH, params=params, timeout=15)

        log_enrichment(
            crd or 0, 'hunter_io', '/domain-search', resp.status_code,
            'success' if resp.status_code == 200 else 'error',
            credits_used=1, db_path=db_path,
        )

        if resp.status_code != 200:
            return []

        data = resp.json().get('data', {})
        emails = data.get('emails', [])

        contacts = []
        for entry in emails:
            email = entry.get('value')
            if not email or _is_generic_email(email):
                continue

            first = entry.get('first_name')
            last = entry.get('last_name')
            if not first or not last:
                continue

            contacts.append({
                'first_name': first,
                'last_name': last,
                'contact_name': f"{first} {last}",
                'contact_title': entry.get('position') or None,
                'contact_email': email,
                'contact_phone': entry.get('phone_number') or None,
                'confidence': entry.get('confidence', 0),
                'source': 'hunter_domain_search',
            })

        return contacts

    except requests.RequestException as e:
        logger.error('Domain search failed for %s: %s', search_label, e)
        log_enrichment(
            crd or 0, 'hunter_io', '/domain-search', 0, 'error',
            db_path=db_path,
        )
        return []


def enrich_contact_hunter(contact_id, first_name, last_name, domain, crd,
                          db_path=None):
    """Find email for a single person via Hunter.io Email Finder.

    Args:
        contact_id: database contact ID to update
        first_name: person's first name
        last_name: person's last name
        domain: firm's website domain
        crd: firm CRD (for logging)
        db_path: database path

    Returns dict with email/phone or None. Uses 1 Hunter.io credit.
    """
    from tools.cache_db import update_contact_email

    if not HUNTER_API_KEY or not first_name or not last_name or not domain:
        return None

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
            phone = data.get('phone_number')

            if email and data.get('score', 0) > 30 and not _is_generic_email(email):
                update_contact_email(contact_id, email, phone, db_path=db_path)
                return {'email': email, 'phone': phone}

    except requests.RequestException:
        log_enrichment(crd, 'hunter_io', '/email-finder', 0, 'error',
                       db_path=db_path)
    return None


def research_firms_batch(crd_list, max_age_days=30, credit_limit=None,
                         db_path=None, progress_callback=None):
    """Research all firms in crd_list using Hunter.io Domain Search.

    For each unprocessed firm:
      1. Call Hunter.io Domain Search with domain and/or company name (1 credit)
      2. Store all returned contacts in the database
      3. Mark firm as processed (cached for max_age_days)

    Args:
        crd_list: List of CRD numbers to research.
        max_age_days: Skip firms processed within this many days.
        credit_limit: Max Hunter.io credits. None = DEFAULT_BATCH_CREDIT_LIMIT.
            0 = no limit.
        db_path: Optional database path.
        progress_callback: Optional callable(current, total, results_dict).

    Returns dict: {processed, cached, skipped, no_contacts, errors,
                   contacts_found, credits_used, credit_limit_hit, no_api_key}
    """
    init_db(db_path)

    if not HUNTER_API_KEY:
        return {
            'processed': 0, 'cached': 0, 'skipped': 0, 'no_contacts': 0,
            'errors': 0, 'contacts_found': 0, 'credits_used': 0,
            'credit_limit_hit': False, 'no_api_key': True,
        }

    if credit_limit is None:
        effective_limit = DEFAULT_BATCH_CREDIT_LIMIT
    elif credit_limit == 0:
        effective_limit = float('inf')
    else:
        effective_limit = credit_limit

    unprocessed = set(get_unprocessed_crds(crd_list, max_age_days, db_path))

    credits_used = 0
    total = len(crd_list)
    results = {
        'processed': 0, 'cached': 0, 'skipped': 0, 'no_contacts': 0,
        'errors': 0, 'contacts_found': 0, 'credits_used': 0,
        'credit_limit_hit': False,
    }

    for i, crd in enumerate(crd_list):
        if crd not in unprocessed:
            results['cached'] += 1
            results['credits_used'] = credits_used
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        if credits_used >= effective_limit:
            results['credit_limit_hit'] = True
            results['credits_used'] = credits_used
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        firm = get_firm_by_crd(crd, db_path=db_path)
        if not firm:
            results['skipped'] += 1
            results['credits_used'] = credits_used
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        domain = _extract_domain(firm.get('website'))
        company_name = firm.get('company') or firm.get('legal_name')

        # Need at least a domain or company name to search
        if not domain and not company_name:
            results['skipped'] += 1
            results['credits_used'] = credits_used
            upsert_form_adv(crd, {
                'cco_name': None, 'cco_email': None, 'cco_phone': None,
                'state_registrations': None, 'state_count': 0,
                'aum_breakdown': None,
            }, db_path=db_path)
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        try:
            contacts = domain_search(
                domain=domain, company=company_name,
                crd=crd, db_path=db_path,
            )
            credits_used += 1

            # Clear old contacts and insert new ones
            delete_contacts_for_firm(crd, db_path=db_path)
            for contact in contacts:
                insert_contact(crd, contact, db_path=db_path)

            # Mark firm as processed
            cco = next(
                (c for c in contacts
                 if c.get('contact_title') and 'compliance' in c['contact_title'].lower()),
                None,
            )
            upsert_form_adv(crd, {
                'cco_name': cco['contact_name'] if cco else None,
                'cco_email': cco['contact_email'] if cco else None,
                'cco_phone': cco.get('contact_phone') if cco else None,
                'state_registrations': None,
                'state_count': 0,
                'aum_breakdown': None,
            }, db_path=db_path)

            if contacts:
                results['processed'] += 1
                results['contacts_found'] += len(contacts)
            else:
                results['no_contacts'] += 1

        except Exception as e:
            logger.error('Error researching CRD %s: %s', crd, e)
            results['errors'] += 1

        results['credits_used'] = credits_used
        if progress_callback:
            progress_callback(i + 1, total, results)

    results['credits_used'] = credits_used
    return results


if __name__ == "__main__":
    import sys
    from tools.cache_db import get_contacts_for_firm

    if len(sys.argv) > 1:
        crd = int(sys.argv[1])

        # First check existing contacts
        contacts = get_contacts_for_firm(crd)
        if contacts:
            print(f"CRD {crd}: {len(contacts)} contacts in database")
            for c in contacts:
                print(f"  {c.get('contact_name')} — {c.get('contact_title', 'N/A')} "
                      f"— {c.get('contact_email', 'no email')}")
        else:
            # Try domain search
            firm = get_firm_by_crd(crd)
            if firm and firm.get('website'):
                domain = _extract_domain(firm['website'])
                if domain:
                    print(f"CRD {crd}: Searching Hunter.io for {domain}...")
                    contacts = domain_search(domain, crd=crd)
                    print(f"Found {len(contacts)} contacts:")
                    for c in contacts:
                        print(f"  {c['contact_name']} — {c.get('contact_title', 'N/A')} "
                              f"— {c['contact_email']}")
                else:
                    print(f"CRD {crd}: No usable website domain")
            else:
                print(f"CRD {crd}: Firm not found or no website")
    else:
        print("Usage: python tools/enrich_contacts.py <CRD_NUMBER>")
