"""
Tool: Hunter.io Contact Enrichment

For each contact extracted from Form ADV PDFs (name + title, but typically
no email), uses Hunter.io Email Finder to look up their email address.

Every API call is logged for credit tracking.
"""

import os
import re

import requests
from dotenv import load_dotenv

from tools.cache_db import (
    init_db, get_firm_by_crd, get_contacts_for_firm, update_contact_email,
    log_enrichment,
)

load_dotenv()

# Bridge Streamlit Cloud secrets into env vars for os.getenv() compatibility
try:
    import streamlit as st
    for key in st.secrets:
        if isinstance(st.secrets[key], str):
            os.environ.setdefault(key, st.secrets[key])
except Exception:
    pass

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY', '')
HUNTER_EMAIL_FINDER = 'https://api.hunter.io/v2/email-finder'

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
        domain = domain.split(':')[0]
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


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


def enrich_contacts_batch(crd_list, credit_limit=None, db_path=None,
                          progress_callback=None):
    """Enrich all contacts for a list of firms using Hunter.io Email Finder.

    For each firm in crd_list, gets all contacts from the database that
    don't have an email yet, and tries Hunter.io Email Finder for each.

    Args:
        crd_list: List of CRD numbers whose contacts to enrich.
        credit_limit: Max Hunter.io credits for this batch.
            None = use DEFAULT_BATCH_CREDIT_LIMIT.
            0 = no limit.
        db_path: Optional database path.
        progress_callback: Optional callable(current, total, results).

    Returns dict: {enriched, skipped, no_result, errors, credits_used,
                   credit_limit_hit}
    """
    init_db(db_path)

    if not HUNTER_API_KEY:
        return {
            'enriched': 0, 'skipped': 0, 'no_result': 0, 'errors': 0,
            'credits_used': 0, 'credit_limit_hit': False,
            'no_api_key': True,
        }

    if credit_limit is None:
        effective_limit = DEFAULT_BATCH_CREDIT_LIMIT
    elif credit_limit == 0:
        effective_limit = float('inf')
    else:
        effective_limit = credit_limit

    credits_used = 0
    total = len(crd_list)
    results = {
        'enriched': 0, 'skipped': 0, 'no_result': 0, 'errors': 0,
        'credits_used': 0, 'credit_limit_hit': False,
    }

    for i, crd in enumerate(crd_list):
        if credits_used >= effective_limit:
            results['credit_limit_hit'] = True
            results['credits_used'] = credits_used
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        firm = get_firm_by_crd(crd, db_path=db_path)
        if not firm or not firm.get('website'):
            results['skipped'] += 1
            results['credits_used'] = credits_used
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        domain = _extract_domain(firm['website'])
        if not domain:
            results['skipped'] += 1
            results['credits_used'] = credits_used
            if progress_callback:
                progress_callback(i + 1, total, results)
            continue

        # Get all contacts for this firm that don't have email yet
        contacts = get_contacts_for_firm(crd, db_path=db_path)
        contacts_needing_email = [
            c for c in contacts
            if not c.get('contact_email') and c.get('first_name') and c.get('last_name')
        ]

        for contact in contacts_needing_email:
            if credits_used >= effective_limit:
                results['credit_limit_hit'] = True
                break

            try:
                result = enrich_contact_hunter(
                    contact['id'],
                    contact['first_name'],
                    contact['last_name'],
                    domain, crd,
                    db_path=db_path,
                )
                credits_used += 1

                if result and result.get('email'):
                    results['enriched'] += 1
                else:
                    results['no_result'] += 1
            except Exception:
                results['errors'] += 1

        results['credits_used'] = credits_used
        if progress_callback:
            progress_callback(i + 1, total, results)

    results['credits_used'] = credits_used
    return results


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        contacts = get_contacts_for_firm(crd)
        if contacts:
            print(f"CRD {crd}: {len(contacts)} contacts found")
            for c in contacts:
                print(f"  {c.get('contact_name')} - {c.get('contact_email', 'no email')}")
        else:
            print(f"CRD {crd}: No contacts in database")
    else:
        print("Usage: python tools/enrich_contacts.py <CRD_NUMBER>")
