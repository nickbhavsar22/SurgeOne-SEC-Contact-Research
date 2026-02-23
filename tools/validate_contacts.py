"""
Tool 6: Contact Data Validation

Validates enriched contact data for accuracy before outreach:
  - Email format (RFC 5322 simplified)
  - Email domain matches firm website domain
  - Phone format (US)
  - Cross-reference CCO name from Form ADV vs enrichment
  - Staleness detection (>90 days since enrichment)
"""

import re
from datetime import datetime, timedelta

from tools.cache_db import (
    init_db, get_contact, get_firm_by_crd, get_form_adv,
    update_contact_validation,
)
from tools.enrich_contacts import _is_generic_email

# Simplified RFC 5322 email regex
EMAIL_PATTERN = re.compile(
    r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@'
    r'[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?'
    r'(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$'
)

# US phone pattern
PHONE_PATTERN = re.compile(
    r'^(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$'
)

STALE_DAYS = 90


def validate_contact(crd, db_path=None):
    """Validate a contact record for a firm.

    Returns (status, issues) where:
      status: 'valid', 'suspect', or 'invalid'
      issues: list of issue strings
    """
    init_db(db_path)
    contact = get_contact(crd, db_path=db_path)
    if not contact:
        return 'invalid', ['No contact found']

    firm = get_firm_by_crd(crd, db_path=db_path)
    form_adv = get_form_adv(crd, db_path=db_path)
    issues = []

    # Check email format
    email = contact.get('contact_email')
    if email:
        if not EMAIL_PATTERN.match(email):
            issues.append(f'Invalid email format: {email}')
        elif _is_generic_email(email):
            issues.append(f'Generic/role-based email address: {email}')
    else:
        issues.append('No email address')

    # Check email domain matches firm website
    if email and firm and firm.get('website'):
        email_domain = email.split('@')[1].lower() if '@' in email else ''
        firm_domain = _extract_domain(firm['website'])
        if firm_domain and email_domain and email_domain != firm_domain:
            # Allow subdomains
            if not email_domain.endswith('.' + firm_domain) and not firm_domain.endswith('.' + email_domain):
                issues.append(f'Email domain ({email_domain}) does not match firm website ({firm_domain})')

    # Check phone format
    phone = contact.get('contact_phone')
    if phone and not PHONE_PATTERN.match(phone.strip()):
        issues.append(f'Invalid phone format: {phone}')

    # Cross-reference CCO name
    if form_adv and form_adv.get('cco_name') and contact.get('contact_name'):
        adv_name = form_adv['cco_name'].lower().strip()
        contact_name = contact['contact_name'].lower().strip()
        if adv_name != contact_name:
            # Check if last names match at least
            adv_last = adv_name.split()[-1] if adv_name.split() else ''
            contact_last = contact_name.split()[-1] if contact_name.split() else ''
            if adv_last != contact_last:
                issues.append(f'Contact name ({contact["contact_name"]}) does not match Form ADV CCO ({form_adv["cco_name"]})')

    # Check staleness
    enriched_at = contact.get('enriched_at')
    if enriched_at:
        try:
            enriched_date = datetime.fromisoformat(enriched_at)
            if datetime.utcnow() - enriched_date > timedelta(days=STALE_DAYS):
                issues.append(f'Contact data is stale (enriched {enriched_at})')
        except (ValueError, TypeError):
            pass

    # Determine status
    if not email:
        status = 'invalid'
    elif any('Invalid email' in i for i in issues):
        status = 'invalid'
    elif len(issues) == 0:
        status = 'valid'
    else:
        status = 'suspect'

    # Update database
    issues_str = '; '.join(issues) if issues else None
    update_contact_validation(contact['id'], status, issues_str, db_path=db_path)

    return status, issues


def validate_batch(crd_list, db_path=None):
    """Validate contacts for a batch of firms.

    Returns summary: {valid, suspect, invalid, no_contact}.
    """
    init_db(db_path)
    results = {'valid': 0, 'suspect': 0, 'invalid': 0, 'no_contact': 0}

    for crd in crd_list:
        contact = get_contact(crd, db_path=db_path)
        if not contact:
            results['no_contact'] += 1
            continue
        status, _ = validate_contact(crd, db_path=db_path)
        results[status] += 1

    return results


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
