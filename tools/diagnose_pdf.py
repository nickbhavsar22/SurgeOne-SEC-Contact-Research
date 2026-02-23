"""
Diagnostic tool: Download real Form ADV PDFs and inspect extracted text.

Selects random unprocessed firms from the database, downloads their PDFs,
saves the raw extracted text, and runs contact extraction to show what
the current regex patterns find (or fail to find).

Usage:
    python tools/diagnose_pdf.py [--count 10] [--reprocess]
"""

import io
import os
import re
import sys
import time
import random
import argparse
from pathlib import Path

import requests
import pdfplumber

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.cache_db import init_db, get_firms, get_unprocessed_crds
from tools.parse_form_adv import extract_contacts_from_pdf

PDF_URL_TEMPLATE = "https://reports.adviserinfo.sec.gov/reports/ADV/{crd}/PDF/{crd}.pdf"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}
DEBUG_DIR = Path(__file__).parent.parent / ".tmp" / "pdf_debug"


def download_and_save_text(crd):
    """Download PDF and save extracted text for inspection."""
    pdf_url = PDF_URL_TEMPLATE.format(crd=crd)

    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=45)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            text = ''
            for page in pdf.pages[:15]:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + '\n'

        if not text.strip():
            return None, "Empty PDF text"

        # Save to file
        debug_file = DEBUG_DIR / f"{crd}.txt"
        debug_file.write_text(text, encoding='utf-8')

        return text, None

    except Exception as e:
        return None, str(e)


def analyze_text(text, crd):
    """Analyze PDF text to find potential contact information."""
    findings = {
        'crd': crd,
        'text_length': len(text),
        'sections_found': [],
        'potential_names': [],
        'emails_found': [],
    }

    # Check for key sections
    if 'your last, first, and middle names' in text.lower():
        findings['sections_found'].append('Item 1.A (Legal Name)')
        # Extract what's there
        match = re.search(
            r'your last, first, and middle names\):\s*(.+?)(?:\n|B\.)',
            text, re.IGNORECASE
        )
        if match:
            findings['legal_name_raw'] = match.group(1).strip()

    if re.search(r'J\.?\s*Chief Compliance Officer', text, re.IGNORECASE):
        findings['sections_found'].append('Section J (CCO)')
        # Extract what's in the Name field
        match = re.search(
            r'Chief Compliance Officer.*?Name:\s*(.{0,80})',
            text, re.IGNORECASE | re.DOTALL
        )
        if match:
            raw = match.group(1).strip().split('\n')[0].strip()
            findings['cco_name_raw'] = raw if raw else '(BLANK)'

    if re.search(r'Schedule\s+A', text, re.IGNORECASE):
        findings['sections_found'].append('Schedule A')

    if re.search(r'Schedule\s+B', text, re.IGNORECASE):
        findings['sections_found'].append('Schedule B')

    # Check for Item 1.I contact person
    match = re.search(
        r'(?:1\.?\s*I|Item\s*I)[\.\s]*.*?(?:contact|person).*?Name:\s*(.{0,80})',
        text, re.IGNORECASE | re.DOTALL
    )
    if match:
        raw = match.group(1).strip().split('\n')[0].strip()
        if raw:
            findings['contact_person_raw'] = raw
            findings['sections_found'].append('Item 1.I (Contact Person)')

    # Find all Name: fields and what follows them
    name_fields = re.findall(r'Name:\s*(.{0,60})', text)
    findings['all_name_fields'] = [n.strip().split('\n')[0].strip() for n in name_fields
                                    if n.strip().split('\n')[0].strip()]

    # Find all emails
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
    findings['emails_found'] = list(set(emails))

    # Find potential person names (2-4 capitalized words in sequence)
    person_patterns = re.findall(
        r'\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)\b', text
    )
    # Deduplicate
    findings['potential_names'] = list(set(person_patterns))[:20]

    return findings


def run_diagnostic(count=10, reprocess=False):
    """Run diagnostic on N random firms."""
    init_db()
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    all_firms = get_firms()
    if not all_firms:
        print("ERROR: No firms in database. Import SEC data first.")
        return

    all_crds = [f['crd'] for f in all_firms]

    if reprocess:
        # Use all firms, even already processed
        candidates = all_crds
    else:
        candidates = get_unprocessed_crds(all_crds)
        if not candidates:
            print("All firms already processed. Use --reprocess to re-test.")
            candidates = all_crds

    # Random sample
    sample_size = min(count, len(candidates))
    selected = random.sample(candidates, sample_size)

    print(f"\n{'='*70}")
    print(f"DIAGNOSTIC: Testing PDF extraction on {sample_size} firms")
    print(f"{'='*70}\n")

    # Build a lookup for company names
    firm_lookup = {f['crd']: f for f in all_firms}

    firms_with_contacts = 0
    total_contacts = 0
    all_results = []

    for i, crd in enumerate(selected):
        firm = firm_lookup.get(crd, {})
        company = firm.get('company', 'Unknown')
        website = firm.get('website', 'N/A')

        print(f"\n--- [{i+1}/{sample_size}] CRD {crd}: {company} ---")
        print(f"    Website: {website}")

        # Download and save text
        text, error = download_and_save_text(crd)
        if error:
            print(f"    ERROR: {error}")
            all_results.append({'crd': crd, 'company': company, 'error': error})
            time.sleep(1)
            continue

        print(f"    PDF text: {len(text)} chars")

        # Analyze what's in the PDF
        analysis = analyze_text(text, crd)
        print(f"    Sections found: {', '.join(analysis['sections_found']) or 'None'}")

        if analysis.get('legal_name_raw'):
            print(f"    Legal Name (raw): {analysis['legal_name_raw']}")
        if analysis.get('cco_name_raw'):
            print(f"    CCO Name (raw): {analysis['cco_name_raw']}")
        if analysis.get('contact_person_raw'):
            print(f"    Contact Person (raw): {analysis['contact_person_raw']}")

        if analysis['all_name_fields']:
            print(f"    All 'Name:' fields: {analysis['all_name_fields'][:10]}")

        if analysis['emails_found']:
            print(f"    Emails found: {analysis['emails_found'][:5]}")

        if analysis['potential_names']:
            print(f"    Potential person names: {analysis['potential_names'][:10]}")

        # Run the actual extraction function
        contacts = extract_contacts_from_pdf(crd)
        print(f"\n    EXTRACTION RESULT: {len(contacts)} contacts")
        for c in contacts:
            print(f"      - {c['name']} ({c['title']}) "
                  f"[email: {c.get('email', 'none')}] "
                  f"[source: {c['source']}]")

        if contacts:
            firms_with_contacts += 1
            total_contacts += len(contacts)

        all_results.append({
            'crd': crd,
            'company': company,
            'contacts_found': len(contacts),
            'contacts': contacts,
            'analysis': analysis,
        })

        time.sleep(1)  # Rate limit

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"Firms tested:          {sample_size}")
    print(f"Firms with contacts:   {firms_with_contacts} ({firms_with_contacts/sample_size*100:.0f}%)")
    print(f"Total contacts found:  {total_contacts}")
    print(f"Success rate:          {firms_with_contacts/sample_size*100:.0f}% (target: 70%)")
    print(f"\nRaw text saved to: {DEBUG_DIR}")
    print(f"Inspect files with: cat .tmp/pdf_debug/<CRD>.txt")

    return all_results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Diagnose Form ADV PDF extraction')
    parser.add_argument('--count', type=int, default=10, help='Number of firms to test')
    parser.add_argument('--reprocess', action='store_true', help='Include already-processed firms')
    args = parser.parse_args()

    run_diagnostic(count=args.count, reprocess=args.reprocess)
