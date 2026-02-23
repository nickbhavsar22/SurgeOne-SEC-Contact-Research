"""
Tool 1: SEC FOIA Data Download & Parse

Downloads the latest SEC investment adviser data CSV, parses it,
and filters into two tracks:
  Track A: 120-day approval firms (already filing for SEC registration)
  Track B: Near-threshold state firms (AUM >= $90M, approaching SEC transition)
"""

import io
import os
import re
import zipfile
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from tools.cache_db import init_db, upsert_firms

SEC_BASE_URL = (
    "https://www.sec.gov/files/investment/data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers/"
)

# The 16 columns we need from the ~448-column CSV
COLUMN_MAP = {
    'Primary Business Name': 'company',
    'Organization CRD#': 'crd',
    'SEC Status Effective Date': 'status_date',
    'Latest ADV Filing Date': 'filing_date',
    'SEC Current Status': 'status',
    'Main Office City': 'city',
    'Main Office State': 'state',
    'Main Office Telephone Number': 'phone',
    'Website Address': 'website',
    'Legal Name': 'legal_name',
    '2A(1)': 'sec_registered',
    '2A(2)': 'era',
    '5A': 'employees',
    '5C(1)': 'clients',
    '5F(2)(a)': 'aum_discretionary',
    '5F(2)(b)': 'aum_nondiscretionary',
    '5F(2)(c)': 'aum',
}

NEAR_THRESHOLD_AUM = 90_000_000  # $90M


def _build_candidate_urls():
    """Build candidate SEC FOIA ZIP URLs for the last 4 months."""
    today = date.today()
    candidates = []
    for months_back in range(0, 4):
        year = today.year
        month = today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        for day in [1, 2]:
            try:
                d = date(year, month, day)
            except ValueError:
                continue
            stamp = d.strftime('%m%d%y')
            url = f"{SEC_BASE_URL}ia{stamp}.zip"
            candidates.append((url, d.strftime('%Y-%m-%d')))
    return candidates


def download_sec_csv(url=None):
    """Download and extract the SEC FOIA CSV from a ZIP URL.

    If no URL is provided, tries candidate URLs for the last 4 months.
    Returns a pandas DataFrame or None if download fails.
    """
    if url:
        urls_to_try = [(url, 'manual')]
    else:
        urls_to_try = _build_candidate_urls()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Bhavsar Growth Consulting SEC Research)'
    }

    for candidate_url, label in urls_to_try:
        try:
            resp = requests.get(candidate_url, headers=headers, timeout=120)
            if resp.status_code == 200:
                zf = zipfile.ZipFile(io.BytesIO(resp.content))
                csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
                if not csv_names:
                    continue
                with zf.open(csv_names[0]) as f:
                    df = pd.read_csv(
                        f, encoding='latin-1', low_memory=False,
                        dtype=str,  # Read everything as string initially
                    )
                return df
        except (requests.RequestException, zipfile.BadZipFile, Exception):
            continue
    return None


def parse_sec_dataframe(df):
    """Parse raw SEC CSV into cleaned records with only the columns we need.

    Returns a list of dicts ready for upsert_firms().
    """
    # Keep only columns we need
    available = [c for c in COLUMN_MAP.keys() if c in df.columns]
    df = df[available].copy()
    df = df.rename(columns=COLUMN_MAP)

    # Clean numeric columns
    for col in ['employees', 'clients', 'aum', 'aum_discretionary', 'aum_nondiscretionary']:
        if col in df.columns:
            df[col] = df[col].apply(_safe_int)

    # Clean CRD
    df['crd'] = df['crd'].apply(_safe_int)
    df = df.dropna(subset=['crd'])
    df['crd'] = df['crd'].astype(int)

    # Clean string columns
    for col in ['company', 'legal_name', 'city', 'state', 'phone', 'website',
                'status', 'sec_registered', 'era']:
        if col in df.columns:
            df[col] = df[col].apply(_safe_str)

    return df.to_dict('records')


def classify_track(record):
    """Classify a firm record into Track A, Track B, or None.

    Track A: 120-day approval status
    Track B: State-registered with AUM >= $90M (near SEC threshold)
    """
    status = (record.get('status') or '').strip()

    # Track A: 120-day approval
    if '120' in status.lower() or 'pending' in status.lower():
        return 'A'

    # Track B: State-registered, near threshold
    sec_reg = (record.get('sec_registered') or '').upper().strip()
    aum = record.get('aum') or 0
    if sec_reg != 'Y' and aum >= NEAR_THRESHOLD_AUM:
        return 'B'

    return None


def load_local_csv(file_path):
    """Load a SEC FOIA CSV from a local file path.

    Accepts either a raw CSV or a ZIP containing a CSV.
    Returns a pandas DataFrame or None.
    """
    path = str(file_path)
    try:
        if path.lower().endswith('.zip'):
            zf = zipfile.ZipFile(path)
            csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
            if not csv_names:
                return None
            with zf.open(csv_names[0]) as f:
                return pd.read_csv(f, encoding='latin-1', low_memory=False, dtype=str)
        else:
            return pd.read_csv(path, encoding='latin-1', low_memory=False, dtype=str)
    except Exception:
        return None


def fetch_and_store(url=None, csv_path=None, db_path=None):
    """Full pipeline: download SEC data, parse, classify, store.

    Args:
        url: SEC ZIP URL to download from (optional)
        csv_path: Local CSV or ZIP file path (optional, takes priority over url)
        db_path: Database path (optional)

    Returns dict with counts: {downloaded, track_a, track_b, skipped}.
    """
    init_db(db_path)

    if csv_path:
        df = load_local_csv(csv_path)
    else:
        df = download_sec_csv(url)
    if df is None:
        return {'downloaded': 0, 'track_a': 0, 'track_b': 0, 'skipped': 0, 'error': 'Download failed'}

    records = parse_sec_dataframe(df)

    track_a = []
    track_b = []
    skipped = 0

    for r in records:
        track = classify_track(r)
        if track == 'A':
            r['track'] = 'A'
            track_a.append(r)
        elif track == 'B':
            r['track'] = 'B'
            track_b.append(r)
        else:
            skipped += 1

    all_targeted = track_a + track_b
    upsert_firms(all_targeted, db_path=db_path)

    return {
        'downloaded': len(records),
        'track_a': len(track_a),
        'track_b': len(track_b),
        'skipped': skipped,
    }


def _safe_int(val):
    """Convert a value to int, handling commas, whitespace, and blanks."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip().replace(',', '').replace('$', '')
    if not s or s == '':
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _safe_str(val):
    """Clean a string value."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s else None


def build_candidate_urls():
    """Build candidate SEC FOIA ZIP URLs for the last 4 months. Public API."""
    return _build_candidate_urls()


def probe_sec_urls(candidates=None):
    """Probe SEC FOIA URLs with HEAD requests to check availability.

    Returns list of dicts: [{'url', 'date_label', 'available', 'size_mb'}]
    """
    if candidates is None:
        candidates = _build_candidate_urls()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Bhavsar Growth Consulting SEC Research)'
    }
    results = []

    for url, date_label in candidates:
        try:
            resp = requests.head(url, headers=headers, timeout=15, allow_redirects=True)
            available = resp.status_code == 200
            size_bytes = resp.headers.get('Content-Length')
            size_mb = round(int(size_bytes) / (1024 * 1024), 1) if size_bytes else None
            results.append({
                'url': url,
                'date_label': date_label,
                'available': available,
                'size_mb': size_mb,
            })
        except requests.RequestException:
            results.append({
                'url': url,
                'date_label': date_label,
                'available': False,
                'size_mb': None,
            })

    return results


if __name__ == "__main__":
    result = fetch_and_store()
    print(f"SEC Data Import: {result}")
