# SEC & Contact Research Pipeline

## Objective
Discover newly SEC-registered RIAs and their key contacts for SurgeOne.ai compliance advisory outreach.

## When to Run
Monthly, after SEC publishes new FOIA data (typically 1st-2nd of each month).

## Prerequisites
- Python environment with `requirements.txt` installed
- `.env` configured with `HUNTER_API_KEY` (optional but recommended)
- Internet access for SEC.gov and Hunter.io API

## Pipeline Stages

### Stage 1: SEC FOIA Data Import
**Tool:** `tools/fetch_sec_data.py`
**Input:** SEC FOIA ZIP/CSV file (uploaded or auto-downloaded)
**Output:** 120-day approval firms stored in `surge_research.db`

1. Loads the monthly CSV from a ZIP file (upload or download from `sec.gov/foia`)
2. Parses ~448 columns down to the 17 we need
3. Filters to firms with status containing "120" or "pending" (120-day approval window)
4. Upserts all matching firms into the `firms` table

**Edge cases:**
- SEC changes the ZIP naming convention -> try 4 months of candidate URLs
- CSV encoding -> always use `latin-1`
- Empty or malformed AUM values -> `_safe_int()` handles gracefully
- SEC FOIA URLs may return 403 -> user uploads local file as primary method

### Stage 2: Research Firms (Hunter.io Domain Search)

**Tool:** `tools/enrich_contacts.py`
**Input:** CRD numbers from Stage 1, user-selected batch size
**Output:** All contacts in `contacts` table with names, titles, emails, phones

Uses Hunter.io Domain Search API to find all people at each firm's website domain
in a single call. Each call uses 1 Hunter.io credit and returns names, titles,
emails, and phone numbers.

1. For each unprocessed firm with a website:
   a. Extract domain from firm's website URL
   b. Skip social media domains (LinkedIn, Facebook, Twitter, etc.)
   c. Call Hunter.io Domain Search API (`/v2/domain-search`)
   d. Filter out generic emails (info@, support@, compliance@, etc.)
   e. Filter out contacts without first/last name
   f. Store all valid contacts in `contacts` table
   g. Mark firm as processed in `form_adv_details` (cached for 30 days)
2. Stops at credit limit (default 100, configurable in UI)

**Edge cases:**

- Hunter.io paid plan: 2,000 credits/month — monitor via sidebar
- Per-batch credit limit prevents runaway credit usage
- Caching: firms processed within 30 days are skipped automatically
- Generic emails (info@, support@, compliance@, etc.) are filtered
- Social media URLs (linkedin.com, facebook.com, etc.) are skipped
- Firms without a website URL are skipped and marked processed

### Stage 3: Export
**Via:** Dashboard export button
**Output:** CSV file with all contacts joined with firm data

Columns: CRD, Company, State, Website, AUM, Contact Name, Title, Email, Phone, Source

## Running the Pipeline

### Via Dashboard (recommended)
```bash
streamlit run app.py
```
1. Upload SEC FOIA ZIP/CSV (or use auto-download)
2. Set batch size and credit limit
3. Click "Start Research" to process firms
4. Download CSV from the Contact List section

### Via CLI (individual tools)

```bash
# Stage 1
python tools/fetch_sec_data.py

# Stage 2 - Hunter.io Domain Search (single firm)
python tools/enrich_contacts.py 123456
```

## Data Model
All data persists in `surge_research.db` (SQLite, WAL mode).

| Table | Purpose |
|-------|---------|
| `firms` | Core firm data from SEC FOIA (120-day approvals) |
| `form_adv_details` | CCO info, processing timestamp |
| `contacts` | All contacts per firm (multiple per CRD) |
| `enrichment_log` | Audit trail for all API calls + credit tracking |
| `export_history` | Record of CSV exports |

## Known Limitations

- SEC FOIA download URLs may return 403; user upload is the reliable method
- Hunter.io paid plan: 2,000 credits/month; per-batch limit configurable
- Hunter.io Domain Search only works for firms with a real website domain (not LinkedIn/social media URLs)
- ~60% of firms yield contacts via Domain Search; firms without web presence return nothing
- Form ADV PDFs have blank person-name fields in practice; PDF extraction (`parse_form_adv.py`) exists but is not used in the primary pipeline
