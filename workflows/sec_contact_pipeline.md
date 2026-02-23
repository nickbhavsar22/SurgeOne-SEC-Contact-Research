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

### Stage 2: Research Firms (PDF Extraction + Hunter.io)
**Tools:** `tools/parse_form_adv.py`, `tools/enrich_contacts.py`
**Input:** CRD numbers from Stage 1, user-selected batch size
**Output:** All contacts in `contacts` table, enriched with email/phone

**Step 2a - Form ADV PDF Extraction** (`parse_form_adv.py`):
1. Downloads Form ADV PDF from `reports.adviserinfo.sec.gov/reports/ADV/{CRD}/PDF/{CRD}.pdf`
2. Extracts text from first 15 pages using pdfplumber
3. Applies regex patterns to find ALL contacts:
   - Pattern 1: Principal/Owner (after "your last, first, and middle names")
   - Pattern 2: Chief Compliance Officer (Section J)
   - Pattern 3: Other officers/directors (Schedule A items with Name: + Title:)
4. Extracts all non-generic emails from PDF text
5. Stores all contacts in `contacts` table (multiple per firm)
6. Marks firm as processed in `form_adv_details`
7. Rate-limited to 1 request/second

**Step 2b - Hunter.io Email Enrichment** (`enrich_contacts.py`):
1. For each contact without an email, calls Hunter.io Email Finder
2. Uses contact's first_name + last_name + firm website domain
3. Accepts emails with score > 30, rejects generic emails
4. Updates contact record with email and phone
5. Each lookup uses 1 Hunter.io credit
6. Stops at credit limit (default 100, configurable in UI)

**Edge cases:**
- Hunter.io paid plan: 2,000 credits/month - monitor via sidebar
- Per-batch credit limit prevents runaway credit usage
- Caching: firms processed within 30 days are skipped automatically
- Generic emails (info@, support@, compliance@, etc.) are filtered
- Contacts without first/last name are skipped for Hunter.io lookup

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

# Stage 2a - PDF extraction (single firm)
python tools/parse_form_adv.py 123456

# Stage 2b - Hunter.io enrichment (single firm)
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
- Form ADV PDFs contain names/titles but NOT email/phone - Hunter.io needed for those
- Hunter.io paid plan: 2,000 credits/month; per-batch limit configurable
- PDF extraction depends on consistent Form ADV formatting; some firms may have non-standard layouts
- Contact name extraction uses heuristic validation; some valid names may be rejected if they contain common business words
