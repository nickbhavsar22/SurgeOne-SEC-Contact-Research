# SEC & Contact Research Pipeline

## Objective
Discover newly SEC-registered RIAs and their key contacts (CCOs) for SurgeOne.ai compliance advisory outreach.

## When to Run
Monthly, after SEC publishes new FOIA data (typically 1st–2nd of each month).

## Prerequisites
- Python environment with `requirements.txt` installed
- `.env` configured with `HUNTER_API_KEY` (optional but recommended)
- Internet access for SEC.gov, adviserinfo.sec.gov, and firm websites

## Pipeline Stages

### Stage 1: SEC FOIA Data Import
**Tool:** `tools/fetch_sec_data.py`
**Input:** SEC FOIA ZIP URL (auto-detected if omitted)
**Output:** Firms stored in `surge_research.db` with Track A/B classification

1. Downloads the latest monthly CSV from `sec.gov/foia`
2. Parses ~448 columns down to the 16 we need
3. Classifies each firm:
   - **Track A:** Status contains "120" or "pending" (firms in SEC registration approval window)
   - **Track B:** Not SEC-registered + AUM >= $90M (approaching the $100M threshold)
4. Upserts all Track A and Track B firms into the `firms` table

**Edge cases:**
- SEC changes the ZIP naming convention → try 4 months of candidate URLs
- CSV encoding → always use `latin-1`
- Empty or malformed AUM values → `_safe_int()` handles gracefully

### Stage 2: IAPD Form ADV Lookup + EDGAR CCO Extraction
**Tools:** `tools/query_iapd.py`, `tools/parse_form_adv.py`
**Input:** CRD numbers from Stage 1
**Output:** State registrations + CCO name/phone in `form_adv_details` table

**Step 2a — IAPD Lookup** (`query_iapd.py`):
1. Queries IAPD JSON API (`api.adviserinfo.sec.gov/search/firm/{crd}`)
2. Extracts state registrations and notice filing data
3. Rate-limited to 1 request/second; skips firms scraped within 30 days

Note: The IAPD JSON API does NOT expose CCO data. CCO name/email/phone
are always NULL from this endpoint.

**Step 2b — EDGAR CCO Extraction** (`parse_form_adv.py`):
1. Searches SEC EDGAR full-text search (EFTS) for the firm name + "compliance officer"
2. Prefers 13F-HR and Form D filings (structured XML with signature blocks)
3. Extracts CCO name, title, and phone from XML tags
4. Stores in `form_adv_details.cco_name/cco_phone` (preserves existing state data)
5. Rate-limited to ~2 req/sec per SEC EDGAR fair-use policy

Expected hit rate: ~35% of firms (those with 13F-HR or Form D filings on EDGAR).
Having the CCO name enables Step 4 of the enrichment waterfall (Hunter.io Email
Finder) to look up the CCO's email by name + domain.

**Edge cases:**
- Form ADV PDFs at `reports.adviserinfo.sec.gov` are template-only (filled values
  are not extractable from the PDF text) → use EDGAR filings instead
- EDGAR EFTS search matches by company name, which may return false matches for
  common names → extraction validates names are real person names
- Smaller firms without 13F or Form D filings won't have EDGAR CCO data → falls
  through to website scraping and Hunter.io domain search in Stage 4

### Stage 3: ICP Fit Scoring
**Tool:** `tools/score_firms.py`
**Input:** Firms from Stage 1 + Form ADV details from Stage 2
**Output:** `fit_score` (0-100) and `fit_reasons` on each firm

Data scoring (max 50 points):
- Website presence (+8), phone (+3)
- Advisory/wealth company name keywords (+6), scale keywords (+4)
- Top financial state (+4)
- Employee tiers (1/3/10+)
- AUM tiers ($0/$10M/$100M/$1B+)
- Client tiers (1/10/100+)
- Multi-state registration 4+ (+5)

Optional deep scoring adds website content analysis (max 75 points):
- Compliance, advisory, cybersecurity, team, client, technology keywords

**Edge cases:**
- Deep scoring changes normalization denominator (score can appear lower than shallow)
- Website scraping may timeout → returns 0 for web score, data score still works

### Stage 4: Contact Enrichment
**Tool:** `tools/enrich_contacts.py`
**Input:** Firms from Stages 1-3
**Output:** Best contact per firm in `contacts` table

Waterfall order:
1. **Form ADV CCO** (confidence 95 if email present, 50 if name-only) — populated by Stage 2b EDGAR extraction
2. **Hunter.io Domain Search** (1 credit per search)
3. **Website Scraping** (homepage + 12 subpages, 0.3s delay)
4. **Hunter.io Email Finder** (1 credit, only if name found but no email) — uses CCO name from Step 1

**Edge cases:**
- Hunter.io paid plan: 2,000 credits/month — monitor via API Usage page
- Per-batch credit limit defaults to 50 (configurable in UI) — prevents runaway credit usage
- If batch stops due to credit limit, re-run to continue with remaining firms
- Generic emails (info@, support@, reporting@, information@, compliance@, etc.) are filtered at ALL stages
- Contact deduplication: keeps highest-confidence contact per CRD

### Stage 5: Contact Validation
**Tool:** `tools/validate_contacts.py`
**Input:** Enriched contacts from Stage 4
**Output:** `validation_status` (valid/suspect/invalid) and `validation_issues` per contact

Checks:
- Email format (simplified RFC 5322)
- Generic/role-based email detection (info@, reporting@, compliance@, etc.)
- Email domain matches firm website domain
- Phone format (US)
- CCO name cross-reference (Form ADV vs enrichment source)
- Staleness (>90 days since enrichment)

### Stage 6: Export
**Via:** Dashboard Export page or manual CSV generation
**Output:** CSV file with firm + contact data, filtered by track/score/contact status

## Running the Pipeline

### Via Dashboard (recommended)
```bash
streamlit run app.py
```
Navigate to "Run Pipeline" and execute stages 1-5 sequentially.

### Via CLI (individual tools)
```bash
# Stage 1
python tools/fetch_sec_data.py

# Stage 2 (single firm)
python tools/query_iapd.py 123456

# Stage 3 (single firm)
python tools/score_firms.py 123456 [--deep]

# Stage 4 (single firm)
python tools/enrich_contacts.py 123456
```

## Data Model
All data persists in `surge_research.db` (SQLite, WAL mode).

| Table | Purpose |
|-------|---------|
| `firms` | Core firm data from SEC FOIA + track + fit score |
| `form_adv_details` | CCO info, state registrations, AUM from IAPD |
| `contacts` | Best enriched contact per firm |
| `enrichment_log` | Audit trail for all API calls + credit tracking |
| `export_history` | Record of CSV exports |

## Known Limitations
- IAPD does not have a public bulk API; scraping is fragile to layout changes
- Hunter.io paid plan: 2,000 credits/month; per-batch limit defaults to 50 — configurable via Stage 4 UI
- Form ADV CCO email is missing ~90% of the time
- Website scraping depends on firm site structure; many smaller RIAs have minimal sites
- Deep ICP scoring adds latency (1 HTTP request per firm website)
- Contact name extraction from website scraping uses heuristic validation; some valid names may be rejected if they contain common business words
