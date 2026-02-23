# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

**SurgeOne — SEC & Contact Research** automates the discovery of newly SEC-registered RIAs (Registered Investment Advisors) and their key contacts for compliance advisory outreach by SurgeOne.ai.

The core workflow: identify firms transitioning from state registration to SEC registration (120-day approval window), extract Chief Compliance Officers and other decision-makers from Form ADV filings, enrich with email/phone via third-party tools, and feed into outreach cadences.

## Domain Knowledge

### SEC Registration Lifecycle
- RIAs under ~$100M AUM are **state-registered**; once they cross the threshold or register in multiple states (5-7+), they transition to **SEC registration**
- Firms file **Form ADV** via **EDGAR** to begin SEC registration
- The SEC has **45 days** to approve or request additional info; firms in this window show as **"120-day approval"** status
- This transition window is the prime outreach moment — firms need compliance infrastructure before SEC approval completes

### Key Data Sources
| Source | What It Provides | Access |
|--------|-----------------|--------|
| **SEC FOIA Data** | Monthly CSV dumps of all registered investment advisers (~448 columns) | `https://www.sec.gov/foia` — public, no auth |
| **IAPD** (Investment Adviser Public Disclosure) | Firm lookup → Form ADV viewer → CCO info, AUM, state registrations | `https://adviserinfo.sec.gov/` — public, no auth |
| **Form ADV** | Section I: CCO name/email; Section D: AUM breakdown by account type | Via IAPD firm detail pages |
| **SEC EDGAR Full-Text Search** | Company filings search | `https://efts.sec.gov/LATEST/search-index` — public API |

### Target Contact: Chief Compliance Officer (CCO)
- Required by SEC — every registered firm must name a CCO on Form ADV
- Often the **managing principal wearing the CCO hat** ("CCO in name only") at smaller firms
- This is the ideal outreach target: they need compliance help but it's not their primary role
- Form ADV Section I lists the CCO name; email is often missing (9 out of 10 times per field observation)

### ICP Signals (SurgeOne.ai Fit)
- **Strong fit:** Multi-state → SEC transition, 50M-150M AUM, growing client count, no dedicated CCO
- **AUM thresholds:** State < $100M < SEC; firms near the boundary are transitioning
- **State registration count:** 4+ state registrations = catalyst for SEC move
- **Growth trajectory:** Compare current AUM against prior filings

## Sibling Project Reference

**Surge Contact Research** (`../Surge Contact Research/`) is a related but separate project that:
- Downloads SEC FOIA ZIP files and parses the CSV data
- Scores firms against SurgeOne's ICP using website keyword analysis
- Enriches contacts via Hunter.io + website scraping
- Runs as a Streamlit dashboard

This project may reuse patterns from that codebase (especially SEC data parsing and contact enrichment) but has a distinct scope: the **full multi-step pipeline** from SEC database discovery through contact-level research, designed for bulk monthly processing.

## Architecture

**UI:** Streamlit dashboard (consistent with other BGC projects)
**Framework:** WAT (inherited from `../CLAUDE.md`)

### Two-Track Pipeline

**Track A: 120-Day Approval Firms** (firms already filing for SEC registration)
```
SEC FOIA CSV → filter status="120-day approval"
    → IAPD lookup → Form ADV CCO extraction
    → Hunter.io / website scraping for email
    → Streamlit dashboard + CSV export
```

**Track B: Near-Threshold State Firms** (firms approaching SEC transition)
```
SEC FOIA CSV → filter state-registered + AUM $90M-$100M+
    → Check state registration count (4+ = catalyst)
    → IAPD lookup → Form ADV details
    → Contact discovery (same fallback chain)
    → Streamlit dashboard + CSV export
```

### Contact Discovery Waterfall
1. **Form ADV Section I** — CCO name + email (email missing ~90% of the time)
2. **Hunter.io Domain Search** — email from firm's website domain (free tier: 50/month)
3. **Website Scraping** — scrape firm homepage + subpages (/contact, /about, /team, /leadership) for emails, phones, bios
4. *(Future: Lemlist People Database — when API access confirmed)*

### Key Technical Considerations
- SEC FOIA CSVs use **latin-1 encoding** and contain ~448 columns; only ~16 are relevant
- IAPD does not have a public bulk API — scraping or structured queries required
- Form ADV CCO email field is frequently empty; multi-source fallback is essential
- Rate limiting needed for any web scraping (SEC, IAPD, firm websites)
- Hunter.io free tier: 50 searches/month — budget carefully or use alternatives
- Near-threshold detection requires AUM parsing + state registration count from Form ADV Section D

## Commands

```bash
# Run the Streamlit app
streamlit run app.py

# Run a tool standalone
python tools/<script_name>.py

# Run tests
python -m pytest tests/
python -m pytest tests/test_<tool_name>.py -v

# Check available tools and workflows
ls tools/
ls workflows/
```

## Quality Standards (from App Audit Checklist)

This project should pass the standard codebase audit (`../app-audit.md.txt`). Key requirements:

### Security
- All secrets in `.env` only; `.env` in `.gitignore`
- `.env.example` documenting required variables
- Input validation on any user-facing surfaces
- No hardcoded API keys, tokens, or passwords

### Code Quality
- Every tool in `tools/` has a corresponding test in `tests/`
- Mock external APIs in tests (SEC, Hunter.io, IAPD) — no paid calls without approval
- Consistent error handling with graceful degradation
- No dead code, unused imports, or commented-out blocks

### Documentation
- This CLAUDE.md kept current as architecture evolves
- Workflows in `workflows/` document each pipeline step
- Version tracking in the app

### Deployment
- Clean `.gitignore` (`.env`, `__pycache__`, `.tmp/`, OS files)
- Clear setup instructions for fresh clone
- Build/run scripts documented
