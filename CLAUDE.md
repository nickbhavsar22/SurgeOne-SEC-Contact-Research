# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

**SurgeOne — SEC & Contact Research** automates the discovery of newly SEC-registered RIAs (Registered Investment Advisors) and their key contacts for compliance advisory outreach by SurgeOne.ai.

The core workflow: identify firms in the 120-day SEC registration approval window, extract all contacts (names + titles) from their Form ADV PDFs, enrich with email/phone via Hunter.io, and export for outreach.

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
| **Form ADV PDFs** | Contact names, titles (Principal/Owner, CCO, officers) | `reports.adviserinfo.sec.gov/reports/ADV/{CRD}/PDF/{CRD}.pdf` — public |
| **Hunter.io** | Email + phone lookup by person name + company domain | API key required, 2,000 credits/month |

### Target Contacts

- **Chief Compliance Officer (CCO)** — required by SEC, often the managing principal at smaller firms
- **Principal/Owner** — listed in Form ADV Part 1A
- **Other officers/directors** — from Schedule A/B of Form ADV
- Form ADV PDFs contain names and titles but NOT email/phone — Hunter.io fills that gap

## Sibling Project Reference

**Surge Contact Research** (`../Surge Contact Research/`) is a related but separate project that:
- Downloads SEC FOIA ZIP files and parses the CSV data
- Scores firms against SurgeOne's ICP using website keyword analysis
- Enriches contacts via Hunter.io + website scraping
- Runs as a Streamlit dashboard

This project may reuse patterns from that codebase (especially SEC data parsing and contact enrichment) but has a distinct scope: the **full multi-step pipeline** from SEC database discovery through contact-level research, designed for bulk monthly processing.

## Architecture

**UI:** Streamlit single-page dashboard (dark theme)
**Framework:** WAT (inherited from `../CLAUDE.md`)

### Two-Stage Pipeline

```
Stage 1: Import SEC Data
  Upload SEC FOIA ZIP/CSV → parse → filter to 120-day approvals → store in DB

Stage 2: Research Firms (user sets batch size)
  For each firm:
    1. Download Form ADV PDF → extract ALL contacts (names + titles) via pdfplumber
    2. For each contact without email → Hunter.io Email Finder (1 credit each)
    3. Mark firm as processed (cached for 30 days)

Export: CSV with all contacts joined with firm data
```

### Key Technical Considerations

- SEC FOIA CSVs use **latin-1 encoding** and contain ~448 columns; only ~17 are relevant
- Form ADV PDFs contain names/titles but NOT email/phone — Hunter.io fills the gap
- Multiple contacts per firm (not just the "best" one)
- Hunter.io paid plan: 2,000 credits/month — per-batch credit limit configurable in UI
- Rate limiting: 1 req/sec for SEC PDF downloads
- Caching: firms processed within 30 days are automatically skipped

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
- **Auto-push:** After every commit in this project, always push to GitHub (`git push`). Streamlit Cloud auto-redeploys from the repo.
- **Version bumping:** On every commit that changes app behavior or UI, bump `APP_VERSION` in `app.py` (line ~29). Use semver: patch for fixes, minor for features/UI changes, major for breaking changes.
