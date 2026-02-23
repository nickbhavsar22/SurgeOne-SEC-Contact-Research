# SurgeOne — SEC & Contact Research

Automates discovery of newly SEC-registered RIAs (Registered Investment Advisors) and their key contacts for compliance advisory outreach. Identifies firms in the 120-day SEC approval window and near-threshold state-registered firms approaching SEC transition.

## Architecture

Two-track pipeline built on the WAT framework (Workflows, Agents, Tools):

- **Track A — 120-Day Approval Firms:** SEC FOIA CSV → filter active filers → IAPD/EDGAR lookup → CCO extraction → contact enrichment
- **Track B — Near-Threshold Firms:** SEC FOIA CSV → filter state-registered firms with AUM near $100M → state registration count check → contact discovery

### Contact Discovery Waterfall

1. Form ADV / EDGAR filings (CCO name + email)
2. Hunter.io domain search (email from firm website)
3. Website scraping (contact pages, team pages)

## Setup

### Prerequisites

- Python 3.10+
- A [Hunter.io](https://hunter.io) API key (for contact enrichment)

### Installation

```bash
git clone https://github.com/<your-username>/SurgeOne-SEC-Contact-Research.git
cd SurgeOne-SEC-Contact-Research
pip install -r requirements.txt
```

### Configuration

Copy the example environment file and add your API keys:

```bash
cp .env.example .env
```

Edit `.env` and fill in your keys:

```
HUNTER_API_KEY=your_key_here
```

### Run

```bash
streamlit run app.py
```

### Run Tests

```bash
python -m pytest tests/ -v
```

## Project Structure

```
app.py              # Streamlit dashboard (main entry point)
tools/              # Deterministic Python scripts for each pipeline stage
  cache_db.py       #   SQLite caching and database operations
  fetch_sec_data.py #   SEC FOIA CSV parsing and import
  parse_form_adv.py #   EDGAR filing CCO extraction
  query_iapd.py     #   IAPD API queries
  enrich_contacts.py#   Hunter.io + website scraping
  score_firms.py    #   ICP fit scoring
  validate_contacts.py# Contact quality validation
tests/              # Test suite (mirrors tools/)
workflows/          # Markdown SOPs for pipeline stages
.env.example        # Environment variable template
```

See [CLAUDE.md](CLAUDE.md) for detailed architecture, domain knowledge, and development guidelines.

## License

[MIT](LICENSE)
