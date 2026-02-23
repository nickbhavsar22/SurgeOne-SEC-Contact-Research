"""
SurgeOne — SEC & Contact Research Dashboard

Streamlit app for discovering newly SEC-registered RIAs and their key contacts.
Two-track pipeline:
  Track A: 120-day approval firms (already filing for SEC registration)
  Track B: Near-threshold state firms (AUM >= $90M, approaching SEC transition)
"""

import io
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from tools.cache_db import (
    init_db, get_firms, get_pipeline_stage_stats, get_enrichment_stats,
    get_export_history, log_export, get_monthly_hunter_credits,
)
from tools.fetch_sec_data import fetch_and_store, probe_sec_urls
from tools.query_iapd import query_firms_batch
from tools.score_firms import score_batch
from tools.enrich_contacts import enrich_batch, HUNTER_API_KEY, DEFAULT_BATCH_CREDIT_LIMIT
from tools.parse_form_adv import extract_cco_batch
from tools.validate_contacts import validate_batch

APP_VERSION = "0.3.1"
LOGO_PATH = Path(__file__).parent / "assets" / "logo.png"

# --- Page Config ---
st.set_page_config(
    page_title="SurgeOne — SEC & Contact Research",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    init_db()
    stats = get_pipeline_stage_stats()

    # --- Sidebar ---
    with st.sidebar:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=200)
        else:
            st.title("SurgeOne")

        st.markdown("**SurgeOne**")
        st.caption("SEC & Contact Research")
        st.caption(f"v{APP_VERSION} · Powered by Bhavsar Growth Consulting")
        st.divider()

        page = st.radio(
            "Navigation",
            ["Pipeline", "Firm Explorer", "API Usage", "Export"],
            label_visibility="collapsed",
        )

        # Compact pipeline health
        st.divider()
        st.caption("Pipeline Health")
        c1, c2, c3 = st.columns(3)
        c1.metric("Firms", stats['total_firms'])
        c2.metric("Enriched", stats['enriched'])
        c3.metric("Valid", stats['valid'])

    # --- Page Router ---
    if page == "Pipeline":
        page_pipeline(stats)
    elif page == "Firm Explorer":
        page_explorer(stats)
    elif page == "API Usage":
        page_api_usage()
    elif page == "Export":
        page_export()


# ============================================================
# PAGE: Pipeline (merged overview + run)
# ============================================================

def page_pipeline(stats):
    st.header("Pipeline")

    # --- Funnel metrics row ---
    cols = st.columns(6)
    cols[0].metric("Imported", stats['total_firms'])
    cols[1].metric("Track A", stats['by_track'].get('A', 0))
    cols[2].metric("Track B", stats['by_track'].get('B', 0))
    cols[3].metric("Scored", stats['scored'])
    cols[4].metric("Enriched", stats['enriched'])
    cols[5].metric("Validated", stats['valid'])

    # --- Next-step guidance ---
    _render_guidance(stats)

    st.divider()

    # --- Stage states ---
    stages = _get_stage_states(stats)

    # --- Stage 1: SEC Import ---
    _render_stage_header(1, "Import SEC FOIA Data", stages[1],
                         "Loads the SEC's monthly investment adviser CSV and identifies target firms in two tracks.")
    with st.expander("What happens & what to do"):
        st.markdown("""
**What this does:** Parses the SEC FOIA investment adviser CSV (~16K rows, ~448 columns) and filters for two target groups:
- **Track A (120-Day Approval):** Firms actively filing for SEC registration — they're in the 45-day approval window and need compliance help now.
- **Track B (Near-Threshold):** State-registered firms with AUM near $90-100M — they'll need to transition to SEC registration soon.

**What you need to do:** Upload the CSV from [sec.gov/foia](https://www.sec.gov/foia), use pre-downloaded data if available, or try auto-detection. Takes ~30 seconds.

📡 **Data Sources:**
- [SEC Investment Adviser Data](https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers) — landing page with downloadable ZIP/CSV files
- [IAPD Compilation Page](https://adviserinfo.sec.gov/compilation) — newer download location (Jan 2025+)
- Download pattern: `ia{mmddyy}.zip` files from the SEC data directory
""")
    if stages[1]['enabled']:
        _stage_1_content()

    st.divider()

    # --- Stage 2: IAPD Lookup ---
    _render_stage_header(2, "IAPD Form ADV Lookup", stages[2],
                         "Queries adviserinfo.sec.gov for each firm's state registrations and Form ADV details.")
    with st.expander("What happens & what to do"):
        st.markdown("""
**What this does:** Calls the IAPD search API for each firm to extract:
- State registrations (which states the firm operates in)
- Notice filings (active state licenses)

Note: CCO name/email data is rarely available from this API endpoint — contact discovery happens in Stage 4.

**What you need to do:** Select a track (or Both) and click Run. Rate-limited to ~1 request/second, so expect **~1 second per firm**. Safe to re-run — already-queried firms (within 30 days) are skipped.

📡 **Data Sources:**
- [IAPD Search API](https://api.adviserinfo.sec.gov/) — JSON API returning firm registration details (queried per CRD number)
- [IAPD Firm Viewer](https://adviserinfo.sec.gov/firm/summary/) — human-readable Form ADV detail page (append CRD to URL)
""")
    if stages[2]['enabled']:
        _stage_2_content()
    else:
        st.caption("Import firms first to unlock this stage.")

    st.divider()

    # --- Stage 3: Scoring ---
    _render_stage_header(3, "ICP Fit Scoring", stages[3],
                         "Scores each firm 0-100 against SurgeOne.ai's ideal customer profile.")
    with st.expander("What happens & what to do"):
        st.markdown("""
**What this does:** Calculates a fit score (0-100) based on:
- AUM size (higher = better fit, especially $10M-$1B range)
- Employee count (3-10+ signals a growing firm that needs compliance help)
- Client count and growth signals
- State registrations (4+ states = approaching SEC transition)
- Website presence and company name keywords (advisory, wealth, etc.)

No API calls — purely data-based scoring, runs instantly.

**What you need to do:** Select a track and click Run. Firms scoring 70+ are strong outreach candidates.

📡 **Data Sources:** None — runs locally using data already collected in previous stages.
""")
    if stages[3]['enabled']:
        _stage_3_content()
    else:
        st.caption("Run IAPD lookup first to unlock this stage.")

    st.divider()

    # --- Stage 4: Enrichment ---
    _render_stage_header(4, "Contact Enrichment", stages[4],
                         "Discovers CCO and key decision-maker contact info via a multi-source waterfall.")
    with st.expander("What happens & what to do"):
        st.markdown(f"""
**What this does:** Runs a 4-step waterfall to find the best contact for each firm:
1. **Form ADV CCO** — Uses CCO name/email from SEC filings (email missing ~90% of the time)
2. **Hunter.io Domain Search** — Looks up the firm's website domain for email contacts {"**(Active)**" if HUNTER_API_KEY else "**(Not configured — add `HUNTER_API_KEY` to `.env`)**"}
3. **Website Scraping** — Scrapes the firm's homepage + 12 subpages (/contact, /about, /team, etc.) for emails, names, and titles
4. **Hunter.io Email Finder** — If a name was found but no email, tries to find the email {"**(Active)**" if HUNTER_API_KEY else "**(Not configured)**"}

Generic/role-based emails (info@, reporting@, compliance@, etc.) are automatically filtered out.

**What you need to do:** Select a track and click Run. Website scraping takes **~5-6 seconds per firm**. Safe to re-run — firms with existing contacts are skipped.

📡 **Data Sources:**
- [Hunter.io Domain Search](https://hunter.io/api-documentation#domain-search) — finds emails associated with a firm's domain
- [Hunter.io Email Finder](https://hunter.io/api-documentation#email-finder) — finds a specific person's email by name + domain
- Firm websites — scrapes homepage + /contact, /about, /team, /leadership, and 8 other subpages
""")
    if stages[4]['enabled']:
        _stage_4_content()
    else:
        st.caption("Score firms first to unlock this stage.")

    st.divider()

    # --- Stage 5: Validation ---
    _render_stage_header(5, "Contact Validation", stages[5],
                         "Checks contact data quality and flags issues before export.")
    with st.expander("What happens & what to do"):
        st.markdown("""
**What this does:** Runs 6 quality checks on each enriched contact:
1. **Email format** — Valid RFC 5322 email address
2. **Generic email detection** — Flags role-based addresses (info@, reporting@, compliance@)
3. **Domain match** — Email domain should match the firm's website
4. **Phone format** — Valid US phone number
5. **CCO name cross-reference** — Contact name should match Form ADV if available
6. **Staleness** — Flags contacts enriched more than 90 days ago

Results: **Valid** (all checks pass), **Suspect** (has email but some warnings), **Invalid** (no email or bad format).

**What you need to do:** Select a track and click Run. Instant — no API calls. Review suspect contacts in Firm Explorer before exporting.

📡 **Data Sources:** None — runs locally using data already collected in previous stages.
""")
    if stages[5]['enabled']:
        _stage_5_content()
    else:
        st.caption("Enrich contacts first to unlock this stage.")

    # --- Funnel chart ---
    st.divider()
    st.subheader("Pipeline Funnel")
    funnel_data = pd.DataFrame({
        'Stage': ['Imported', 'IAPD Queried', 'Scored', 'Enriched', 'Validated'],
        'Count': [
            stats['total_firms'],
            stats['iapd_queried'],
            stats['scored'],
            stats['enriched'],
            stats['valid'],
        ],
    })
    st.bar_chart(funnel_data.set_index('Stage'))


def _render_guidance(stats):
    """Show a next-step guidance banner based on pipeline state."""
    if stats['total_firms'] == 0:
        st.info("**Get started:** Import SEC FOIA data below to populate your firm database.")
    elif stats['iapd_queried'] == 0:
        st.info(f"**Next step:** {stats['total_firms']} firms imported. Run IAPD Form ADV lookup to discover CCOs.")
    elif stats['scored'] == 0:
        st.info(f"**Next step:** IAPD data collected for {stats['iapd_queried']} firms. Score them against SurgeOne's ICP.")
    elif stats['enriched'] == 0:
        st.info(f"**Next step:** {stats['scored']} firms scored. Enrich contacts via Hunter.io and website scraping.")
    elif stats['validated'] == 0:
        st.info(f"**Next step:** {stats['enriched']} contacts enriched. Validate data quality before export.")
    else:
        st.success(f"**Pipeline complete!** {stats['valid']} validated contacts ready. Head to **Export** to download your leads.")


def _render_stage_header(number, title, state, description):
    """Render a stage header with progress indicator."""
    if state['count'] > 0 and state['count'] >= state['total'] and state['total'] > 0:
        icon = "✅"
    elif state['count'] > 0:
        icon = "🔄"
    elif state['enabled']:
        icon = "▶️"
    else:
        icon = "🔒"

    st.subheader(f"{icon} Stage {number}: {title}")
    st.caption(description)

    if state['total'] > 0:
        progress = min(state['count'] / state['total'], 1.0)
        st.progress(progress, text=f"{state['count']:,} / {state['total']:,}")


def _stage_1_content():
    """SEC FOIA Import — upload CSV/ZIP or auto-detect from SEC.gov."""
    import tempfile

    # Check for sibling project's pre-downloaded data
    sibling_csv = Path(__file__).parent.parent / "Surge Contact Research" / "data" / "sec_advisers.csv"

    # Option A: Upload file
    uploaded = st.file_uploader(
        "Upload SEC FOIA CSV or ZIP",
        type=["csv", "zip"],
        key="sec_upload",
        help="Download the file from sec.gov/foia in your browser, then upload it here.",
    )

    if uploaded is not None:
        if st.button("Import Uploaded File", type="primary", key="btn_import_upload"):
            with st.spinner(f"Parsing {uploaded.name}..."):
                # Save to temp file for load_local_csv
                suffix = ".zip" if uploaded.name.endswith(".zip") else ".csv"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uploaded.getvalue())
                    tmp_path = tmp.name
                result = fetch_and_store(csv_path=tmp_path)
            if result.get('error'):
                st.error(f"Import failed: {result['error']}")
            else:
                st.success(
                    f"Imported {result['downloaded']:,} firms — "
                    f"**{result['track_a']}** Track A, **{result['track_b']}** Track B, "
                    f"{result['skipped']:,} skipped"
                )
                st.rerun()

    # Option B: Use sibling project data if available
    if sibling_csv.exists():
        st.divider()
        import os
        mod_time = datetime.fromtimestamp(os.path.getmtime(sibling_csv))
        st.caption(f"Pre-downloaded SEC data found (updated {mod_time.strftime('%b %d, %Y')})")
        if st.button("Import from Pre-Downloaded Data", key="btn_import_sibling"):
            with st.spinner("Parsing pre-downloaded SEC data..."):
                result = fetch_and_store(csv_path=str(sibling_csv))
            if result.get('error'):
                st.error(f"Import failed: {result['error']}")
            else:
                st.success(
                    f"Imported {result['downloaded']:,} firms — "
                    f"**{result['track_a']}** Track A, **{result['track_b']}** Track B, "
                    f"{result['skipped']:,} skipped"
                )
                st.rerun()

    # Option C: Auto-detect from SEC.gov
    with st.expander("Download from SEC.gov"):
        st.caption("SEC.gov may block automated downloads. If this fails, download the file manually and upload above.")
        if st.button("Check Available Files", key="btn_probe", type="secondary"):
            with st.spinner("Probing SEC.gov for available files..."):
                results = probe_sec_urls()
            st.session_state['sec_probe_results'] = results

        if 'sec_probe_results' in st.session_state:
            results = st.session_state['sec_probe_results']
            available = [r for r in results if r['available']]

            if not available:
                st.warning("No files detected. SEC.gov may be blocking automated requests. Download manually instead.")
            else:
                options = {}
                for r in available:
                    size_str = f" — {r['size_mb']} MB" if r['size_mb'] else ""
                    label = f"{r['date_label']}{size_str}"
                    options[label] = r['url']

                selected_label = st.radio(
                    "Select a file to import:",
                    list(options.keys()),
                    key="sec_file_select",
                )

                if st.button("Download & Import", type="primary", key="btn_import_url"):
                    url = options[selected_label]
                    with st.spinner(f"Downloading {selected_label}..."):
                        result = fetch_and_store(url=url)
                    if result.get('error'):
                        st.error(f"Import failed: {result['error']}")
                    else:
                        st.success(
                            f"Imported {result['downloaded']:,} firms — "
                            f"**{result['track_a']}** Track A, **{result['track_b']}** Track B, "
                            f"{result['skipped']:,} skipped"
                        )
                        del st.session_state['sec_probe_results']
                        st.rerun()


def _stage_2_content():
    """IAPD Form ADV Lookup."""
    col1, col2 = st.columns([3, 1])
    with col1:
        adv_track = st.selectbox("Track to query", ["A", "B", "Both"], key="adv_track")
    with col2:
        st.write("")
        st.write("")
        run_adv = st.button("Run IAPD Lookup", key="btn_adv", type="primary")

    if run_adv:
        firms = _get_crd_list(adv_track)
        if not firms:
            st.warning("No firms found for this track.")
        else:
            with st.spinner(f"Querying IAPD for {len(firms)} firms (1 req/sec)..."):
                result = query_firms_batch(firms)
            st.success(
                f"Queried **{result['queried']}**, "
                f"cached **{result['cached']}**, "
                f"errors **{result['errors']}**"
            )
            st.rerun()

    # --- CCO Extraction from EDGAR ---
    st.divider()
    st.subheader("EDGAR CCO Extraction")
    st.caption(
        "Searches SEC EDGAR filings (13F-HR, Form D) for Chief Compliance Officer "
        "names and phone numbers. Extracted CCO names are used by Step 4 of the "
        "contact enrichment waterfall to find emails via Hunter.io."
    )
    st.markdown(
        "📡 **Data Sources:** "
        "[EDGAR Full-Text Search](https://efts.sec.gov/LATEST/search-index) — "
        "searches 13F-HR and Form D filings for \"compliance officer\" · "
        "[EDGAR Filing Archives](https://www.sec.gov/cgi-bin/browse-edgar) — "
        "individual filing documents (XML/HTML)"
    )

    col_a, col_b = st.columns([3, 1])
    with col_a:
        cco_track = st.selectbox(
            "Track to extract CCOs", ["A", "B", "Both"], key="cco_track",
        )
    with col_b:
        st.write("")
        st.write("")
        run_cco = st.button("Extract CCO Names", key="btn_cco", type="primary")

    # Show results from previous run
    if 'last_cco_result' in st.session_state:
        result = st.session_state.pop('last_cco_result')
        st.success(
            f"Extracted **{result['extracted']}**, "
            f"cached **{result['cached']}**, "
            f"no result **{result['no_result']}**, "
            f"errors **{result['errors']}**"
        )

    if run_cco:
        crd_company_pairs = _get_crd_company_list(cco_track)
        if not crd_company_pairs:
            st.warning("No firms found for this track.")
        else:
            progress_bar = st.progress(0, text="Starting CCO extraction...")
            status_text = st.empty()
            start_time = time.time()

            def _on_cco_progress(current, total, res):
                progress_bar.progress(
                    current / total,
                    text=f"Searching EDGAR for firm {current} / {total}",
                )
                elapsed = time.time() - start_time
                status_text.text(
                    f"Extracted: {res['extracted']} | Cached: {res['cached']} | "
                    f"No result: {res['no_result']} | Errors: {res['errors']} | "
                    f"Elapsed: {elapsed:.0f}s"
                )

            result = extract_cco_batch(
                crd_company_pairs, progress_callback=_on_cco_progress,
            )
            elapsed = time.time() - start_time
            progress_bar.progress(1.0, text=f"CCO extraction complete! ({elapsed:.0f}s)")
            status_text.empty()
            st.session_state['last_cco_result'] = result
            st.rerun()


def _stage_3_content():
    """ICP Fit Scoring."""
    col1, col2 = st.columns([3, 1])
    with col1:
        score_track = st.selectbox("Track to score", ["A", "B", "Both"], key="score_track")
    with col2:
        st.write("")
        st.write("")
        run_score = st.button("Run Scoring", key="btn_score", type="primary")

    if run_score:
        firms = _get_crd_list(score_track)
        if not firms:
            st.warning("No firms found for this track.")
        else:
            with st.spinner(f"Scoring {len(firms)} firms..."):
                result = score_batch(firms)
            st.success(
                f"Scored **{result['scored']}** firms, "
                f"errors **{result['errors']}**"
            )
            st.rerun()


def _stage_4_content():
    """Contact Enrichment."""
    if HUNTER_API_KEY:
        monthly_credits = get_monthly_hunter_credits()
        st.success(
            f"Hunter.io: **Active** (paid plan) — "
            f"{monthly_credits:,} / 2,000 credits used this month."
        )
    else:
        st.warning(
            "Hunter.io: **Not configured** — enrichment uses Form ADV + website scraping only. "
            "Add `HUNTER_API_KEY` to `.env` for better email discovery. "
            "[Get an API key](https://hunter.io/)"
        )

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        enrich_track = st.selectbox("Track to enrich", ["A", "B", "Both"], key="enrich_track")
    with col2:
        batch_credit_limit = st.number_input(
            "Hunter.io credit limit (this batch)",
            min_value=0,
            max_value=2000,
            value=DEFAULT_BATCH_CREDIT_LIMIT,
            step=10,
            key="batch_credit_limit",
            help="Max Hunter.io credits for this run. 0 = no limit. Each firm uses 0-2 credits.",
        )
    with col3:
        st.write("")
        st.write("")
        run_enrich = st.button("Run Enrichment", key="btn_enrich", type="primary")

    # Show results from previous run (persisted across rerun)
    if 'last_enrich_result' in st.session_state:
        result = st.session_state.pop('last_enrich_result')
        st.success(
            f"Enriched **{result['enriched']}**, "
            f"cached **{result['cached']}**, "
            f"no result **{result['no_result']}**, "
            f"errors **{result['errors']}**"
        )
        if result.get('credits_used', 0) > 0 or HUNTER_API_KEY:
            limit_label = result.get('credit_limit') or 'unlimited'
            st.info(f"Hunter.io credits used this batch: **{result['credits_used']}** / {limit_label}")
        if result.get('credit_limit_hit'):
            st.warning(
                f"Credit limit reached! **{result['skipped_credit_limit']}** firms were skipped. "
                f"Run enrichment again to continue (skipped firms will be picked up)."
            )

    if run_enrich:
        firms = _get_crd_list(enrich_track)
        if not firms:
            st.warning("No firms found for this track.")
        else:
            progress_bar = st.progress(0, text="Starting enrichment...")
            status_text = st.empty()
            start_time = time.time()

            def _on_progress(current, total, res):
                progress_bar.progress(
                    current / total,
                    text=f"Processing firm {current} / {total}",
                )
                elapsed = time.time() - start_time
                status_text.text(
                    f"Enriched: {res['enriched']} | Cached: {res['cached']} | "
                    f"No result: {res['no_result']} | Errors: {res['errors']} | "
                    f"Credits: {res['credits_used']} | "
                    f"Elapsed: {elapsed:.0f}s"
                )

            result = enrich_batch(
                firms, credit_limit=batch_credit_limit,
                progress_callback=_on_progress,
            )
            elapsed = time.time() - start_time
            progress_bar.progress(1.0, text=f"Enrichment complete! ({elapsed:.0f}s)")
            status_text.empty()
            st.session_state['last_enrich_result'] = result
            st.rerun()


def _stage_5_content():
    """Contact Validation."""
    col1, col2 = st.columns([3, 1])
    with col1:
        val_track = st.selectbox("Track to validate", ["A", "B", "Both"], key="val_track")
    with col2:
        st.write("")
        st.write("")
        run_validate = st.button("Run Validation", key="btn_validate", type="primary")

    if run_validate:
        firms = _get_crd_list(val_track)
        if not firms:
            st.warning("No firms found for this track.")
        else:
            with st.spinner(f"Validating contacts for {len(firms)} firms..."):
                result = validate_batch(firms)
            st.success(
                f"Valid **{result['valid']}**, "
                f"suspect **{result['suspect']}**, "
                f"invalid **{result['invalid']}**, "
                f"no contact **{result['no_contact']}**"
            )
            st.rerun()


# ============================================================
# PAGE: Firm Explorer
# ============================================================

def page_explorer(stats):
    st.header("Firm Explorer")

    if stats['total_firms'] == 0:
        st.info("No firms in the database yet. Head to **Pipeline** to import SEC data first.")
        return

    # Search bar
    search_query = st.text_input(
        "Search by company name or CRD",
        placeholder="e.g. 'Acme Financial' or '123456'",
        key="search_query",
    )

    # Filter row
    col1, col2, col3 = st.columns(3)
    with col1:
        track_filter = st.selectbox("Track", ["All", "A — 120-Day", "B — Near-Threshold"])
    with col2:
        min_score = st.slider("Min Fit Score", 0, 100, 0)
    with col3:
        contact_filter = st.selectbox("Contact Status", ["All", "Has Email", "Missing Email"])

    # Explicit search action
    if st.button("Search Firms", type="primary", key="btn_search"):
        track = None
        if track_filter.startswith("A"):
            track = "A"
        elif track_filter.startswith("B"):
            track = "B"

        has_contact = None
        if contact_filter == "Has Email":
            has_contact = True
        elif contact_filter == "Missing Email":
            has_contact = False

        score_val = min_score if min_score > 0 else None

        firms = get_firms(track=track, min_score=score_val, has_contact=has_contact)

        # Client-side text filter
        if search_query.strip():
            q = search_query.strip().lower()
            firms = [
                f for f in firms
                if q in (f.get('company') or '').lower()
                or q == str(f.get('crd', ''))
            ]

        st.session_state['explorer_results'] = firms

    # Display results
    if 'explorer_results' in st.session_state:
        firms = st.session_state['explorer_results']
        if not firms:
            st.warning("No firms match your search criteria.")
        else:
            st.caption(f"{len(firms)} firms found")
            _render_firms_table(firms)
    else:
        st.info("Set your filters and click **Search Firms** to find leads.")


def _render_firms_table(firms):
    """Render the firms data table."""
    display_cols = [
        'crd', 'company', 'track', 'state', 'aum', 'employees', 'clients',
        'fit_score', 'first_name', 'last_name', 'contact_email', 'contact_title',
    ]
    df = pd.DataFrame(firms)
    available_cols = [c for c in display_cols if c in df.columns]
    df_display = df[available_cols].copy()

    if 'aum' in df_display.columns:
        df_display['aum'] = df_display['aum'].apply(_format_aum)

    st.dataframe(
        df_display,
        width='stretch',
        hide_index=True,
        column_config={
            'crd': st.column_config.NumberColumn('CRD', format='%d'),
            'company': 'Company',
            'track': 'Track',
            'state': 'State',
            'aum': 'AUM',
            'employees': 'Employees',
            'clients': 'Clients',
            'fit_score': st.column_config.ProgressColumn('Fit Score', min_value=0, max_value=100),
            'first_name': 'First Name',
            'last_name': 'Last Name',
            'contact_email': 'Email',
            'contact_title': 'Title',
        },
    )


# ============================================================
# PAGE: API Usage
# ============================================================

def page_api_usage():
    st.header("API Usage & Credits")

    if not HUNTER_API_KEY:
        st.info(
            "**Hunter.io is not configured.** No API key is set in `.env`. "
            "Hunter.io enables email discovery for firms where Form ADV and website scraping "
            "don't find a contact. [Get an API key](https://hunter.io/)."
        )

    stats = get_enrichment_stats()
    if not stats:
        st.info("No API calls logged yet. Run the pipeline to see usage data.")
        return

    df = pd.DataFrame(stats)
    df = df.rename(columns={
        'api_source': 'API Source',
        'total_calls': 'Total Calls',
        'total_credits': 'Credits Used',
        'successes': 'Successes',
        'not_found': 'Not Found',
        'errors': 'Errors',
    })
    st.dataframe(df, width='stretch', hide_index=True)

    hunter_row = next((s for s in stats if s['api_source'] == 'hunter_io'), None)
    if hunter_row or HUNTER_API_KEY:
        monthly_credits = get_monthly_hunter_credits()
        monthly_limit = 2000

        st.subheader("Hunter.io Monthly Usage")
        progress = min(monthly_credits / monthly_limit, 1.0)
        st.progress(progress, text=f"{monthly_credits:,} / {monthly_limit:,} credits used this month")

        if monthly_credits >= 1800:
            st.error(f"Approaching monthly limit! {monthly_limit - monthly_credits} credits remaining.")
        elif monthly_credits >= 1000:
            st.warning(f"{monthly_limit - monthly_credits} credits remaining this month.")
        else:
            st.caption(f"{monthly_limit - monthly_credits} credits remaining this month.")

        st.caption(
            f"Per-batch default limit: {DEFAULT_BATCH_CREDIT_LIMIT} credits "
            f"(configurable in Stage 4)"
        )


# ============================================================
# PAGE: Export
# ============================================================

def page_export():
    st.header("Export to CSV")

    col1, col2, col3 = st.columns(3)
    with col1:
        export_track = st.selectbox("Track", ["All", "A", "B"], key="export_track")
    with col2:
        export_min_score = st.slider("Min Fit Score", 0, 100, 0, key="export_score")
    with col3:
        export_contact = st.selectbox("Contact", ["All", "Has Email", "Missing Email"], key="export_contact")

    track = export_track if export_track != "All" else None
    score_val = export_min_score if export_min_score > 0 else None
    has_contact = None
    if export_contact == "Has Email":
        has_contact = True
    elif export_contact == "Missing Email":
        has_contact = False

    firms = get_firms(track=track, min_score=score_val, has_contact=has_contact)

    st.caption(f"{len(firms)} firms match filters")

    if not firms:
        st.info("No firms to export. Run the pipeline first.")
        return

    export_cols = [
        'crd', 'company', 'legal_name', 'track', 'status', 'state', 'city',
        'phone', 'website', 'aum', 'employees', 'clients',
        'fit_score', 'fit_reasons',
        'contact_name', 'first_name', 'last_name',
        'contact_email', 'contact_title', 'contact_phone',
    ]
    df = pd.DataFrame(firms)
    available = [c for c in export_cols if c in df.columns]
    df_export = df[available]

    st.subheader("Preview")
    st.dataframe(df_export.head(20), width='stretch', hide_index=True)

    csv_buffer = io.StringIO()
    df_export.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"surgeone_leads_{timestamp}.csv"
    filters_desc = f"track={track or 'all'}, min_score={score_val or 0}, contact={export_contact}"

    if st.download_button(
        label=f"Download CSV ({len(firms)} records)",
        data=csv_data,
        file_name=filename,
        mime="text/csv",
        type="primary",
    ):
        log_export(filename, len(firms), filters_desc)
        st.success(f"Exported {len(firms)} records to {filename}")


# ============================================================
# Helpers
# ============================================================

def _get_stage_states(stats):
    """Determine enabled/completed state for each pipeline stage."""
    total = stats['total_firms']
    return {
        1: {'enabled': True, 'count': total, 'total': total},
        2: {'enabled': total > 0,
            'count': stats['iapd_queried'], 'total': total},
        3: {'enabled': stats['iapd_queried'] > 0,
            'count': stats['scored'], 'total': total},
        4: {'enabled': stats['scored'] > 0,
            'count': stats['enriched'], 'total': total},
        5: {'enabled': stats['enriched'] > 0,
            'count': stats['validated'], 'total': stats['enriched']},
    }


def _get_crd_list(track_selection):
    """Get CRD list for a track selection (A, B, or Both)."""
    if track_selection == "Both":
        firms_a = get_firms(track="A")
        firms_b = get_firms(track="B")
        firms = firms_a + firms_b
    else:
        firms = get_firms(track=track_selection)
    return [f['crd'] for f in firms]


def _get_crd_company_list(track_selection):
    """Get list of (crd, company) tuples for a track selection."""
    if track_selection == "Both":
        firms_a = get_firms(track="A")
        firms_b = get_firms(track="B")
        firms = firms_a + firms_b
    else:
        firms = get_firms(track=track_selection)
    return [(f['crd'], f.get('company', '')) for f in firms]


def _format_aum(val):
    """Format AUM as a readable dollar amount."""
    if val is None or pd.isna(val):
        return "—"
    val = float(val)
    if val >= 1_000_000_000:
        return f"${val / 1_000_000_000:.1f}B"
    elif val >= 1_000_000:
        return f"${val / 1_000_000:.0f}M"
    elif val >= 1_000:
        return f"${val / 1_000:.0f}K"
    elif val > 0:
        return f"${val:,.0f}"
    return "—"


if __name__ == "__main__":
    main()
