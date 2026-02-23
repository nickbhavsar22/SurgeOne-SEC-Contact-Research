"""
SurgeOne — SEC & Contact Research Dashboard

Streamlit app for discovering newly SEC-registered RIAs (120-day approval firms)
and extracting their key contacts from Form ADV PDFs + Hunter.io enrichment.

Simplified 2-stage pipeline:
  1. Import SEC FOIA data → filter to 120-day approvals
  2. Research firms: read Form ADV PDF → extract contacts → Hunter.io email lookup
"""

import io
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from tools.cache_db import (
    init_db, get_firms, get_pipeline_stats, get_enrichment_stats,
    get_all_contacts_with_firms, get_contact_stats, log_export,
    get_monthly_hunter_credits, get_unprocessed_crds,
)
from tools.fetch_sec_data import fetch_and_store, probe_sec_urls
from tools.enrich_contacts import (
    research_firms_batch, HUNTER_API_KEY, DEFAULT_BATCH_CREDIT_LIMIT,
)

APP_VERSION = "0.5.1"
LOGO_PATH = Path(__file__).parent / "assets" / "logo.png"

# --- Page Config ---
st.set_page_config(
    page_title="SurgeOne — SEC & Contact Research",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _inject_custom_css():
    """Inject custom CSS for Stakent-inspired dark theme."""
    st.markdown("""
    <style>
    /* --- Inter font --- */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* --- Metric cards --- */
    [data-testid="stMetric"] {
        background: #181820;
        border: 1px solid #2A2A35;
        border-radius: 12px;
        padding: 0.8rem 1rem;
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }
    [data-testid="stMetric"]:hover {
        border-color: #7C5CFC;
        box-shadow: 0 0 12px rgba(124, 92, 252, 0.12);
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-weight: 600;
        font-size: 1.4rem;
    }
    [data-testid="stMetric"] [data-testid="stMetricLabel"] {
        color: #8B8B9E;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }

    /* --- Primary buttons --- */
    button[kind="primary"] {
        background: #7C5CFC !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: background 0.2s ease, box-shadow 0.2s ease !important;
    }
    button[kind="primary"]:hover {
        background: #9B7FFF !important;
        box-shadow: 0 0 16px rgba(124, 92, 252, 0.3) !important;
    }
    button[kind="secondary"] {
        border: 1px solid #2A2A35 !important;
        border-radius: 8px !important;
        background: transparent !important;
        transition: border-color 0.2s ease !important;
    }
    button[kind="secondary"]:hover {
        border-color: #7C5CFC !important;
    }

    /* --- Progress bars --- */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #7C5CFC, #9B7FFF) !important;
        border-radius: 8px !important;
    }
    .stProgress > div > div {
        background: #2A2A35 !important;
        border-radius: 8px !important;
    }

    /* --- Sidebar --- */
    section[data-testid="stSidebar"] {
        background: #131318 !important;
        border-right: 1px solid #2A2A35 !important;
    }

    /* --- Expanders --- */
    [data-testid="stExpander"] {
        background: #181820;
        border: 1px solid #2A2A35;
        border-radius: 12px;
        overflow: hidden;
    }
    [data-testid="stExpander"] summary:hover {
        color: #F0F0F0;
    }

    /* --- Alert boxes --- */
    .stAlert > div {
        border-radius: 10px !important;
        border-left-width: 4px !important;
    }

    /* --- Dividers --- */
    hr {
        border-color: #2A2A35 !important;
        opacity: 0.6;
    }

    /* --- Selectboxes & inputs --- */
    [data-testid="stSelectbox"] > div > div,
    [data-testid="stNumberInput"] > div > div > input,
    [data-testid="stTextInput"] > div > div > input {
        border: 1px solid #2A2A35 !important;
        border-radius: 8px !important;
        background: #181820 !important;
    }
    [data-testid="stSelectbox"] > div > div:focus-within,
    [data-testid="stTextInput"] > div > div:focus-within {
        border-color: #7C5CFC !important;
        box-shadow: 0 0 0 1px #7C5CFC !important;
    }

    /* --- File uploader --- */
    [data-testid="stFileUploader"] > div {
        border: 1px dashed #2A2A35 !important;
        border-radius: 12px !important;
        background: #181820 !important;
    }
    [data-testid="stFileUploader"] > div:hover {
        border-color: #7C5CFC !important;
    }

    /* --- Dataframes --- */
    [data-testid="stDataFrame"] {
        border: 1px solid #2A2A35 !important;
        border-radius: 12px !important;
        overflow: hidden;
    }

    /* --- Slider thumb --- */
    [data-testid="stSlider"] [role="slider"] {
        background: #7C5CFC !important;
    }

    /* --- Download button --- */
    [data-testid="stDownloadButton"] button {
        background: #7C5CFC !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
    }
    [data-testid="stDownloadButton"] button:hover {
        background: #9B7FFF !important;
        box-shadow: 0 0 16px rgba(124, 92, 252, 0.3) !important;
    }

    /* --- Subheader spacing --- */
    h2 {
        margin-top: 0.5rem !important;
        font-weight: 600 !important;
    }
    h3 {
        font-weight: 600 !important;
    }
    </style>
    """, unsafe_allow_html=True)


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


def main():
    init_db()
    _inject_custom_css()
    stats = get_pipeline_stats()
    contact_stats = get_contact_stats()

    # --- Sidebar ---
    with st.sidebar:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=200)
        else:
            st.title("SurgeOne")

        st.markdown("**SurgeOne**")
        st.caption("SEC & Contact Research")
        st.caption(f"v{APP_VERSION} · Bhavsar Growth Consulting")
        st.divider()

        st.caption("Quick Stats")
        c1, c2 = st.columns(2)
        c1.metric("Firms", stats['total_firms'])
        c2.metric("Processed", stats['firms_processed'])
        c1.metric("Contacts", contact_stats['total_contacts'])
        c2.metric("With Email", contact_stats['with_email'])

        st.divider()
        st.caption("Hunter.io Credits")
        if HUNTER_API_KEY:
            monthly = get_monthly_hunter_credits()
            st.progress(
                min(monthly / 2000, 1.0),
                text=f"{monthly:,} / 2,000 this month"
            )
        else:
            st.warning("Not configured. Add `HUNTER_API_KEY` to `.env`.")

        st.divider()
        # API Usage summary
        st.caption("API Usage")
        api_stats = get_enrichment_stats()
        if api_stats:
            for s in api_stats:
                st.text(f"{s['api_source']}: {s['total_calls']} calls")
        else:
            st.text("No API calls yet")

    # --- Main Content ---

    # ========================
    # SECTION 1: Import
    # ========================
    st.header("1. Import SEC Data")

    if stats['total_firms'] > 0:
        st.success(f"**{stats['total_firms']}** firms imported (120-day approvals)")
    else:
        st.info("Upload the SEC's monthly investment adviser data to get started.")

    _section_import()

    st.divider()

    # ========================
    # SECTION 2: Research
    # ========================
    st.header("2. Research Firms")

    if stats['total_firms'] == 0:
        st.caption("Import firms first to start research.")
    else:
        _section_research(stats)

    st.divider()

    # ========================
    # SECTION 3: Contacts & Export
    # ========================
    st.header("3. Contact List & Export")
    _section_contacts_export()


# ============================================================
# SECTION 1: Import SEC Data
# ============================================================

def _section_import():
    """SEC FOIA data import — upload CSV/ZIP or auto-detect."""
    import tempfile

    sibling_csv = Path(__file__).parent.parent / "Surge Contact Research" / "data" / "sec_advisers.csv"

    # File uploader
    uploaded = st.file_uploader(
        "Upload SEC FOIA CSV or ZIP",
        type=["csv", "zip"],
        key="sec_upload",
        help="Download from sec.gov, then upload the ZIP or CSV here.",
    )

    if uploaded is not None:
        if st.button("Import Uploaded File", type="primary", key="btn_import_upload"):
            with st.spinner(f"Parsing {uploaded.name}..."):
                suffix = ".zip" if uploaded.name.endswith(".zip") else ".csv"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uploaded.getvalue())
                    tmp_path = tmp.name
                result = fetch_and_store(csv_path=tmp_path)
            if result.get('error'):
                st.error(f"Import failed: {result['error']}")
            else:
                st.success(
                    f"Imported **{result['firms_imported']}** firms "
                    f"(120-day approvals) from {result['downloaded']:,} total records"
                )
                st.rerun()

    # Pre-downloaded data option
    if sibling_csv.exists():
        import os
        mod_time = datetime.fromtimestamp(os.path.getmtime(sibling_csv))
        st.caption(f"Pre-downloaded SEC data found (updated {mod_time.strftime('%b %d, %Y')})")
        if st.button("Import Pre-Downloaded Data", key="btn_import_sibling"):
            with st.spinner("Parsing pre-downloaded SEC data..."):
                result = fetch_and_store(csv_path=str(sibling_csv))
            if result.get('error'):
                st.error(f"Import failed: {result['error']}")
            else:
                st.success(
                    f"Imported **{result['firms_imported']}** firms "
                    f"(120-day approvals) from {result['downloaded']:,} total records"
                )
                st.rerun()

    # Auto-download option
    with st.expander("Download from SEC.gov"):
        st.caption("SEC.gov may block automated downloads. If this fails, download manually.")
        if st.button("Check Available Files", key="btn_probe", type="secondary"):
            with st.spinner("Probing SEC.gov..."):
                results = probe_sec_urls()
            st.session_state['sec_probe_results'] = results

        if 'sec_probe_results' in st.session_state:
            results = st.session_state['sec_probe_results']
            available = [r for r in results if r['available']]

            if not available:
                st.warning("No files detected. Download manually instead.")
            else:
                options = {}
                for r in available:
                    size_str = f" — {r['size_mb']} MB" if r['size_mb'] else ""
                    label = f"{r['date_label']}{size_str}"
                    options[label] = r['url']

                selected_label = st.radio(
                    "Select a file:", list(options.keys()),
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
                            f"Imported **{result['firms_imported']}** firms "
                            f"from {result['downloaded']:,} total records"
                        )
                        del st.session_state['sec_probe_results']
                        st.rerun()


# ============================================================
# SECTION 2: Research Firms
# ============================================================

def _section_research(stats):
    """Research firms — Hunter.io Domain Search finds contacts + emails."""

    if not HUNTER_API_KEY:
        st.warning("Hunter.io not configured. Add `HUNTER_API_KEY` to `.env` to start research.")
        return

    # Get list of unprocessed firms
    all_firms = get_firms()
    all_crds = [f['crd'] for f in all_firms]
    unprocessed = get_unprocessed_crds(all_crds)

    st.caption(
        f"{len(all_firms)} firms total — "
        f"**{len(unprocessed)}** unprocessed, "
        f"**{len(all_firms) - len(unprocessed)}** already researched"
    )

    if not unprocessed:
        st.success("All firms have been researched! Run again in 30 days to refresh.")
        return

    # Controls
    col1, col2 = st.columns(2)
    with col1:
        batch_size = st.number_input(
            "How many firms to research?",
            min_value=1,
            max_value=len(unprocessed),
            value=min(10, len(unprocessed)),
            step=1,
            key="batch_size",
            help="Number of unprocessed firms to research. Each uses 1 Hunter.io credit.",
        )
    with col2:
        credit_limit = st.number_input(
            "Hunter.io credit limit (this run)",
            min_value=0,
            max_value=2000,
            value=min(DEFAULT_BATCH_CREDIT_LIMIT, len(unprocessed)),
            step=10,
            key="credit_limit",
            help="Max Hunter.io credits to use. Each firm uses 1 credit. 0 = no limit.",
        )

    run_btn = st.button("Start Research", type="primary", key="btn_research")

    # Show results from previous run
    if 'last_research_result' in st.session_state:
        result = st.session_state.pop('last_research_result')
        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("Firms Processed", result.get('processed', 0))
        col_b.metric("Contacts Found", result.get('contacts_found', 0))
        col_c.metric("Skipped (no domain)", result.get('skipped', 0))
        col_d.metric("Credits Used", result.get('credits_used', 0))

    if run_btn:
        firms_to_process = unprocessed[:batch_size]

        st.subheader("Searching Hunter.io for contacts...")
        progress_bar = st.progress(0, text="Starting domain search...")
        status_text = st.empty()
        start_time = time.time()

        def _on_progress(current, total, res):
            progress_bar.progress(
                current / total,
                text=f"Researching firm {current} / {total}",
            )
            elapsed = time.time() - start_time
            status_text.text(
                f"Contacts found: {res['contacts_found']} | "
                f"Firms with results: {res['processed']} | "
                f"Skipped: {res['skipped']} | "
                f"Credits: {res['credits_used']} | "
                f"Elapsed: {elapsed:.0f}s"
            )

        result = research_firms_batch(
            firms_to_process,
            credit_limit=credit_limit,
            progress_callback=_on_progress,
        )
        elapsed = time.time() - start_time
        progress_bar.progress(1.0, text=f"Research complete! ({elapsed:.0f}s)")
        status_text.empty()

        st.success(
            f"Found **{result['contacts_found']}** contacts across "
            f"**{result['processed']}** firms using "
            f"**{result['credits_used']}** credits"
        )
        if result.get('credit_limit_hit'):
            st.warning("Credit limit reached. Run again to continue.")
        if result.get('skipped'):
            st.info(f"{result['skipped']} firms skipped (no usable website domain).")

        # Store results for display after rerun
        st.session_state['last_research_result'] = result
        st.rerun()


# ============================================================
# SECTION 3: Contact List & Export
# ============================================================

def _section_contacts_export():
    """Display all contacts and provide export."""
    contacts = get_all_contacts_with_firms()

    if not contacts:
        st.info("No contacts found yet. Import firms and run research first.")
        return

    # Summary metrics
    total = len(contacts)
    with_email = sum(1 for c in contacts if c.get('contact_email'))
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Contacts", total)
    col2.metric("With Email", with_email)
    col3.metric("Missing Email", total - with_email)

    # Build DataFrame
    df = pd.DataFrame(contacts)
    display_cols = [
        'crd', 'company', 'contact_name', 'contact_title',
        'contact_email', 'contact_phone', 'state', 'aum', 'source',
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    df_display = df[available_cols].copy()

    if 'aum' in df_display.columns:
        df_display['aum'] = df_display['aum'].apply(_format_aum)

    # Contact table
    st.dataframe(
        df_display,
        width=None,
        hide_index=True,
        use_container_width=True,
        column_config={
            'crd': st.column_config.NumberColumn('CRD', format='%d'),
            'company': 'Company',
            'contact_name': 'Name',
            'contact_title': 'Title',
            'contact_email': 'Email',
            'contact_phone': 'Phone',
            'state': 'State',
            'aum': 'AUM',
            'source': 'Source',
        },
    )

    # Export
    st.subheader("Export")
    export_cols = [
        'crd', 'company', 'state', 'website', 'aum',
        'contact_name', 'first_name', 'last_name',
        'contact_title', 'contact_email', 'contact_phone',
        'source', 'confidence',
    ]
    available_export = [c for c in export_cols if c in df.columns]
    df_export = df[available_export]

    csv_buffer = io.StringIO()
    df_export.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"surgeone_contacts_{timestamp}.csv"

    if st.download_button(
        label=f"Download CSV ({len(df_export)} contacts)",
        data=csv_data,
        file_name=filename,
        mime="text/csv",
        type="primary",
    ):
        log_export(filename, len(df_export), "all_contacts")
        st.success(f"Exported {len(df_export)} contacts to {filename}")


if __name__ == "__main__":
    main()
