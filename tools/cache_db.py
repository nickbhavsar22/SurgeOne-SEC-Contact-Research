"""
SQLite database layer for SEC & Contact Research.

Provides persistent caching for all pipeline stages to avoid
redundant API calls and re-downloading data.
"""

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# Default database path: project root
DB_DIR = Path(__file__).resolve().parent.parent
DB_PATH = DB_DIR / "surge_research.db"


def get_connection(db_path=None):
    """Get a SQLite connection with WAL mode for concurrent reads."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path=None):
    """Create all tables if they don't exist."""
    conn = get_connection(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS firms (
                crd             INTEGER PRIMARY KEY,
                company         TEXT,
                legal_name      TEXT,
                status          TEXT,
                status_date     TEXT,
                filing_date     TEXT,
                city            TEXT,
                state           TEXT,
                phone           TEXT,
                website         TEXT,
                sec_registered  TEXT,
                era             TEXT,
                employees       INTEGER,
                clients         INTEGER,
                aum             INTEGER,
                aum_discretionary   INTEGER,
                aum_nondiscretionary INTEGER,
                track           TEXT,
                fit_score       REAL,
                fit_reasons     TEXT,
                scored_at       TEXT,
                imported_at     TEXT,
                updated_at      TEXT
            );

            CREATE TABLE IF NOT EXISTS form_adv_details (
                crd             INTEGER PRIMARY KEY,
                cco_name        TEXT,
                cco_email       TEXT,
                cco_phone       TEXT,
                state_registrations TEXT,
                state_count     INTEGER,
                aum_breakdown   TEXT,
                scraped_at      TEXT,
                FOREIGN KEY (crd) REFERENCES firms(crd)
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                crd             INTEGER,
                contact_name    TEXT,
                first_name      TEXT,
                last_name       TEXT,
                contact_email   TEXT,
                contact_title   TEXT,
                contact_phone   TEXT,
                contact_type    TEXT,
                contact_linkedin TEXT,
                source          TEXT,
                confidence      REAL,
                enriched_at     TEXT,
                validated_at    TEXT,
                validation_status TEXT,
                validation_issues TEXT,
                FOREIGN KEY (crd) REFERENCES firms(crd)
            );

            CREATE TABLE IF NOT EXISTS enrichment_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                crd             INTEGER,
                api_source      TEXT,
                endpoint        TEXT,
                status_code     INTEGER,
                result_status   TEXT,
                credits_used    INTEGER DEFAULT 0,
                called_at       TEXT,
                FOREIGN KEY (crd) REFERENCES firms(crd)
            );

            CREATE TABLE IF NOT EXISTS export_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                filename        TEXT,
                record_count    INTEGER,
                filters_used    TEXT,
                exported_at     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_firms_track ON firms(track);
            CREATE INDEX IF NOT EXISTS idx_firms_status ON firms(status);
            CREATE INDEX IF NOT EXISTS idx_contacts_crd ON contacts(crd);
            CREATE INDEX IF NOT EXISTS idx_enrichment_log_crd ON enrichment_log(crd);
        """)
        # Migration: add columns if not present
        for col in ('first_name', 'last_name', 'contact_type'):
            try:
                conn.execute(f"ALTER TABLE contacts ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass  # Column already exists
        conn.commit()
    finally:
        conn.close()


# --- Firms ---

def upsert_firms(records, db_path=None):
    """Insert or update firms from SEC FOIA data. Records is a list of dicts."""
    if not records:
        return 0
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    count = 0
    try:
        for r in records:
            conn.execute("""
                INSERT INTO firms (crd, company, legal_name, status, status_date,
                    filing_date, city, state, phone, website, sec_registered, era,
                    employees, clients, aum, aum_discretionary, aum_nondiscretionary,
                    track, imported_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(crd) DO UPDATE SET
                    company=excluded.company, legal_name=excluded.legal_name,
                    status=excluded.status, status_date=excluded.status_date,
                    filing_date=excluded.filing_date, city=excluded.city,
                    state=excluded.state, phone=excluded.phone,
                    website=excluded.website, sec_registered=excluded.sec_registered,
                    era=excluded.era, employees=excluded.employees,
                    clients=excluded.clients, aum=excluded.aum,
                    aum_discretionary=excluded.aum_discretionary,
                    aum_nondiscretionary=excluded.aum_nondiscretionary,
                    track=excluded.track, updated_at=excluded.updated_at
            """, (
                r.get('crd'), r.get('company'), r.get('legal_name'),
                r.get('status'), r.get('status_date'), r.get('filing_date'),
                r.get('city'), r.get('state'), r.get('phone'), r.get('website'),
                r.get('sec_registered'), r.get('era'),
                r.get('employees'), r.get('clients'), r.get('aum'),
                r.get('aum_discretionary'), r.get('aum_nondiscretionary'),
                r.get('track'), now, now,
            ))
            count += 1
        conn.commit()
    finally:
        conn.close()
    return count


def get_firms(db_path=None):
    """Query all imported firms. Returns list of dicts."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM firms ORDER BY company"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_firm_by_crd(crd, db_path=None):
    """Get a single firm by CRD number."""
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT * FROM firms WHERE crd = ?", (crd,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_firm_score(crd, fit_score, fit_reasons, db_path=None):
    """Update a firm's fit score."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        conn.execute(
            "UPDATE firms SET fit_score=?, fit_reasons=?, scored_at=? WHERE crd=?",
            (fit_score, fit_reasons, now, crd)
        )
        conn.commit()
    finally:
        conn.close()


# --- Form ADV Details ---

def upsert_form_adv(crd, details, db_path=None):
    """Insert or update Form ADV details for a firm."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        conn.execute("""
            INSERT INTO form_adv_details (crd, cco_name, cco_email, cco_phone,
                state_registrations, state_count, aum_breakdown, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(crd) DO UPDATE SET
                cco_name=excluded.cco_name, cco_email=excluded.cco_email,
                cco_phone=excluded.cco_phone,
                state_registrations=excluded.state_registrations,
                state_count=excluded.state_count,
                aum_breakdown=excluded.aum_breakdown,
                scraped_at=excluded.scraped_at
        """, (
            crd, details.get('cco_name'), details.get('cco_email'),
            details.get('cco_phone'), details.get('state_registrations'),
            details.get('state_count'), details.get('aum_breakdown'), now,
        ))
        conn.commit()
    finally:
        conn.close()


def get_form_adv(crd, db_path=None):
    """Get Form ADV details for a firm."""
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM form_adv_details WHERE crd = ?", (crd,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_stale_form_adv_crds(crd_list, max_age_days=30, db_path=None):
    """Return CRDs from the list that have no Form ADV data or stale data."""
    if not crd_list:
        return []
    conn = get_connection(db_path)
    cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
    try:
        # Batch in chunks of 500 for SQLite variable limit
        stale = []
        for i in range(0, len(crd_list), 500):
            chunk = crd_list[i:i + 500]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(f"""
                SELECT crd FROM form_adv_details
                WHERE crd IN ({placeholders}) AND scraped_at > ?
            """, chunk + [cutoff]).fetchall()
            fresh = {row['crd'] for row in rows}
            stale.extend(c for c in chunk if c not in fresh)
        return stale
    finally:
        conn.close()


# --- Contacts ---

def _parse_name(full_name):
    """Split 'First Last' into (first_name, last_name)."""
    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]


def upsert_contact(crd, contact, db_path=None):
    """Insert or update a contact for a firm. Keeps the best contact per CRD."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        # Check if a contact already exists for this CRD
        existing = conn.execute(
            "SELECT id, confidence FROM contacts WHERE crd = ? ORDER BY confidence DESC LIMIT 1",
            (crd,)
        ).fetchone()

        new_confidence = contact.get('confidence', 0) or 0

        if existing and (existing['confidence'] or 0) >= new_confidence:
            return  # Keep existing higher-confidence contact

        # Parse first/last name (explicit fields take precedence)
        parsed_first, parsed_last = _parse_name(contact.get('contact_name'))
        first_name = contact.get('first_name') or parsed_first
        last_name = contact.get('last_name') or parsed_last

        if existing:
            conn.execute("""
                UPDATE contacts SET contact_name=?, first_name=?, last_name=?,
                    contact_email=?, contact_title=?,
                    contact_phone=?, contact_linkedin=?, source=?, confidence=?, enriched_at=?
                WHERE id=?
            """, (
                contact.get('contact_name'), first_name, last_name,
                contact.get('contact_email'),
                contact.get('contact_title'), contact.get('contact_phone'),
                contact.get('contact_linkedin'), contact.get('source'),
                new_confidence, now, existing['id'],
            ))
        else:
            conn.execute("""
                INSERT INTO contacts (crd, contact_name, first_name, last_name,
                    contact_email, contact_title,
                    contact_phone, contact_linkedin, source, confidence, enriched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                crd, contact.get('contact_name'), first_name, last_name,
                contact.get('contact_email'),
                contact.get('contact_title'), contact.get('contact_phone'),
                contact.get('contact_linkedin'), contact.get('source'),
                new_confidence, now,
            ))
        conn.commit()
    finally:
        conn.close()


def get_contact(crd, db_path=None):
    """Get the best contact for a firm (legacy, returns single contact)."""
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM contacts WHERE crd = ? ORDER BY confidence DESC LIMIT 1",
            (crd,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def insert_contact(crd, contact, db_path=None):
    """Insert a contact for a firm. Allows multiple contacts per firm."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        parsed_first, parsed_last = _parse_name(contact.get('contact_name'))
        first_name = contact.get('first_name') or parsed_first
        last_name = contact.get('last_name') or parsed_last
        conn.execute("""
            INSERT INTO contacts (crd, contact_name, first_name, last_name,
                contact_email, contact_title, contact_phone, contact_type,
                contact_linkedin, source, confidence, enriched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            crd, contact.get('contact_name'), first_name, last_name,
            contact.get('contact_email'), contact.get('contact_title'),
            contact.get('contact_phone'), contact.get('contact_type'),
            contact.get('contact_linkedin'), contact.get('source'),
            contact.get('confidence', 0), now,
        ))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        conn.close()


def get_contacts_for_firm(crd, db_path=None):
    """Get ALL contacts for a firm."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM contacts WHERE crd = ? ORDER BY contact_title, contact_name",
            (crd,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_all_contacts_with_firms(db_path=None):
    """Join all contacts with firm data for display/export."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT c.id, c.crd, f.company, f.state, f.website, f.aum,
                   c.contact_name, c.first_name, c.last_name,
                   c.contact_email, c.contact_title, c.contact_phone,
                   c.contact_type, c.source, c.confidence, c.enriched_at
            FROM contacts c
            JOIN firms f ON c.crd = f.crd
            ORDER BY f.company, c.contact_title
        """).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def delete_contacts_for_firm(crd, db_path=None):
    """Delete all contacts for a firm (used before re-processing)."""
    conn = get_connection(db_path)
    try:
        conn.execute("DELETE FROM contacts WHERE crd = ?", (crd,))
        conn.commit()
    finally:
        conn.close()


def get_unprocessed_crds(crd_list, max_age_days=30, db_path=None):
    """Return CRDs that haven't been PDF-processed recently."""
    if not crd_list:
        return []
    conn = get_connection(db_path)
    cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
    try:
        fresh = set()
        for i in range(0, len(crd_list), 500):
            chunk = crd_list[i:i + 500]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(f"""
                SELECT crd FROM form_adv_details
                WHERE crd IN ({placeholders}) AND scraped_at > ?
            """, chunk + [cutoff]).fetchall()
            fresh.update(row['crd'] for row in rows)
        return [c for c in crd_list if c not in fresh]
    finally:
        conn.close()


def update_contact_email(contact_id, email, phone=None, db_path=None):
    """Update a contact's email and optionally phone after Hunter.io enrichment."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        if phone:
            conn.execute(
                "UPDATE contacts SET contact_email=?, contact_phone=?, enriched_at=? WHERE id=?",
                (email, phone, now, contact_id)
            )
        else:
            conn.execute(
                "UPDATE contacts SET contact_email=?, enriched_at=? WHERE id=?",
                (email, now, contact_id)
            )
        conn.commit()
    finally:
        conn.close()


def get_contact_stats(db_path=None):
    """Get summary contact statistics."""
    conn = get_connection(db_path)
    try:
        total = conn.execute("SELECT COUNT(*) as n FROM contacts").fetchone()['n']
        with_email = conn.execute(
            "SELECT COUNT(*) as n FROM contacts WHERE contact_email IS NOT NULL"
        ).fetchone()['n']
        firms_processed = conn.execute(
            "SELECT COUNT(*) as n FROM form_adv_details"
        ).fetchone()['n']
        return {
            'total_contacts': total,
            'with_email': with_email,
            'without_email': total - with_email,
            'firms_processed': firms_processed,
        }
    finally:
        conn.close()


# --- Enrichment Log ---

def log_enrichment(crd, api_source, endpoint, status_code, result_status,
                   credits_used=0, db_path=None):
    """Log an API call for auditing and credit tracking."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        conn.execute("""
            INSERT INTO enrichment_log (crd, api_source, endpoint, status_code,
                result_status, credits_used, called_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (crd, api_source, endpoint, status_code, result_status, credits_used, now))
        conn.commit()
    finally:
        conn.close()


def get_enrichment_stats(db_path=None):
    """Get summary stats for enrichment API usage."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT api_source,
                   COUNT(*) as total_calls,
                   SUM(credits_used) as total_credits,
                   SUM(CASE WHEN result_status='success' THEN 1 ELSE 0 END) as successes,
                   SUM(CASE WHEN result_status='not_found' THEN 1 ELSE 0 END) as not_found,
                   SUM(CASE WHEN result_status='error' THEN 1 ELSE 0 END) as errors
            FROM enrichment_log
            GROUP BY api_source
        """).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_monthly_hunter_credits(year=None, month=None, db_path=None):
    """Get total Hunter.io credits used in a given month.

    Defaults to the current month if year/month not specified.
    """
    conn = get_connection(db_path)
    try:
        if year is None or month is None:
            now = datetime.utcnow()
            year = now.year
            month = now.month
        start = f"{year:04d}-{month:02d}-01"
        if month == 12:
            end = f"{year + 1:04d}-01-01"
        else:
            end = f"{year:04d}-{month + 1:02d}-01"
        row = conn.execute("""
            SELECT COALESCE(SUM(credits_used), 0) as total
            FROM enrichment_log
            WHERE api_source = 'hunter_io'
              AND called_at >= ? AND called_at < ?
        """, (start, end)).fetchone()
        return row['total']
    finally:
        conn.close()


# --- Export History ---

def log_export(filename, record_count, filters_used, db_path=None):
    """Log a CSV export."""
    conn = get_connection(db_path)
    now = datetime.utcnow().isoformat()
    try:
        conn.execute(
            "INSERT INTO export_history (filename, record_count, filters_used, exported_at) VALUES (?, ?, ?, ?)",
            (filename, record_count, filters_used, now)
        )
        conn.commit()
    finally:
        conn.close()


def get_export_history(db_path=None):
    """Get export history."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM export_history ORDER BY exported_at DESC"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


# --- Pipeline Stats ---

def get_pipeline_stats(db_path=None):
    """Get summary stats for the simplified pipeline."""
    conn = get_connection(db_path)
    try:
        total = conn.execute("SELECT COUNT(*) as n FROM firms").fetchone()['n']
        processed = conn.execute(
            "SELECT COUNT(*) as n FROM form_adv_details"
        ).fetchone()['n']
        total_contacts = conn.execute(
            "SELECT COUNT(*) as n FROM contacts"
        ).fetchone()['n']
        with_email = conn.execute(
            "SELECT COUNT(*) as n FROM contacts WHERE contact_email IS NOT NULL"
        ).fetchone()['n']
        return {
            'total_firms': total,
            'firms_processed': processed,
            'total_contacts': total_contacts,
            'contacts_with_email': with_email,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
