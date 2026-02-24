"""Tests for cache_db.py — database CRUD, multi-contact, cache expiry."""

import pytest
from datetime import datetime, timedelta
from tools.cache_db import (
    init_db, get_connection, upsert_firms, get_firms, get_firm_by_crd,
    upsert_form_adv, get_form_adv, get_stale_form_adv_crds,
    insert_contact, get_contact, get_contacts_for_firm,
    get_all_contacts_with_firms, delete_contacts_for_firm,
    update_contact_email, get_contact_stats,
    log_enrichment, get_enrichment_stats, get_monthly_hunter_credits,
    log_export, get_export_history,
    get_pipeline_stats, get_unprocessed_crds,
)


class TestInitDb:
    def test_creates_tables(self, tmp_db):
        conn = get_connection(tmp_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {row['name'] for row in tables}
        assert 'firms' in table_names
        assert 'form_adv_details' in table_names
        assert 'contacts' in table_names
        assert 'enrichment_log' in table_names
        assert 'export_history' in table_names
        conn.close()

    def test_idempotent(self, tmp_db):
        # Calling init_db twice should not error
        init_db(tmp_db)
        init_db(tmp_db)


class TestFirms:
    def test_upsert_insert(self, tmp_db, sample_firm):
        count = upsert_firms([sample_firm], db_path=tmp_db)
        assert count == 1
        firm = get_firm_by_crd(sample_firm['crd'], db_path=tmp_db)
        assert firm is not None
        assert firm['company'] == 'Test Wealth Management LLC'
        assert firm['aum'] == 150000000

    def test_upsert_update(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        sample_firm['aum'] = 200000000
        upsert_firms([sample_firm], db_path=tmp_db)
        firm = get_firm_by_crd(sample_firm['crd'], db_path=tmp_db)
        assert firm['aum'] == 200000000

    def test_upsert_empty_list(self, tmp_db):
        count = upsert_firms([], db_path=tmp_db)
        assert count == 0

    def test_get_firms_returns_all(self, tmp_db, sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        firms = get_firms(db_path=tmp_db)
        assert len(firms) == 2


class TestFormAdv:
    def test_upsert_and_get(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        adv = get_form_adv(sample_firm['crd'], db_path=tmp_db)
        assert adv is not None
        assert adv['cco_name'] == 'Eric Heiting'
        assert adv['state_count'] == 4

    def test_stale_detection(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        # No data yet — should be stale
        stale = get_stale_form_adv_crds([sample_firm['crd']], db_path=tmp_db)
        assert sample_firm['crd'] in stale
        # Add fresh data
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        stale = get_stale_form_adv_crds([sample_firm['crd']], db_path=tmp_db)
        assert sample_firm['crd'] not in stale

    def test_stale_detection_old_data(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        # Manually set scraped_at to 60 days ago
        conn = get_connection(tmp_db)
        old_date = (datetime.utcnow() - timedelta(days=60)).isoformat()
        conn.execute(
            "UPDATE form_adv_details SET scraped_at=? WHERE crd=?",
            (old_date, sample_firm['crd'])
        )
        conn.commit()
        conn.close()
        stale = get_stale_form_adv_crds(
            [sample_firm['crd']], max_age_days=30, db_path=tmp_db
        )
        assert sample_firm['crd'] in stale


class TestMultiContacts:
    def test_insert_contact(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'source': 'pdf_cco',
            'confidence': 80.0,
        }, db_path=tmp_db)
        assert contact_id is not None
        assert isinstance(contact_id, int)

    def test_insert_multiple_contacts(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'source': 'pdf_cco',
        }, db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'John Smith',
            'contact_title': 'Principal/Owner',
            'source': 'pdf_principal',
        }, db_path=tmp_db)
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert len(contacts) == 2

    def test_get_contacts_for_firm(self, tmp_db, sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)
        insert_contact(sample_firm_120day['crd'], {
            'contact_name': 'Bob Smith', 'source': 'pdf',
        }, db_path=tmp_db)
        # Each firm gets only its own contacts
        contacts_1 = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        contacts_2 = get_contacts_for_firm(sample_firm_120day['crd'], db_path=tmp_db)
        assert len(contacts_1) == 1
        assert len(contacts_2) == 1
        assert contacts_1[0]['contact_name'] == 'Jane Doe'
        assert contacts_2[0]['contact_name'] == 'Bob Smith'

    def test_get_all_contacts_with_firms(self, tmp_db, sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'contact_title': 'CCO', 'source': 'pdf',
        }, db_path=tmp_db)
        insert_contact(sample_firm_120day['crd'], {
            'contact_name': 'Bob Smith', 'contact_title': 'Partner', 'source': 'pdf',
        }, db_path=tmp_db)
        all_contacts = get_all_contacts_with_firms(db_path=tmp_db)
        assert len(all_contacts) == 2
        # Should have firm data joined
        for c in all_contacts:
            assert 'company' in c
            assert 'state' in c
            assert 'contact_name' in c

    def test_delete_contacts_for_firm(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'John Smith', 'source': 'pdf',
        }, db_path=tmp_db)
        assert len(get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)) == 2
        delete_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert len(get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)) == 0

    def test_update_contact_email(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'source': 'pdf_cco',
        }, db_path=tmp_db)
        update_contact_email(contact_id, 'jane@testwm.com', '212-555-9999',
                             db_path=tmp_db)
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert contacts[0]['contact_email'] == 'jane@testwm.com'
        assert contacts[0]['contact_phone'] == '212-555-9999'

    def test_update_contact_email_without_phone(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)
        update_contact_email(contact_id, 'jane@testwm.com', db_path=tmp_db)
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert contacts[0]['contact_email'] == 'jane@testwm.com'

    def test_insert_contact_with_type(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'contact_type': 'compliance',
            'source': 'hunter_domain_search',
        }, db_path=tmp_db)
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert contacts[0]['contact_type'] == 'compliance'

    def test_get_all_contacts_includes_type(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'contact_type': 'compliance',
            'source': 'hunter_domain_search',
        }, db_path=tmp_db)
        all_contacts = get_all_contacts_with_firms(db_path=tmp_db)
        assert all_contacts[0]['contact_type'] == 'compliance'

    def test_insert_parses_first_last_name(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Brandon Smith',
            'source': 'pdf', 'confidence': 80.0,
        }, db_path=tmp_db)
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert contacts[0]['first_name'] == 'Brandon'
        assert contacts[0]['last_name'] == 'Smith'


class TestUnprocessedCrds:
    def test_all_unprocessed(self, tmp_db, sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        unprocessed = get_unprocessed_crds(
            [sample_firm['crd'], sample_firm_120day['crd']], db_path=tmp_db
        )
        assert len(unprocessed) == 2

    def test_skips_recently_processed(self, tmp_db, sample_firm, sample_firm_120day,
                                       sample_form_adv):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        unprocessed = get_unprocessed_crds(
            [sample_firm['crd'], sample_firm_120day['crd']], db_path=tmp_db
        )
        assert len(unprocessed) == 1
        assert unprocessed[0] == sample_firm_120day['crd']

    def test_includes_old_processed(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        # Set scraped_at to 60 days ago
        conn = get_connection(tmp_db)
        old_date = (datetime.utcnow() - timedelta(days=60)).isoformat()
        conn.execute(
            "UPDATE form_adv_details SET scraped_at=? WHERE crd=?",
            (old_date, sample_firm['crd'])
        )
        conn.commit()
        conn.close()
        unprocessed = get_unprocessed_crds(
            [sample_firm['crd']], max_age_days=30, db_path=tmp_db
        )
        assert sample_firm['crd'] in unprocessed


class TestContactStats:
    def test_empty_db(self, tmp_db):
        stats = get_contact_stats(db_path=tmp_db)
        assert stats['total_contacts'] == 0
        assert stats['with_email'] == 0
        assert stats['without_email'] == 0
        assert stats['firms_processed'] == 0

    def test_with_data(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_email': 'jane@test.com',
            'source': 'pdf',
        }, db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'John Smith',
            'source': 'pdf',
        }, db_path=tmp_db)
        stats = get_contact_stats(db_path=tmp_db)
        assert stats['total_contacts'] == 2
        assert stats['with_email'] == 1
        assert stats['without_email'] == 1
        assert stats['firms_processed'] == 1


class TestEnrichmentLog:
    def test_log_and_stats(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'success', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'not_found', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'pdf_extraction', 'form_adv_pdf',
                       200, 'success', credits_used=0, db_path=tmp_db)
        stats = get_enrichment_stats(db_path=tmp_db)
        hunter = next(s for s in stats if s['api_source'] == 'hunter_io')
        assert hunter['total_calls'] == 2
        assert hunter['total_credits'] == 2
        assert hunter['successes'] == 1
        pdf = next(s for s in stats if s['api_source'] == 'pdf_extraction')
        assert pdf['total_calls'] == 1
        assert pdf['total_credits'] == 0


class TestMonthlyHunterCredits:
    def test_returns_zero_when_empty(self, tmp_db):
        credits = get_monthly_hunter_credits(db_path=tmp_db)
        assert credits == 0

    def test_counts_current_month(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'success', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'success', credits_used=1, db_path=tmp_db)
        credits = get_monthly_hunter_credits(db_path=tmp_db)
        assert credits == 2

    def test_excludes_non_hunter_calls(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'success', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'pdf_extraction', 'form_adv_pdf',
                       200, 'success', credits_used=0, db_path=tmp_db)
        credits = get_monthly_hunter_credits(db_path=tmp_db)
        assert credits == 1

    def test_specific_month(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        conn = get_connection(tmp_db)
        conn.execute("""
            INSERT INTO enrichment_log (crd, api_source, endpoint, status_code,
                result_status, credits_used, called_at)
            VALUES (?, 'hunter_io', '/email-finder', 200, 'success', 1, '2026-01-15T12:00:00')
        """, (sample_firm['crd'],))
        conn.commit()
        conn.close()
        assert get_monthly_hunter_credits(year=2026, month=1, db_path=tmp_db) == 1
        assert get_monthly_hunter_credits(year=2026, month=3, db_path=tmp_db) == 0


class TestExportHistory:
    def test_log_and_retrieve(self, tmp_db):
        log_export('leads_2026_02.csv', 50, 'all contacts', db_path=tmp_db)
        history = get_export_history(db_path=tmp_db)
        assert len(history) == 1
        assert history[0]['filename'] == 'leads_2026_02.csv'
        assert history[0]['record_count'] == 50


class TestPipelineStats:
    def test_empty_db(self, tmp_db):
        stats = get_pipeline_stats(db_path=tmp_db)
        assert stats['total_firms'] == 0
        assert stats['firms_processed'] == 0
        assert stats['total_contacts'] == 0
        assert stats['contacts_with_email'] == 0

    def test_full_pipeline(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_email': 'jane@test.com',
            'source': 'pdf_cco',
        }, db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'John Smith',
            'source': 'pdf_principal',
        }, db_path=tmp_db)
        stats = get_pipeline_stats(db_path=tmp_db)
        assert stats['total_firms'] == 1
        assert stats['firms_processed'] == 1
        assert stats['total_contacts'] == 2
        assert stats['contacts_with_email'] == 1
