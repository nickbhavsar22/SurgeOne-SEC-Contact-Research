"""Tests for cache_db.py — database CRUD, dedup, cache expiry."""

import pytest
from datetime import datetime, timedelta
from tools.cache_db import (
    init_db, get_connection, upsert_firms, get_firms, get_firm_by_crd,
    update_firm_score, upsert_form_adv, get_form_adv, get_stale_form_adv_crds,
    upsert_contact, get_contact, get_unenriched_crds, update_contact_validation,
    log_enrichment, get_enrichment_stats, get_monthly_hunter_credits,
    log_export, get_export_history,
    get_pipeline_stats, get_pipeline_stage_stats,
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

    def test_upsert_preserves_score(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        update_firm_score(sample_firm['crd'], 85.0, 'high AUM', db_path=tmp_db)
        # Re-upsert should not overwrite score
        sample_firm['aum'] = 200000000
        upsert_firms([sample_firm], db_path=tmp_db)
        firm = get_firm_by_crd(sample_firm['crd'], db_path=tmp_db)
        assert firm['fit_score'] == 85.0

    def test_upsert_empty_list(self, tmp_db):
        count = upsert_firms([], db_path=tmp_db)
        assert count == 0

    def test_get_firms_filter_track(self, tmp_db, sample_firm, sample_firm_near_threshold):
        upsert_firms([sample_firm, sample_firm_near_threshold], db_path=tmp_db)
        track_a = get_firms(track='A', db_path=tmp_db)
        track_b = get_firms(track='B', db_path=tmp_db)
        assert len(track_a) == 1
        assert track_a[0]['company'] == 'Test Wealth Management LLC'
        assert len(track_b) == 1
        assert track_b[0]['company'] == 'Growing Advisory Partners'

    def test_get_firms_filter_score(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        update_firm_score(sample_firm['crd'], 70.0, 'good fit', db_path=tmp_db)
        results = get_firms(min_score=80, db_path=tmp_db)
        assert len(results) == 0
        results = get_firms(min_score=50, db_path=tmp_db)
        assert len(results) == 1

    def test_get_firms_filter_contact(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        # No contact yet
        no_contact = get_firms(has_contact=False, db_path=tmp_db)
        assert len(no_contact) == 1
        has_contact = get_firms(has_contact=True, db_path=tmp_db)
        assert len(has_contact) == 0
        # Add contact
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        has_contact = get_firms(has_contact=True, db_path=tmp_db)
        assert len(has_contact) == 1


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


class TestContacts:
    def test_upsert_and_get(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        contact = get_contact(sample_firm['crd'], db_path=tmp_db)
        assert contact is not None
        assert contact['contact_email'] == 'eheiting@testwm.com'
        assert contact['source'] == 'hunter_io'

    def test_keeps_higher_confidence(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        # Try to upsert a lower-confidence contact
        weak_contact = {
            'contact_name': 'John Doe',
            'contact_email': 'jdoe@testwm.com',
            'source': 'website_scrape',
            'confidence': 30.0,
        }
        upsert_contact(sample_firm['crd'], weak_contact, db_path=tmp_db)
        contact = get_contact(sample_firm['crd'], db_path=tmp_db)
        assert contact['contact_name'] == 'Eric Heiting'  # Higher confidence kept

    def test_replaces_lower_confidence(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        weak = {'contact_name': 'Weak', 'contact_email': 'w@test.com',
                'source': 'scrape', 'confidence': 30.0}
        upsert_contact(sample_firm['crd'], weak, db_path=tmp_db)
        strong = {'contact_name': 'Strong', 'contact_email': 's@test.com',
                  'source': 'hunter', 'confidence': 90.0}
        upsert_contact(sample_firm['crd'], strong, db_path=tmp_db)
        contact = get_contact(sample_firm['crd'], db_path=tmp_db)
        assert contact['contact_name'] == 'Strong'

    def test_unenriched_crds(self, tmp_db, sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        unenriched = get_unenriched_crds(db_path=tmp_db)
        assert len(unenriched) == 2
        # Enrich one
        upsert_contact(sample_firm['crd'], {
            'contact_name': 'Test', 'contact_email': 't@test.com',
            'source': 'test', 'confidence': 50.0,
        }, db_path=tmp_db)
        unenriched = get_unenriched_crds(db_path=tmp_db)
        assert len(unenriched) == 1
        assert unenriched[0] == sample_firm_120day['crd']

    def test_validation_update(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        contact = get_contact(sample_firm['crd'], db_path=tmp_db)
        update_contact_validation(contact['id'], 'valid', None, db_path=tmp_db)
        contact = get_contact(sample_firm['crd'], db_path=tmp_db)
        assert contact['validation_status'] == 'valid'

    def test_upsert_parses_first_last_name(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact = {
            'contact_name': 'Brandon Smith',
            'contact_email': 'brandon@test.com',
            'source': 'test', 'confidence': 80.0,
        }
        upsert_contact(sample_firm['crd'], contact, db_path=tmp_db)
        result = get_contact(sample_firm['crd'], db_path=tmp_db)
        assert result['first_name'] == 'Brandon'
        assert result['last_name'] == 'Smith'

    def test_upsert_explicit_name_fields(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact = {
            'contact_name': 'B Smith',
            'first_name': 'Brandon',
            'last_name': 'Smith',
            'contact_email': 'brandon@test.com',
            'source': 'hunter_io', 'confidence': 90.0,
        }
        upsert_contact(sample_firm['crd'], contact, db_path=tmp_db)
        result = get_contact(sample_firm['crd'], db_path=tmp_db)
        assert result['first_name'] == 'Brandon'
        assert result['last_name'] == 'Smith'


class TestEnrichmentLog:
    def test_log_and_stats(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/domain-search',
                       200, 'success', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'not_found', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'website_scrape', '/contact',
                       200, 'success', credits_used=0, db_path=tmp_db)
        stats = get_enrichment_stats(db_path=tmp_db)
        hunter = next(s for s in stats if s['api_source'] == 'hunter_io')
        assert hunter['total_calls'] == 2
        assert hunter['total_credits'] == 2
        assert hunter['successes'] == 1
        scrape = next(s for s in stats if s['api_source'] == 'website_scrape')
        assert scrape['total_calls'] == 1
        assert scrape['total_credits'] == 0


class TestMonthlyHunterCredits:
    def test_returns_zero_when_empty(self, tmp_db):
        credits = get_monthly_hunter_credits(db_path=tmp_db)
        assert credits == 0

    def test_counts_current_month(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/domain-search',
                       200, 'success', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/email-finder',
                       200, 'success', credits_used=1, db_path=tmp_db)
        credits = get_monthly_hunter_credits(db_path=tmp_db)
        assert credits == 2

    def test_excludes_non_hunter_calls(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'hunter_io', '/domain-search',
                       200, 'success', credits_used=1, db_path=tmp_db)
        log_enrichment(sample_firm['crd'], 'website_scrape', '/homepage',
                       200, 'success', credits_used=0, db_path=tmp_db)
        credits = get_monthly_hunter_credits(db_path=tmp_db)
        assert credits == 1

    def test_specific_month(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        conn = get_connection(tmp_db)
        conn.execute("""
            INSERT INTO enrichment_log (crd, api_source, endpoint, status_code,
                result_status, credits_used, called_at)
            VALUES (?, 'hunter_io', '/domain-search', 200, 'success', 1, '2026-01-15T12:00:00')
        """, (sample_firm['crd'],))
        conn.commit()
        conn.close()
        assert get_monthly_hunter_credits(year=2026, month=1, db_path=tmp_db) == 1
        assert get_monthly_hunter_credits(year=2026, month=3, db_path=tmp_db) == 0


class TestExportHistory:
    def test_log_and_retrieve(self, tmp_db):
        log_export('leads_2026_02.csv', 50, 'track=A,score>=70', db_path=tmp_db)
        history = get_export_history(db_path=tmp_db)
        assert len(history) == 1
        assert history[0]['filename'] == 'leads_2026_02.csv'
        assert history[0]['record_count'] == 50


class TestPipelineStats:
    def test_full_funnel(self, tmp_db, sample_firm, sample_firm_120day,
                         sample_contact):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        update_firm_score(sample_firm['crd'], 80.0, 'good', db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        update_contact_validation(
            get_contact(sample_firm['crd'], db_path=tmp_db)['id'],
            'valid', None, db_path=tmp_db
        )
        stats = get_pipeline_stats(db_path=tmp_db)
        assert stats['total_firms'] == 2
        assert stats['scored'] == 1
        assert stats['enriched'] == 1
        assert stats['validated'] == 1


class TestPipelineStageStats:
    def test_empty_db(self, tmp_db):
        stats = get_pipeline_stage_stats(db_path=tmp_db)
        assert stats['total_firms'] == 0
        assert stats['iapd_queried'] == 0
        assert stats['scored'] == 0
        assert stats['enriched'] == 0
        assert stats['validated'] == 0
        assert stats['valid'] == 0

    def test_with_data(self, tmp_db, sample_firm, sample_form_adv, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        update_firm_score(sample_firm['crd'], 80.0, 'good', db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        update_contact_validation(
            get_contact(sample_firm['crd'], db_path=tmp_db)['id'],
            'valid', None, db_path=tmp_db
        )
        stats = get_pipeline_stage_stats(db_path=tmp_db)
        assert stats['total_firms'] == 1
        assert stats['iapd_queried'] == 1
        assert stats['scored'] == 1
        assert stats['enriched'] == 1
        assert stats['validated'] == 1
        assert stats['valid'] == 1
        assert 'A' in stats['by_track']
