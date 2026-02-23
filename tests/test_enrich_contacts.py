"""Tests for enrich_contacts.py — contact discovery waterfall with mocked APIs."""

from unittest.mock import patch, MagicMock
import pytest

from tools.enrich_contacts import (
    enrich_firm, enrich_batch, _extract_domain, _is_generic_email,
    _pick_best_hunter_contact, _match_email_to_name,
    _pick_best_scraped_contact, _is_valid_person_name,
)
from tools.cache_db import upsert_firms, upsert_form_adv, get_contact


class TestExtractDomain:
    def test_simple_url(self):
        assert _extract_domain('https://www.testwm.com') == 'testwm.com'

    def test_url_without_www(self):
        assert _extract_domain('https://testwm.com') == 'testwm.com'

    def test_url_with_path(self):
        assert _extract_domain('https://www.testwm.com/about') == 'testwm.com'

    def test_url_without_protocol(self):
        assert _extract_domain('www.testwm.com') == 'testwm.com'

    def test_none(self):
        assert _extract_domain(None) is None

    def test_empty(self):
        assert _extract_domain('') is None


class TestIsGenericEmail:
    def test_generic_prefix(self):
        assert _is_generic_email('info@company.com') is True
        assert _is_generic_email('support@company.com') is True

    def test_generic_domain(self):
        assert _is_generic_email('john@gmail.com') is True
        assert _is_generic_email('jane@sec.gov') is True

    def test_personal_email(self):
        assert _is_generic_email('eheiting@centralwealth.com') is False

    def test_reporting_is_generic(self):
        assert _is_generic_email('reporting@epogeecapital.com') is True

    def test_information_is_generic(self):
        assert _is_generic_email('Information@everviewcap.com') is True

    def test_compliance_is_generic(self):
        assert _is_generic_email('compliance@firm.com') is True

    def test_operations_is_generic(self):
        assert _is_generic_email('operations@advisory.com') is True


class TestIsValidPersonName:
    def test_valid_names(self):
        assert _is_valid_person_name('Brandon Smith') is True
        assert _is_valid_person_name('Eric J Heiting') is True
        assert _is_valid_person_name('Mary Anne Johnson') is True

    def test_city_rejected(self):
        assert _is_valid_person_name('New York') is False
        assert _is_valid_person_name('San Francisco') is False
        assert _is_valid_person_name('Los Angeles') is False

    def test_company_name_rejected(self):
        assert _is_valid_person_name('Epogee Capital Management') is False
        assert _is_valid_person_name('Spring Street Wealth') is False

    def test_single_word_rejected(self):
        assert _is_valid_person_name('Brandon') is False

    def test_too_long_rejected(self):
        assert _is_valid_person_name('A Very Long Name ' * 5) is False

    def test_none_rejected(self):
        assert _is_valid_person_name(None) is False

    def test_empty_rejected(self):
        assert _is_valid_person_name('') is False


class TestPickBestHunterContact:
    def test_prefers_cco(self):
        emails = [
            {'first_name': 'John', 'last_name': 'Doe', 'value': 'jdoe@test.com',
             'position': 'Advisor', 'confidence': 90},
            {'first_name': 'Jane', 'last_name': 'Smith', 'value': 'jsmith@test.com',
             'position': 'Chief Compliance Officer', 'confidence': 80},
        ]
        best = _pick_best_hunter_contact(emails)
        assert best['value'] == 'jsmith@test.com'

    def test_prefers_higher_confidence_same_title(self):
        emails = [
            {'first_name': 'A', 'last_name': 'B', 'value': 'a@test.com',
             'position': 'Partner', 'confidence': 70},
            {'first_name': 'C', 'last_name': 'D', 'value': 'c@test.com',
             'position': 'Partner', 'confidence': 95},
        ]
        best = _pick_best_hunter_contact(emails)
        assert best['value'] == 'c@test.com'

    def test_empty_list(self):
        assert _pick_best_hunter_contact([]) is None


class TestMatchEmailToName:
    def test_matches_by_name_part(self):
        emails = {'jsmith@test.com', 'info@test.com'}
        result = _match_email_to_name('John Smith', emails)
        assert result == 'jsmith@test.com'

    def test_fallback_to_first(self):
        emails = {'random@test.com'}
        result = _match_email_to_name('John Smith', emails)
        assert result == 'random@test.com'

    def test_no_emails(self):
        result = _match_email_to_name('John Smith', set())
        assert result is None


class TestPickBestScrapedContact:
    def test_prefers_email_over_no_email(self):
        contacts = [
            {'contact_name': 'John Doe', 'contact_email': None, 'contact_title': 'CCO'},
            {'contact_name': 'Jane Smith', 'contact_email': 'has@test.com', 'contact_title': 'Advisor'},
        ]
        best = _pick_best_scraped_contact(contacts)
        assert best['contact_name'] == 'Jane Smith'

    def test_prefers_cco_title(self):
        contacts = [
            {'contact_name': 'John Doe', 'contact_email': 'a@t.com', 'contact_title': 'Advisor'},
            {'contact_name': 'Jane Smith', 'contact_email': 'b@t.com', 'contact_title': 'Chief Compliance Officer'},
        ]
        best = _pick_best_scraped_contact(contacts)
        assert best['contact_name'] == 'Jane Smith'

    def test_filters_non_person_names(self):
        contacts = [
            {'contact_name': 'New York', 'contact_email': 'ny@test.com', 'contact_title': 'Office'},
            {'contact_name': 'John Doe', 'contact_email': 'john@test.com', 'contact_title': 'Partner'},
        ]
        best = _pick_best_scraped_contact(contacts)
        assert best['contact_name'] == 'John Doe'

    def test_keeps_email_only_contacts(self):
        contacts = [
            {'contact_name': None, 'contact_email': 'anon@test.com', 'contact_title': None},
        ]
        best = _pick_best_scraped_contact(contacts)
        assert best['contact_email'] == 'anon@test.com'


class TestEnrichFirm:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_uses_form_adv_cco(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)

        result = enrich_firm(sample_firm['crd'], db_path=tmp_db)
        assert result is not None
        assert result['contact_name'] == 'Eric Heiting'
        assert result['source'] == 'form_adv'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_returns_cached(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        from tools.cache_db import upsert_contact as db_upsert
        db_upsert(sample_firm['crd'], sample_contact, db_path=tmp_db)

        result = enrich_firm(sample_firm['crd'], db_path=tmp_db)
        assert result['contact_email'] == 'eheiting@testwm.com'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts._hunter_domain_search')
    def test_hunter_enrichment(self, mock_hunter, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_hunter.return_value = {
            'contact_name': 'Hunter Contact',
            'contact_email': 'hunter@testwm.com',
            'contact_title': 'CCO',
            'source': 'hunter_io',
            'confidence': 90.0,
        }

        result = enrich_firm(sample_firm['crd'], db_path=tmp_db)
        assert result['contact_email'] == 'hunter@testwm.com'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_no_firm_returns_none(self, tmp_db):
        result = enrich_firm(999999, db_path=tmp_db)
        assert result is None


class TestEnrichFirmCreditBudget:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts._hunter_domain_search')
    def test_skips_hunter_when_budget_exhausted(self, mock_hunter, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        budget = {'used': 50, 'limit': 50}
        enrich_firm(sample_firm['crd'], db_path=tmp_db, credit_budget=budget)
        mock_hunter.assert_not_called()
        assert budget['used'] == 50

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts._hunter_domain_search')
    def test_increments_budget_on_hunter_call(self, mock_hunter, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_hunter.return_value = {
            'contact_name': 'Test Person',
            'contact_email': 'test@testwm.com',
            'contact_title': 'CCO',
            'source': 'hunter_io',
            'confidence': 90.0,
        }
        budget = {'used': 0, 'limit': 50}
        enrich_firm(sample_firm['crd'], db_path=tmp_db, credit_budget=budget)
        assert budget['used'] == 1

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts._hunter_email_finder')
    @patch('tools.enrich_contacts._hunter_domain_search')
    @patch('tools.enrich_contacts._scrape_website')
    def test_both_hunter_steps_increment(self, mock_scrape, mock_domain,
                                          mock_finder, tmp_db, sample_firm,
                                          sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], {
            'cco_name': 'Eric Heiting', 'cco_email': None, 'cco_phone': None,
            'state_registrations': 'IL', 'state_count': 1,
        }, db_path=tmp_db)
        mock_domain.return_value = None
        mock_scrape.return_value = None
        mock_finder.return_value = 'eric@testwm.com'
        budget = {'used': 0, 'limit': 50}
        enrich_firm(sample_firm['crd'], db_path=tmp_db, credit_budget=budget)
        assert budget['used'] == 2

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_no_budget_backwards_compatible(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        result = enrich_firm(sample_firm['crd'], db_path=tmp_db, credit_budget=None)
        # Should work without error


class TestEnrichBatch:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_batch_counts(self, tmp_db, sample_firm, sample_firm_120day, sample_form_adv):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)

        results = enrich_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db
        )
        assert results['enriched'] + results['no_result'] + results['cached'] == 2

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_progress_callback_invoked(self, tmp_db, sample_firm, sample_firm_120day,
                                        sample_form_adv):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        calls = []

        def on_progress(current, total, results):
            calls.append((current, total, dict(results)))

        enrich_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db, progress_callback=on_progress,
        )
        assert len(calls) == 2
        assert calls[0][0] == 1
        assert calls[0][1] == 2
        assert calls[1][0] == 2
        assert calls[1][1] == 2


class TestEnrichBatchCreditLimit:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts._hunter_domain_search')
    def test_stops_at_credit_limit(self, mock_hunter, tmp_db,
                                    sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        mock_hunter.return_value = {
            'contact_name': 'Test Person', 'contact_email': 'test@test.com',
            'contact_title': 'CCO', 'source': 'hunter_io', 'confidence': 90.0,
        }
        results = enrich_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db, credit_limit=1,
        )
        assert results['credits_used'] <= 1
        assert results['credit_limit_hit'] is True
        assert results['skipped_credit_limit'] >= 1

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_default_limit_applied(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        results = enrich_batch([sample_firm['crd']], db_path=tmp_db)
        assert results['credit_limit'] == 50

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_zero_limit_means_unlimited(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        results = enrich_batch(
            [sample_firm['crd']], db_path=tmp_db, credit_limit=0,
        )
        assert results['credit_limit'] == 0
        assert results['credit_limit_hit'] is False

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_cached_firms_dont_count(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        from tools.cache_db import upsert_contact
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        results = enrich_batch(
            [sample_firm['crd']], db_path=tmp_db, credit_limit=1,
        )
        assert results['cached'] == 1
        assert results['credits_used'] == 0
        assert results['credit_limit_hit'] is False
