"""Tests for enrich_contacts.py — Hunter.io Email Finder enrichment."""

from unittest.mock import patch, MagicMock
import pytest

from tools.enrich_contacts import (
    enrich_contact_hunter, enrich_contacts_batch,
    _extract_domain, _is_generic_email,
)
from tools.cache_db import upsert_firms, upsert_form_adv, insert_contact


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


class TestEnrichContactHunter:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_finds_email(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'source': 'pdf_cco',
        }, db_path=tmp_db)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {
                'email': 'jane.doe@testwm.com',
                'score': 90,
                'phone_number': '212-555-1234',
            }
        }
        mock_get.return_value = mock_resp

        result = enrich_contact_hunter(
            contact_id, 'Jane', 'Doe', 'testwm.com',
            sample_firm['crd'], db_path=tmp_db,
        )
        assert result is not None
        assert result['email'] == 'jane.doe@testwm.com'
        assert result['phone'] == '212-555-1234'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_rejects_low_score(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {'email': 'maybe@testwm.com', 'score': 10}
        }
        mock_get.return_value = mock_resp

        result = enrich_contact_hunter(
            contact_id, 'Jane', 'Doe', 'testwm.com',
            sample_firm['crd'], db_path=tmp_db,
        )
        assert result is None

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_rejects_generic_email(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {'email': 'info@testwm.com', 'score': 90}
        }
        mock_get.return_value = mock_resp

        result = enrich_contact_hunter(
            contact_id, 'Jane', 'Doe', 'testwm.com',
            sample_firm['crd'], db_path=tmp_db,
        )
        assert result is None

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_returns_none_without_api_key(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        result = enrich_contact_hunter(1, 'Jane', 'Doe', 'testwm.com',
                                        sample_firm['crd'], db_path=tmp_db)
        assert result is None

    def test_returns_none_without_name(self, tmp_db, sample_firm):
        result = enrich_contact_hunter(1, None, 'Doe', 'testwm.com',
                                        sample_firm['crd'], db_path=tmp_db)
        assert result is None
        result = enrich_contact_hunter(1, 'Jane', None, 'testwm.com',
                                        sample_firm['crd'], db_path=tmp_db)
        assert result is None

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_handles_network_error(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)

        import requests as req
        mock_get.side_effect = req.RequestException("timeout")

        result = enrich_contact_hunter(
            contact_id, 'Jane', 'Doe', 'testwm.com',
            sample_firm['crd'], db_path=tmp_db,
        )
        assert result is None


class TestEnrichContactsBatch:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_returns_no_api_key(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        result = enrich_contacts_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['no_api_key'] is True

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.enrich_contact_hunter')
    def test_enriches_contacts_without_email(self, mock_hunter, tmp_db,
                                              sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'source': 'pdf_cco',
        }, db_path=tmp_db)

        mock_hunter.return_value = {'email': 'jane@testwm.com', 'phone': None}

        result = enrich_contacts_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['enriched'] == 1

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.enrich_contact_hunter')
    def test_skips_contacts_with_email(self, mock_hunter, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_email': 'already@testwm.com',
            'source': 'pdf_cco',
        }, db_path=tmp_db)

        result = enrich_contacts_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        mock_hunter.assert_not_called()

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.enrich_contact_hunter')
    def test_skips_firms_without_website(self, mock_hunter, tmp_db):
        firm_no_website = {
            'crd': 999003, 'company': 'No Website LLC',
            'status': '120-Day Approval', 'track': 'A',
        }
        upsert_firms([firm_no_website], db_path=tmp_db)
        insert_contact(999003, {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)

        result = enrich_contacts_batch([999003], db_path=tmp_db)
        assert result['skipped'] == 1
        mock_hunter.assert_not_called()

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.enrich_contact_hunter')
    def test_credit_limit(self, mock_hunter, tmp_db, sample_firm,
                           sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)
        insert_contact(sample_firm_120day['crd'], {
            'contact_name': 'Bob Smith', 'source': 'pdf',
        }, db_path=tmp_db)

        mock_hunter.return_value = {'email': 'jane@test.com', 'phone': None}

        result = enrich_contacts_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            credit_limit=1, db_path=tmp_db,
        )
        assert result['credits_used'] <= 1
        assert result['credit_limit_hit'] is True

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.enrich_contact_hunter')
    def test_progress_callback(self, mock_hunter, tmp_db, sample_firm,
                                sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe', 'source': 'pdf',
        }, db_path=tmp_db)
        insert_contact(sample_firm_120day['crd'], {
            'contact_name': 'Bob Smith', 'source': 'pdf',
        }, db_path=tmp_db)
        mock_hunter.return_value = None

        calls = []
        def on_progress(current, total, results):
            calls.append((current, total))

        enrich_contacts_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db, progress_callback=on_progress,
        )
        assert len(calls) == 2
        assert calls[0] == (1, 2)
        assert calls[1] == (2, 2)
