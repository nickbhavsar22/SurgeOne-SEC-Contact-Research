"""Tests for enrich_contacts.py — Hunter.io Domain Search + Email Finder."""

from unittest.mock import patch, MagicMock
import pytest

from tools.enrich_contacts import (
    domain_search, enrich_contact_hunter, research_firms_batch,
    _extract_domain, _is_generic_email,
    _classify_contact, _filter_contacts_by_relevance,
)
from tools.cache_db import (
    upsert_firms, upsert_form_adv, insert_contact, get_contacts_for_firm,
)


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

    def test_linkedin_returns_none(self):
        assert _extract_domain('https://www.linkedin.com/in/someone') is None

    def test_facebook_returns_none(self):
        assert _extract_domain('https://facebook.com/company') is None

    def test_twitter_returns_none(self):
        assert _extract_domain('https://twitter.com/company') is None

    def test_x_returns_none(self):
        assert _extract_domain('https://x.com/company') is None


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


class TestClassifyContact:
    def test_cco_is_compliance(self):
        assert _classify_contact({'contact_title': 'Chief Compliance Officer'}) == 'compliance'

    def test_compliance_analyst_is_compliance(self):
        assert _classify_contact({'contact_title': 'Compliance Analyst'}) == 'compliance'

    def test_cco_abbreviation(self):
        assert _classify_contact({'contact_title': 'CCO'}) == 'compliance'

    def test_compound_compliance_title(self):
        assert _classify_contact({'contact_title': 'Managing Principal / CCO'}) == 'compliance'

    def test_ceo_is_c_suite(self):
        assert _classify_contact({'contact_title': 'Chief Executive Officer'}) == 'c_suite'

    def test_cfo_is_c_suite(self):
        assert _classify_contact({'contact_title': 'CFO'}) == 'c_suite'

    def test_managing_partner_is_c_suite(self):
        assert _classify_contact({'contact_title': 'Managing Partner'}) == 'c_suite'

    def test_president_is_c_suite(self):
        assert _classify_contact({'contact_title': 'President'}) == 'c_suite'

    def test_general_counsel_is_legal(self):
        assert _classify_contact({'contact_title': 'General Counsel'}) == 'legal_regulatory'

    def test_risk_officer_is_legal(self):
        assert _classify_contact({'contact_title': 'Chief Risk Officer'}) == 'legal_regulatory'

    def test_regulatory_is_legal(self):
        assert _classify_contact({'contact_title': 'VP Regulatory Affairs'}) == 'legal_regulatory'

    def test_none_title(self):
        assert _classify_contact({'contact_title': None}) is None

    def test_empty_title(self):
        assert _classify_contact({'contact_title': ''}) is None

    def test_irrelevant_title(self):
        assert _classify_contact({'contact_title': 'Marketing Coordinator'}) is None

    def test_case_insensitive(self):
        assert _classify_contact({'contact_title': 'CHIEF COMPLIANCE OFFICER'}) == 'compliance'

    def test_compliance_beats_legal(self):
        """Title with both compliance and legal → classified as compliance."""
        c = {'contact_title': 'Chief Compliance Officer & General Counsel'}
        assert _classify_contact(c) == 'compliance'


class TestFilterContactsByRelevance:
    def test_keeps_only_relevant(self):
        contacts = [
            {'contact_name': 'Jane', 'contact_title': 'CCO', 'confidence': 90},
            {'contact_name': 'Bob', 'contact_title': 'Marketing Manager', 'confidence': 85},
        ]
        result = _filter_contacts_by_relevance(contacts)
        assert len(result) == 1
        assert result[0]['contact_name'] == 'Jane'
        assert result[0]['contact_type'] == 'compliance'

    def test_keeps_multiple_relevant(self):
        contacts = [
            {'contact_name': 'Jane', 'contact_title': 'CCO', 'confidence': 90},
            {'contact_name': 'Bob', 'contact_title': 'CEO', 'confidence': 85},
            {'contact_name': 'Eve', 'contact_title': 'Intern', 'confidence': 70},
        ]
        result = _filter_contacts_by_relevance(contacts)
        assert len(result) == 2
        types = {c['contact_type'] for c in result}
        assert types == {'compliance', 'c_suite'}

    def test_fallback_to_seniority(self):
        contacts = [
            {'contact_name': 'Jane', 'contact_title': 'Founder', 'confidence': 80},
            {'contact_name': 'Bob', 'contact_title': 'Analyst', 'confidence': 90},
        ]
        result = _filter_contacts_by_relevance(contacts)
        assert len(result) == 1
        assert result[0]['contact_name'] == 'Jane'
        assert result[0]['contact_type'] == 'fallback'

    def test_fallback_to_highest_confidence(self):
        contacts = [
            {'contact_name': 'Jane', 'contact_title': 'Analyst', 'confidence': 60},
            {'contact_name': 'Bob', 'contact_title': 'Associate', 'confidence': 90},
        ]
        result = _filter_contacts_by_relevance(contacts)
        assert len(result) == 1
        assert result[0]['contact_name'] == 'Bob'
        assert result[0]['contact_type'] == 'fallback'

    def test_fallback_no_titles(self):
        contacts = [
            {'contact_name': 'Jane', 'contact_title': None, 'confidence': 80},
            {'contact_name': 'Bob', 'contact_title': None, 'confidence': 95},
        ]
        result = _filter_contacts_by_relevance(contacts)
        assert len(result) == 1
        assert result[0]['contact_name'] == 'Bob'

    def test_empty_list(self):
        assert _filter_contacts_by_relevance([]) == []

    def test_does_not_tag_excluded(self):
        contacts = [
            {'contact_name': 'Jane', 'contact_title': 'CCO', 'confidence': 90},
            {'contact_name': 'Bob', 'contact_title': 'Intern', 'confidence': 70},
        ]
        _filter_contacts_by_relevance(contacts)
        assert 'contact_type' not in contacts[1]


class TestDomainSearch:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_returns_contacts(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {
                'domain': 'testwm.com',
                'emails': [
                    {
                        'value': 'jane.doe@testwm.com',
                        'first_name': 'Jane',
                        'last_name': 'Doe',
                        'position': 'Chief Compliance Officer',
                        'confidence': 90,
                        'phone_number': '212-555-1234',
                    },
                    {
                        'value': 'john.smith@testwm.com',
                        'first_name': 'John',
                        'last_name': 'Smith',
                        'position': 'Managing Partner',
                        'confidence': 85,
                        'phone_number': None,
                    },
                ]
            }
        }
        mock_get.return_value = mock_resp

        contacts = domain_search('testwm.com', crd=sample_firm['crd'],
                                 db_path=tmp_db)
        assert len(contacts) == 2
        assert contacts[0]['contact_name'] == 'Jane Doe'
        assert contacts[0]['contact_email'] == 'jane.doe@testwm.com'
        assert contacts[0]['contact_title'] == 'Chief Compliance Officer'
        assert contacts[0]['contact_phone'] == '212-555-1234'
        assert contacts[0]['source'] == 'hunter_domain_search'
        assert contacts[1]['contact_name'] == 'John Smith'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_filters_generic_emails(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {
                'domain': 'testwm.com',
                'emails': [
                    {
                        'value': 'info@testwm.com',
                        'first_name': None,
                        'last_name': None,
                        'position': None,
                        'confidence': 90,
                    },
                    {
                        'value': 'jane@testwm.com',
                        'first_name': 'Jane',
                        'last_name': 'Doe',
                        'position': 'CEO',
                        'confidence': 85,
                    },
                ]
            }
        }
        mock_get.return_value = mock_resp

        contacts = domain_search('testwm.com', crd=sample_firm['crd'],
                                 db_path=tmp_db)
        # info@ should be filtered, null name should be filtered
        assert len(contacts) == 1
        assert contacts[0]['contact_email'] == 'jane@testwm.com'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_returns_empty_on_no_results(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {'domain': 'unknown.com', 'emails': []}
        }
        mock_get.return_value = mock_resp

        contacts = domain_search('unknown.com', crd=sample_firm['crd'],
                                 db_path=tmp_db)
        assert contacts == []

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_company_name_only(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'data': {
                'domain': 'testwm.com',
                'emails': [
                    {
                        'value': 'jane@testwm.com',
                        'first_name': 'Jane',
                        'last_name': 'Doe',
                        'position': 'CEO',
                        'confidence': 85,
                    },
                ]
            }
        }
        mock_get.return_value = mock_resp

        contacts = domain_search(company='Test Wealth Management LLC',
                                 crd=sample_firm['crd'], db_path=tmp_db)
        assert len(contacts) == 1
        assert contacts[0]['contact_name'] == 'Jane Doe'
        # Verify company param was passed to the API
        call_args = mock_get.call_args
        assert call_args[1]['params']['company'] == 'Test Wealth Management LLC'
        assert 'domain' not in call_args[1]['params']

    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_returns_empty_without_api_key(self, tmp_db):
        contacts = domain_search('testwm.com', db_path=tmp_db)
        assert contacts == []

    def test_returns_empty_without_domain_or_company(self, tmp_db):
        contacts = domain_search(db_path=tmp_db)
        assert contacts == []

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_handles_network_error(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        import requests as req
        mock_get.side_effect = req.RequestException("timeout")

        contacts = domain_search('testwm.com', crd=sample_firm['crd'],
                                 db_path=tmp_db)
        assert contacts == []


class TestEnrichContactHunter:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.requests.get')
    def test_finds_email(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        contact_id = insert_contact(sample_firm['crd'], {
            'contact_name': 'Jane Doe',
            'contact_title': 'CCO',
            'source': 'hunter_domain_search',
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
            'contact_name': 'Jane Doe', 'source': 'hunter_domain_search',
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
            'contact_name': 'Jane Doe', 'source': 'hunter_domain_search',
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
            'contact_name': 'Jane Doe', 'source': 'hunter_domain_search',
        }, db_path=tmp_db)

        import requests as req
        mock_get.side_effect = req.RequestException("timeout")

        result = enrich_contact_hunter(
            contact_id, 'Jane', 'Doe', 'testwm.com',
            sample_firm['crd'], db_path=tmp_db,
        )
        assert result is None


class TestResearchFirmsBatch:
    @patch('tools.enrich_contacts.HUNTER_API_KEY', '')
    def test_returns_no_api_key(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        result = research_firms_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['no_api_key'] is True

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_stores_contacts(self, mock_search, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)

        mock_search.return_value = [
            {
                'contact_name': 'Jane Doe',
                'first_name': 'Jane',
                'last_name': 'Doe',
                'contact_title': 'CCO',
                'contact_email': 'jane@testwm.com',
                'contact_phone': None,
                'confidence': 90,
                'source': 'hunter_domain_search',
            },
        ]

        result = research_firms_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['processed'] == 1
        assert result['contacts_found'] == 1
        assert result['credits_used'] == 1

        # Verify contact_type was set
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert contacts[0]['contact_type'] == 'compliance'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_filters_irrelevant_contacts(self, mock_search, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)

        mock_search.return_value = [
            {
                'contact_name': 'Jane Doe', 'first_name': 'Jane',
                'last_name': 'Doe', 'contact_title': 'CCO',
                'contact_email': 'jane@testwm.com', 'confidence': 90,
                'source': 'hunter_domain_search',
            },
            {
                'contact_name': 'Bob Intern', 'first_name': 'Bob',
                'last_name': 'Intern', 'contact_title': 'Associate',
                'contact_email': 'bob@testwm.com', 'confidence': 50,
                'source': 'hunter_domain_search',
            },
        ]

        result = research_firms_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['total_raw_contacts'] == 2
        assert result['contacts_found'] == 1  # Only CCO kept
        contacts = get_contacts_for_firm(sample_firm['crd'], db_path=tmp_db)
        assert len(contacts) == 1
        assert contacts[0]['contact_name'] == 'Jane Doe'

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_searches_firms_without_website_by_company_name(
            self, mock_search, tmp_db):
        firm_no_website = {
            'crd': 999003, 'company': 'No Website LLC',
            'status': '120-Day Approval', 'track': 'A',
        }
        upsert_firms([firm_no_website], db_path=tmp_db)
        mock_search.return_value = []

        result = research_firms_batch([999003], db_path=tmp_db)
        # Should search by company name, not skip
        mock_search.assert_called_once()
        call_kwargs = mock_search.call_args[1]
        assert call_kwargs.get('company') == 'No Website LLC'
        assert call_kwargs.get('domain') is None

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_searches_linkedin_firms_by_company_name(
            self, mock_search, tmp_db):
        firm_linkedin = {
            'crd': 999004, 'company': 'LinkedIn Firm',
            'website': 'https://www.linkedin.com/company/test',
            'status': '120-Day Approval', 'track': 'A',
        }
        upsert_firms([firm_linkedin], db_path=tmp_db)
        mock_search.return_value = []

        result = research_firms_batch([999004], db_path=tmp_db)
        # Should search by company name (domain is None for LinkedIn)
        mock_search.assert_called_once()
        call_kwargs = mock_search.call_args[1]
        assert call_kwargs.get('company') == 'LinkedIn Firm'
        assert call_kwargs.get('domain') is None

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_skips_cached(self, mock_search, tmp_db, sample_firm,
                           sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        # Mark as recently processed
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)

        result = research_firms_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['cached'] == 1
        mock_search.assert_not_called()

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_credit_limit(self, mock_search, tmp_db, sample_firm,
                           sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        mock_search.return_value = [
            {
                'contact_name': 'Jane Doe', 'first_name': 'Jane',
                'last_name': 'Doe', 'contact_email': 'jane@test.com',
                'source': 'hunter_domain_search', 'confidence': 90,
            },
        ]

        result = research_firms_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            credit_limit=1, db_path=tmp_db,
        )
        assert result['credits_used'] <= 1
        assert result['credit_limit_hit'] is True

    @patch('tools.enrich_contacts.HUNTER_API_KEY', 'test_key')
    @patch('tools.enrich_contacts.domain_search')
    def test_progress_callback(self, mock_search, tmp_db, sample_firm,
                                sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        mock_search.return_value = []

        calls = []
        def on_progress(current, total, results):
            calls.append((current, total))

        research_firms_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db, progress_callback=on_progress,
        )
        assert len(calls) == 2
        assert calls[0] == (1, 2)
        assert calls[1] == (2, 2)
