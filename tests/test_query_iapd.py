"""Tests for query_iapd.py — IAPD search API with mocked responses."""

from unittest.mock import patch, MagicMock
import json
import pytest

from tools.query_iapd import (
    query_firm_adv, _state_to_abbr, query_firms_batch,
)
from tools.cache_db import upsert_firms, get_form_adv


# Sample IAPD search API response (mirrors real structure)
SAMPLE_IAPD_API_RESPONSE = {
    "hits": {
        "total": 1,
        "hits": [{
            "_type": "_doc",
            "_source": {
                "iacontent": json.dumps({
                    "basicInformation": {
                        "firmId": 293172,
                        "firmName": "CENTRAL WEALTH MANAGEMENT LLC",
                    },
                    "registrationStatus": [
                        {"secJurisdiction": "SEC", "status": "120-Day Approval", "effectiveDate": "12/19/2025"},
                        {"secJurisdiction": "Illinois", "status": "Terminated", "effectiveDate": "6/4/2021"},
                        {"secJurisdiction": "Iowa", "status": "Terminated", "effectiveDate": "4/14/2021"},
                        {"secJurisdiction": "Texas", "status": "Terminated", "effectiveDate": "4/14/2021"},
                        {"secJurisdiction": "Wisconsin", "status": "Terminated", "effectiveDate": "4/14/2021"},
                    ],
                    "noticeFilings": [
                        {"jurisdiction": "Wisconsin", "status": "Notice Filed", "effectiveDate": "12/19/2025"},
                    ],
                })
            }
        }]
    }
}

SAMPLE_IAPD_MULTI_STATE = {
    "hits": {
        "total": 1,
        "hits": [{
            "_type": "_doc",
            "_source": {
                "iacontent": json.dumps({
                    "basicInformation": {"firmId": 100001, "firmName": "Multi State Firm"},
                    "registrationStatus": [
                        {"secJurisdiction": "SEC", "status": "Approved", "effectiveDate": "1/1/2025"},
                    ],
                    "noticeFilings": [
                        {"jurisdiction": "New York", "status": "Notice Filed", "effectiveDate": "1/1/2025"},
                        {"jurisdiction": "California", "status": "Notice Filed", "effectiveDate": "1/1/2025"},
                        {"jurisdiction": "Texas", "status": "Notice Filed", "effectiveDate": "1/1/2025"},
                        {"jurisdiction": "Illinois", "status": "Notice Filed", "effectiveDate": "1/1/2025"},
                        {"jurisdiction": "Florida", "status": "Notice Filed", "effectiveDate": "1/1/2025"},
                    ],
                })
            }
        }]
    }
}

SAMPLE_IAPD_EMPTY = {
    "hits": {"total": 0, "hits": []}
}


class TestStateToAbbr:
    def test_full_name(self):
        assert _state_to_abbr('Wisconsin') == 'WI'
        assert _state_to_abbr('New York') == 'NY'
        assert _state_to_abbr('District of Columbia') == 'DC'

    def test_abbreviation(self):
        assert _state_to_abbr('WI') == 'WI'
        assert _state_to_abbr('NY') == 'NY'

    def test_case_insensitive(self):
        assert _state_to_abbr('wisconsin') == 'WI'
        assert _state_to_abbr('TEXAS') == 'TX'

    def test_none_and_empty(self):
        assert _state_to_abbr(None) is None
        assert _state_to_abbr('') is None

    def test_unknown(self):
        assert _state_to_abbr('Narnia') is None
        assert _state_to_abbr('SEC') is None


class TestQueryFirmAdv:
    @patch('tools.query_iapd.requests.get')
    def test_extracts_state_registrations(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = SAMPLE_IAPD_API_RESPONSE
        mock_get.return_value = mock_resp

        result = query_firm_adv(293172)
        assert result['state_registrations'] == 'WI'
        assert result['state_count'] == 1

    @patch('tools.query_iapd.requests.get')
    def test_multi_state_notice_filings(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = SAMPLE_IAPD_MULTI_STATE
        mock_get.return_value = mock_resp

        result = query_firm_adv(100001)
        states = result['state_registrations'].split(',')
        assert result['state_count'] == 5
        assert 'NY' in states
        assert 'CA' in states
        assert 'TX' in states

    @patch('tools.query_iapd.requests.get')
    def test_handles_empty_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = SAMPLE_IAPD_EMPTY
        mock_get.return_value = mock_resp

        result = query_firm_adv(999999)
        assert result['cco_name'] is None
        assert result['state_count'] == 0

    @patch('tools.query_iapd.requests.get')
    def test_handles_api_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_get.return_value = mock_resp

        result = query_firm_adv(100001)
        assert result['cco_name'] is None
        assert result['state_count'] == 0

    @patch('tools.query_iapd.requests.get')
    def test_handles_network_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.RequestException("Connection failed")

        result = query_firm_adv(100001)
        assert result['cco_name'] is None
        assert result['state_count'] == 0


class TestQueryFirmsBatch:
    @patch('tools.query_iapd.query_firm_adv')
    @patch('tools.query_iapd.time.sleep')
    def test_skips_cached(self, mock_sleep, mock_query, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        from tools.cache_db import upsert_form_adv as db_upsert
        db_upsert(sample_firm['crd'], sample_form_adv, db_path=tmp_db)

        result = query_firms_batch([sample_firm['crd']], db_path=tmp_db)
        assert result['cached'] == 1
        assert result['queried'] == 0
        mock_query.assert_not_called()

    @patch('tools.query_iapd.query_firm_adv')
    @patch('tools.query_iapd.time.sleep')
    def test_queries_stale(self, mock_sleep, mock_query, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_query.return_value = {
            'cco_name': None, 'cco_email': None,
            'cco_phone': None, 'state_registrations': 'NY',
            'state_count': 1, 'aum_breakdown': None,
        }

        result = query_firms_batch([sample_firm['crd']], db_path=tmp_db)
        assert result['queried'] == 1
        assert result['cached'] == 0
        adv = get_form_adv(sample_firm['crd'], db_path=tmp_db)
        assert adv['state_registrations'] == 'NY'
        assert adv['state_count'] == 1
