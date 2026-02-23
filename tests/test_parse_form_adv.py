"""Tests for tools/parse_form_adv.py — EDGAR CCO extraction."""

import json
from unittest.mock import patch, MagicMock

import pytest

from tools.parse_form_adv import (
    _clean_company_name,
    _is_valid_person_name,
    _extract_cco_from_13f_xml,
    _extract_cco_from_form_d_xml,
    _format_phone,
    search_edgar_cco,
    extract_cco_batch,
)


# --- Unit tests for helpers ---

class TestCleanCompanyName:
    def test_strips_corporate_suffixes(self):
        assert _clean_company_name("ACME CAPITAL, LLC") == "ACME"

    def test_strips_multiple_suffixes(self):
        assert _clean_company_name("ACME INVESTMENT ADVISORS, INC.") == "ACME"

    def test_preserves_core_name(self):
        name = _clean_company_name("DUPREE & COMPANY, INC.")
        assert "DUPREE" in name

    def test_truncates_long_names(self):
        result = _clean_company_name("A" * 100 + " CAPITAL, LLC")
        assert len(result) <= 40

    def test_handles_empty(self):
        assert _clean_company_name("") == ""

    def test_comma_splits(self):
        result = _clean_company_name("SMITH BARNEY, A DIVISION OF CITIGROUP")
        assert "SMITH BARNEY" in result


class TestIsValidPersonName:
    def test_valid_two_word_name(self):
        assert _is_valid_person_name("John Smith") is True

    def test_valid_three_word_name(self):
        assert _is_valid_person_name("John Michael Smith") is True

    def test_rejects_single_word(self):
        assert _is_valid_person_name("John") is False

    def test_rejects_title_words(self):
        assert _is_valid_person_name("Vice President") is False

    def test_rejects_too_many_words(self):
        assert _is_valid_person_name("A B C D E F") is False

    def test_rejects_empty(self):
        assert _is_valid_person_name("") is False
        assert _is_valid_person_name(None) is False

    def test_rejects_all_caps_long(self):
        assert _is_valid_person_name("ACME INVESTMENT ADVISORS") is False

    def test_accepts_mixed_case_initials(self):
        assert _is_valid_person_name("William M. Ambrose") is True


class TestFormatPhone:
    def test_ten_digits(self):
        assert _format_phone("2129705713") == "212-970-5713"

    def test_eleven_digits_with_country(self):
        assert _format_phone("12129705713") == "212-970-5713"

    def test_already_formatted(self):
        assert _format_phone("212-970-5713") == "212-970-5713"

    def test_none_input(self):
        assert _format_phone(None) is None

    def test_empty_string(self):
        assert _format_phone("") is None

    def test_with_parens(self):
        assert _format_phone("(212) 970-5713") == "212-970-5713"


# --- Unit tests for XML extraction ---

SAMPLE_13F_XML = """<?xml version="1.0"?>
<edgarSubmission>
  <coverPage>
    <reportCalendarOrQuarter>12-31-2025</reportCalendarOrQuarter>
    <filingManager><name>Test Capital LP</name></filingManager>
    <crdNumber>00123456</crdNumber>
  </coverPage>
  <signatureBlock>
    <name>Jane Doe</name>
    <title>Chief Compliance Officer</title>
    <phone>5551234567</phone>
    <signature>Jane Doe</signature>
    <city>New York</city>
    <stateOrCountry>NY</stateOrCountry>
    <signatureDate>02-14-2026</signatureDate>
  </signatureBlock>
</edgarSubmission>
"""

SAMPLE_FORM_D_XML = """<?xml version="1.0"?>
<edgarSubmission>
  <offeringData>
    <signatureBlock>
      <signature>
        <signatureName>/s/ Jane Doe</signatureName>
        <nameOfSigner>Jane Doe</nameOfSigner>
        <signatureTitle>Chief Compliance Officer</signatureTitle>
        <signatureDate>2025-09-26</signatureDate>
      </signature>
    </signatureBlock>
  </offeringData>
</edgarSubmission>
"""


class TestExtractFrom13F:
    def test_extracts_cco(self):
        result = _extract_cco_from_13f_xml(SAMPLE_13F_XML)
        assert result is not None
        assert result['cco_name'] == 'Jane Doe'
        assert result['cco_title'] == 'Chief Compliance Officer'
        assert result['cco_phone'] == '555-123-4567'

    def test_returns_none_when_no_compliance_title(self):
        xml = SAMPLE_13F_XML.replace('Chief Compliance Officer', 'CEO')
        assert _extract_cco_from_13f_xml(xml) is None

    def test_returns_none_when_name_is_title(self):
        xml = SAMPLE_13F_XML.replace('Jane Doe', 'Vice President')
        assert _extract_cco_from_13f_xml(xml) is None

    def test_returns_none_on_empty(self):
        assert _extract_cco_from_13f_xml("") is None


class TestExtractFromFormD:
    def test_extracts_cco(self):
        result = _extract_cco_from_form_d_xml(SAMPLE_FORM_D_XML)
        assert result is not None
        assert result['cco_name'] == 'Jane Doe'
        assert 'Compliance Officer' in result['cco_title']

    def test_strips_signature_prefix(self):
        result = _extract_cco_from_form_d_xml(SAMPLE_FORM_D_XML)
        assert '/s/' not in result['cco_name']

    def test_returns_none_when_no_compliance(self):
        xml = SAMPLE_FORM_D_XML.replace('Compliance Officer', 'CEO')
        assert _extract_cco_from_form_d_xml(xml) is None


# --- Integration tests (mocked HTTP) ---

class TestSearchEdgarCCO:
    @patch('tools.parse_form_adv.requests.get')
    def test_successful_13f_extraction(self, mock_get):
        # Mock EFTS search response
        efts_response = {
            'hits': {
                'total': {'value': 1},
                'hits': [{
                    '_id': '0001234567-26-000001:primary_doc.xml',
                    '_source': {
                        'ciks': ['0001234567'],
                        'root_forms': ['13F-HR'],
                        'file_date': '2026-01-15',
                    },
                }],
            },
        }
        mock_efts = MagicMock()
        mock_efts.status_code = 200
        mock_efts.json.return_value = efts_response

        mock_filing = MagicMock()
        mock_filing.status_code = 200
        mock_filing.text = SAMPLE_13F_XML

        mock_get.side_effect = [mock_efts, mock_filing]

        result = search_edgar_cco("Test Capital LP")
        assert result is not None
        assert result['cco_name'] == 'Jane Doe'
        assert result['source'] == 'edgar_13F-HR'

    @patch('tools.parse_form_adv.requests.get')
    def test_returns_none_when_no_hits(self, mock_get):
        empty_response = {'hits': {'total': {'value': 0}, 'hits': []}}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = empty_response

        # Two searches (13F-HR + D), both empty
        mock_get.return_value = mock_resp

        result = search_edgar_cco("Nonexistent Firm")
        assert result is None

    @patch('tools.parse_form_adv.requests.get')
    def test_handles_network_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.RequestException("Network error")

        result = search_edgar_cco("Test Firm")
        assert result is None


class TestExtractCCOBatch:
    @patch('tools.parse_form_adv.extract_cco')
    @patch('tools.parse_form_adv._get_stale_cco_crds')
    @patch('tools.parse_form_adv.init_db')
    def test_batch_counts(self, mock_init, mock_stale, mock_extract):
        mock_stale.return_value = [1, 2, 3]
        mock_extract.side_effect = [
            {'cco_name': 'Jane Doe'},  # success
            None,                       # no result
            Exception("error"),         # error
        ]

        # Fix: the third call raises, so use side_effect properly
        def side_effect(crd, company, db_path=None):
            if crd == 1:
                return {'cco_name': 'Jane Doe'}
            elif crd == 2:
                return None
            else:
                raise Exception("error")

        mock_extract.side_effect = side_effect

        pairs = [(1, 'Firm A'), (2, 'Firm B'), (3, 'Firm C')]
        result = extract_cco_batch(pairs)

        assert result['extracted'] == 1
        assert result['no_result'] == 1
        assert result['errors'] == 1
        assert result['cached'] == 0

    @patch('tools.parse_form_adv.extract_cco')
    @patch('tools.parse_form_adv._get_stale_cco_crds')
    @patch('tools.parse_form_adv.init_db')
    def test_skips_cached(self, mock_init, mock_stale, mock_extract):
        # CRD 1 is stale, CRD 2 is cached (not in stale list)
        mock_stale.return_value = [1]
        mock_extract.return_value = {'cco_name': 'Jane Doe'}

        pairs = [(1, 'Firm A'), (2, 'Firm B')]
        result = extract_cco_batch(pairs)

        assert result['extracted'] == 1
        assert result['cached'] == 1

    @patch('tools.parse_form_adv.extract_cco')
    @patch('tools.parse_form_adv._get_stale_cco_crds')
    @patch('tools.parse_form_adv.init_db')
    def test_progress_callback(self, mock_init, mock_stale, mock_extract):
        mock_stale.return_value = [1]
        mock_extract.return_value = {'cco_name': 'Test'}

        calls = []
        def callback(current, total, results):
            calls.append((current, total))

        pairs = [(1, 'Firm A'), (2, 'Firm B')]
        extract_cco_batch(pairs, progress_callback=callback)

        assert len(calls) == 2
        assert calls[0] == (1, 2)
        assert calls[1] == (2, 2)
