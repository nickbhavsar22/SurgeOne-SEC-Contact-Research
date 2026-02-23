"""Tests for tools/parse_form_adv.py — Form ADV PDF contact extraction."""

import io
from unittest.mock import patch, MagicMock

import pytest

from tools.parse_form_adv import (
    _is_valid_person_name,
    _format_phone,
    _is_generic_email,
    _extract_phone_near_name,
    extract_contacts_from_pdf,
    extract_contacts_batch,
)


# --- Unit tests for helpers ---

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

    def test_rejects_corporate_names(self):
        assert _is_valid_person_name("Epogee Capital Management") is False
        assert _is_valid_person_name("Spring Street Wealth") is False


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


class TestIsGenericEmail:
    def test_sec_gov(self):
        assert _is_generic_email('filing@sec.gov') is True

    def test_finra(self):
        assert _is_generic_email('john@finra.org') is True

    def test_info_prefix(self):
        assert _is_generic_email('info@company.com') is True

    def test_compliance_prefix(self):
        assert _is_generic_email('compliance@firm.com') is True

    def test_personal_email(self):
        assert _is_generic_email('jsmith@advisors.com') is False

    def test_none_is_generic(self):
        assert _is_generic_email(None) is True

    def test_empty_is_generic(self):
        assert _is_generic_email('') is True


class TestExtractPhoneNearName:
    def test_finds_phone_after_name(self):
        text = "John Smith\nTelephone: (212) 555-1234\nFax: 212-555-9999"
        result = _extract_phone_near_name(text, "John Smith")
        assert result == "212-555-1234"

    def test_returns_none_when_no_phone(self):
        text = "John Smith is the principal."
        assert _extract_phone_near_name(text, "John Smith") is None

    def test_returns_none_when_name_not_found(self):
        text = "Jane Doe\nTelephone: 555-1234"
        assert _extract_phone_near_name(text, "John Smith") is None

    def test_returns_none_on_empty(self):
        assert _extract_phone_near_name("", "John") is None
        assert _extract_phone_near_name(None, "John") is None
        assert _extract_phone_near_name("text", None) is None


# --- PDF extraction tests (mocked HTTP + pdfplumber) ---

SAMPLE_PDF_TEXT_WITH_CONTACTS = """
FORM ADV
Part 1A

your last, first, and middle names):
John Michael Smith
B. Registration details...

J. Chief Compliance Officer
Name: Jane Doe
Other titles: Managing Director
Telephone: (212) 555-9876

Schedule A - Direct Owners
Name: Robert Johnson
Title: Senior Partner

Full Legal Name: Sarah Williams
Position: Vice President of Operations

Email contacts: jsmith@testfirm.com
jane.doe@testfirm.com
info@testfirm.com
"""

SAMPLE_PDF_TEXT_NO_CONTACTS = """
FORM ADV
Part 1A

This is a template form with no filled-in values.
Name:
Title:
Telephone:
"""


def _make_mock_pdf(text):
    """Create a mock pdfplumber PDF that returns the given text."""
    mock_page = MagicMock()
    mock_page.extract_text.return_value = text

    mock_pdf = MagicMock()
    mock_pdf.pages = [mock_page]
    mock_pdf.__enter__ = lambda self: self
    mock_pdf.__exit__ = MagicMock(return_value=False)
    return mock_pdf


class TestExtractContactsFromPdf:
    @patch('tools.parse_form_adv.pdfplumber.open')
    @patch('tools.parse_form_adv.requests.get')
    def test_extracts_principal_and_cco(self, mock_get, mock_pdf_open):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'fake-pdf-bytes'
        mock_get.return_value = mock_resp
        mock_pdf_open.return_value = _make_mock_pdf(SAMPLE_PDF_TEXT_WITH_CONTACTS)

        contacts = extract_contacts_from_pdf(123456)
        assert len(contacts) >= 2
        names = [c['name'] for c in contacts]
        assert 'John Michael Smith' in names
        assert 'Jane Doe' in names
        # Check titles
        principal = next(c for c in contacts if c['name'] == 'John Michael Smith')
        assert principal['title'] == 'Principal/Owner'
        cco = next(c for c in contacts if c['name'] == 'Jane Doe')
        assert cco['title'] == 'Chief Compliance Officer'

    @patch('tools.parse_form_adv.pdfplumber.open')
    @patch('tools.parse_form_adv.requests.get')
    def test_extracts_emails(self, mock_get, mock_pdf_open):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'fake-pdf-bytes'
        mock_get.return_value = mock_resp
        mock_pdf_open.return_value = _make_mock_pdf(SAMPLE_PDF_TEXT_WITH_CONTACTS)

        contacts = extract_contacts_from_pdf(123456)
        # Should assign non-generic emails to contacts
        emails = [c.get('email') for c in contacts if c.get('email')]
        assert len(emails) >= 1
        # info@ should be filtered as generic
        assert 'info@testfirm.com' not in emails

    @patch('tools.parse_form_adv.pdfplumber.open')
    @patch('tools.parse_form_adv.requests.get')
    def test_returns_empty_for_no_contacts(self, mock_get, mock_pdf_open):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'fake-pdf-bytes'
        mock_get.return_value = mock_resp
        mock_pdf_open.return_value = _make_mock_pdf(SAMPLE_PDF_TEXT_NO_CONTACTS)

        contacts = extract_contacts_from_pdf(999999)
        assert contacts == []

    @patch('tools.parse_form_adv.requests.get')
    def test_returns_empty_on_http_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        contacts = extract_contacts_from_pdf(999999)
        assert contacts == []

    @patch('tools.parse_form_adv.requests.get')
    def test_returns_empty_on_network_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.RequestException("timeout")

        contacts = extract_contacts_from_pdf(999999)
        assert contacts == []

    @patch('tools.parse_form_adv.pdfplumber.open')
    @patch('tools.parse_form_adv.requests.get')
    def test_contact_sources(self, mock_get, mock_pdf_open):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'fake-pdf-bytes'
        mock_get.return_value = mock_resp
        mock_pdf_open.return_value = _make_mock_pdf(SAMPLE_PDF_TEXT_WITH_CONTACTS)

        contacts = extract_contacts_from_pdf(123456)
        sources = {c['source'] for c in contacts}
        # Should have at least principal and cco sources
        assert 'pdf_principal' in sources
        assert 'pdf_cco' in sources


class TestExtractContactsBatch:
    @patch('tools.parse_form_adv.extract_contacts_from_pdf')
    @patch('tools.parse_form_adv.time.sleep')
    def test_batch_counts(self, mock_sleep, mock_extract, tmp_db, sample_firm,
                           sample_firm_120day):
        from tools.cache_db import upsert_firms
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)

        mock_extract.side_effect = [
            [{'name': 'Jane Doe', 'title': 'CCO', 'email': None,
              'phone': None, 'source': 'pdf_cco'}],
            [],  # no contacts found
        ]

        result = extract_contacts_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db,
        )
        assert result['processed'] == 1
        assert result['no_contacts'] == 1
        assert result['errors'] == 0

    @patch('tools.parse_form_adv.extract_contacts_from_pdf')
    @patch('tools.parse_form_adv.time.sleep')
    def test_skips_cached(self, mock_sleep, mock_extract, tmp_db, sample_firm,
                           sample_form_adv):
        from tools.cache_db import upsert_firms, upsert_form_adv
        upsert_firms([sample_firm], db_path=tmp_db)
        # Mark as recently processed
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)

        result = extract_contacts_batch(
            [sample_firm['crd']], db_path=tmp_db,
        )
        assert result['cached'] == 1
        mock_extract.assert_not_called()

    @patch('tools.parse_form_adv.extract_contacts_from_pdf')
    @patch('tools.parse_form_adv.time.sleep')
    def test_progress_callback(self, mock_sleep, mock_extract, tmp_db,
                                sample_firm, sample_firm_120day):
        from tools.cache_db import upsert_firms
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        mock_extract.return_value = []

        calls = []
        def callback(current, total, results):
            calls.append((current, total))

        extract_contacts_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db, progress_callback=callback,
        )
        assert len(calls) == 2
        assert calls[0] == (1, 2)
        assert calls[1] == (2, 2)
