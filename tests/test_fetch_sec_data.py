"""Tests for fetch_sec_data.py — SEC FOIA download, parsing, and classification."""

import io
import zipfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from tools.fetch_sec_data import (
    parse_sec_dataframe, classify_track, fetch_and_store,
    _safe_int, _safe_str, _build_candidate_urls, download_sec_csv,
    build_candidate_urls, probe_sec_urls,
)

FIXTURES = Path(__file__).parent / 'fixtures'


@pytest.fixture
def sample_df():
    """Load the sample SEC CSV as a DataFrame."""
    return pd.read_csv(FIXTURES / 'sample_sec_rows.csv', dtype=str)


@pytest.fixture
def mock_zip_response(sample_df):
    """Create a mock HTTP response containing a ZIP with the sample CSV."""
    csv_buffer = io.BytesIO()
    sample_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue()

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('ia020126.csv', csv_bytes)
    zip_bytes = zip_buffer.getvalue()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = zip_bytes
    return mock_resp


class TestSafeConversions:
    def test_safe_int_normal(self):
        assert _safe_int('150000000') == 150000000

    def test_safe_int_commas(self):
        assert _safe_int('150,000,000') == 150000000

    def test_safe_int_dollar(self):
        assert _safe_int('$150,000,000') == 150000000

    def test_safe_int_blank(self):
        assert _safe_int('') is None
        assert _safe_int(None) is None

    def test_safe_int_whitespace(self):
        assert _safe_int('  42  ') == 42

    def test_safe_int_float(self):
        assert _safe_int('150000000.0') == 150000000

    def test_safe_str_normal(self):
        assert _safe_str('  Hello  ') == 'Hello'

    def test_safe_str_blank(self):
        assert _safe_str('') is None
        assert _safe_str(None) is None


class TestCandidateUrls:
    def test_generates_urls(self):
        urls = _build_candidate_urls()
        assert len(urls) > 0
        for url, label in urls:
            assert url.startswith('https://www.sec.gov/')
            assert url.endswith('.zip')


class TestParseSecDataframe:
    def test_parses_columns(self, sample_df):
        records = parse_sec_dataframe(sample_df)
        assert len(records) == 5
        first = records[0]
        assert first['company'] == 'Central Wealth Management LLC'
        assert first['crd'] == 100001
        assert first['aum'] == 95000000
        assert first['state'] == 'IL'

    def test_cleans_aum(self, sample_df):
        records = parse_sec_dataframe(sample_df)
        for r in records:
            if r['aum'] is not None:
                assert isinstance(r['aum'], (int, type(None)))

    def test_handles_missing_website(self, sample_df):
        records = parse_sec_dataframe(sample_df)
        tiny = next(r for r in records if r['crd'] == 100004)
        assert tiny['website'] is None


class TestClassifyTrack:
    def test_track_a_120day(self):
        record = {'status': '120-Day Approval'}
        assert classify_track(record) == 'A'

    def test_track_a_pending(self):
        record = {'status': 'Pending'}
        assert classify_track(record) == 'A'

    def test_no_track_approved(self):
        record = {'status': 'Approved'}
        assert classify_track(record) is None

    def test_no_track_empty_status(self):
        record = {'status': ''}
        assert classify_track(record) is None

    def test_no_track_none_status(self):
        record = {'status': None}
        assert classify_track(record) is None


class TestFetchAndStore:
    @patch('tools.fetch_sec_data.download_sec_csv')
    def test_stores_targeted_firms(self, mock_download, sample_df, tmp_db):
        mock_download.return_value = sample_df
        result = fetch_and_store(db_path=tmp_db)
        assert result['downloaded'] == 5
        assert result['firms_imported'] >= 1  # At least the 120-day firm
        assert result['skipped'] >= 1  # Non-120-day firms

    @patch('tools.fetch_sec_data.download_sec_csv')
    def test_handles_download_failure(self, mock_download, tmp_db):
        mock_download.return_value = None
        result = fetch_and_store(db_path=tmp_db)
        assert result['downloaded'] == 0
        assert 'error' in result


class TestDownloadSecCsv:
    def test_successful_download(self, mock_zip_response):
        with patch('tools.fetch_sec_data.requests.get', return_value=mock_zip_response):
            df = download_sec_csv(url='https://example.com/test.zip')
            assert df is not None
            assert len(df) == 5

    def test_failed_download(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch('tools.fetch_sec_data.requests.get', return_value=mock_resp):
            df = download_sec_csv(url='https://example.com/missing.zip')
            assert df is None


class TestBuildCandidateUrlsPublic:
    def test_returns_same_as_private(self):
        public = build_candidate_urls()
        private = _build_candidate_urls()
        assert public == private

    def test_returns_list_of_tuples(self):
        results = build_candidate_urls()
        assert len(results) > 0
        for url, date_label in results:
            assert url.startswith('https://www.sec.gov/')
            assert url.endswith('.zip')


class TestProbeSecUrls:
    @patch('tools.fetch_sec_data.requests.head')
    def test_available_url(self, mock_head):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'Content-Length': '52428800'}  # 50MB
        mock_head.return_value = mock_resp

        candidates = [('https://example.com/ia020126.zip', '2026-02-01')]
        results = probe_sec_urls(candidates)
        assert len(results) == 1
        assert results[0]['available'] is True
        assert results[0]['size_mb'] == 50.0
        assert results[0]['date_label'] == '2026-02-01'

    @patch('tools.fetch_sec_data.requests.head')
    def test_unavailable_url(self, mock_head):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_head.return_value = mock_resp

        candidates = [('https://example.com/ia990101.zip', '1999-01-01')]
        results = probe_sec_urls(candidates)
        assert len(results) == 1
        assert results[0]['available'] is False

    @patch('tools.fetch_sec_data.requests.head')
    def test_request_exception(self, mock_head):
        import requests
        mock_head.side_effect = requests.RequestException('timeout')

        candidates = [('https://example.com/ia020126.zip', '2026-02-01')]
        results = probe_sec_urls(candidates)
        assert len(results) == 1
        assert results[0]['available'] is False
        assert results[0]['size_mb'] is None

    @patch('tools.fetch_sec_data.requests.head')
    def test_mixed_availability(self, mock_head):
        def side_effect(url, **kwargs):
            resp = MagicMock()
            if '0201' in url:
                resp.status_code = 200
                resp.headers = {'Content-Length': '10485760'}
            else:
                resp.status_code = 404
                resp.headers = {}
            return resp
        mock_head.side_effect = side_effect

        candidates = [
            ('https://example.com/ia020126.zip', '2026-02-01'),
            ('https://example.com/ia010226.zip', '2026-01-02'),
        ]
        results = probe_sec_urls(candidates)
        assert results[0]['available'] is True
        assert results[1]['available'] is False
