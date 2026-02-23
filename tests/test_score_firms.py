"""Tests for tools/score_firms.py — ICP Fit Scoring."""

from unittest.mock import patch, MagicMock
import pytest

from tools.cache_db import init_db, upsert_firms, upsert_form_adv, get_firm_by_crd
from tools.score_firms import (
    score_firm, score_batch, _score_data, _score_website,
    NAME_ADVISORY_KW, NAME_SCALE_KW, TOP_STATES, WEBSITE_KEYWORDS,
)


# --- _score_data tests ---

class TestScoreData:
    """Tests for data-based scoring (max 50 points)."""

    def test_full_score_ideal_firm(self, tmp_db, sample_firm, sample_form_adv):
        """A well-populated firm with multi-state should score near max."""
        upsert_firms([sample_firm], db_path=tmp_db)
        firm = get_firm_by_crd(sample_firm['crd'], db_path=tmp_db)
        # Create a form_adv dict (not DB row, just dict)
        score, reasons = _score_data(firm, sample_form_adv)
        assert score > 0
        assert score <= 50

    def test_website_presence_adds_points(self, tmp_db):
        """Firms with a website get +8."""
        firm_with = {'website': 'https://example.com', 'company': 'Acme LLC'}
        firm_without = {'website': None, 'company': 'Acme LLC'}
        s1, _ = _score_data(firm_with, None)
        s2, _ = _score_data(firm_without, None)
        assert s1 > s2

    def test_phone_presence_adds_points(self):
        firm_with = {'phone': '212-555-1234', 'company': 'Acme LLC'}
        firm_without = {'phone': None, 'company': 'Acme LLC'}
        s1, _ = _score_data(firm_with, None)
        s2, _ = _score_data(firm_without, None)
        assert s1 > s2

    def test_advisory_name_keywords(self):
        """Company names with advisory keywords score higher."""
        firm = {'company': 'Summit Wealth Advisory Group'}
        score, reasons = _score_data(firm, None)
        reason_text = ' '.join(reasons)
        assert 'Advisory/wealth name' in reason_text or 'Scale/team name' in reason_text

    def test_top_state_bonus(self):
        firm_ny = {'company': 'Acme LLC', 'state': 'NY'}
        firm_wy = {'company': 'Acme LLC', 'state': 'WY'}
        s1, _ = _score_data(firm_ny, None)
        s2, _ = _score_data(firm_wy, None)
        assert s1 > s2

    def test_employee_tiers(self):
        """Higher employee counts should yield higher scores."""
        scores = []
        for emp in [0, 1, 5, 15]:
            firm = {'company': 'Acme LLC', 'employees': emp}
            s, _ = _score_data(firm, None)
            scores.append(s)
        # Monotonically increasing (or at least non-decreasing)
        for i in range(len(scores) - 1):
            assert scores[i] <= scores[i + 1]

    def test_aum_tiers(self):
        """Higher AUM should yield higher scores."""
        scores = []
        for aum in [0, 5_000_000, 50_000_000, 500_000_000, 2_000_000_000]:
            firm = {'company': 'Acme LLC', 'aum': aum}
            s, _ = _score_data(firm, None)
            scores.append(s)
        for i in range(len(scores) - 1):
            assert scores[i] <= scores[i + 1]

    def test_client_tiers(self):
        scores = []
        for clients in [0, 5, 50, 200]:
            firm = {'company': 'Acme LLC', 'clients': clients}
            s, _ = _score_data(firm, None)
            scores.append(s)
        for i in range(len(scores) - 1):
            assert scores[i] <= scores[i + 1]

    def test_multi_state_bonus(self):
        """4+ state registrations add bonus points."""
        firm = {'company': 'Acme LLC'}
        form_adv_multi = {'state_count': 5}
        form_adv_few = {'state_count': 2}
        s1, r1 = _score_data(firm, form_adv_multi)
        s2, r2 = _score_data(firm, form_adv_few)
        assert s1 > s2
        assert any('Multi-state' in r for r in r1)

    def test_score_capped_at_50(self):
        """Data score should never exceed 50."""
        firm = {
            'company': 'Global Wealth Advisory Partners',
            'website': 'https://example.com',
            'phone': '212-555-1234',
            'state': 'NY',
            'employees': 50,
            'clients': 500,
            'aum': 5_000_000_000,
        }
        form_adv = {'state_count': 10}
        score, _ = _score_data(firm, form_adv)
        assert score <= 50

    def test_empty_firm_scores_zero(self):
        """A firm with no data should score 0."""
        score, reasons = _score_data({}, None)
        assert score == 0
        assert len(reasons) == 0


# --- _score_website tests ---

class TestScoreWebsite:
    """Tests for website-based scoring (max 75 points)."""

    @patch('tools.score_firms.requests.get')
    def test_reachable_site_gets_points(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '<html><body>Welcome to our firm</body></html>'
        mock_get.return_value = mock_resp

        score, reasons = _score_website('https://example.com')
        assert score >= 5
        assert any('reachable' in r.lower() for r in reasons)

    @patch('tools.score_firms.requests.get')
    def test_unreachable_site(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        score, reasons = _score_website('https://example.com')
        assert score == 0
        assert any('unreachable' in r.lower() for r in reasons)

    @patch('tools.score_firms.requests.get')
    def test_compliance_keywords_add_points(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '''<html><body>
            We are a SEC registered firm focused on compliance and fiduciary duty.
            Our Form ADV is available for review.
        </body></html>'''
        mock_get.return_value = mock_resp

        score, reasons = _score_website('https://example.com')
        assert score > 5  # More than just "reachable"
        assert any('compliance' in r.lower() for r in reasons)

    @patch('tools.score_firms.requests.get')
    def test_advisory_keywords_add_points(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '''<html><body>
            We provide wealth management, financial planning, and investment advisory services.
        </body></html>'''
        mock_get.return_value = mock_resp

        score, reasons = _score_website('https://example.com')
        assert any('advisory' in r.lower() for r in reasons)

    @patch('tools.score_firms.requests.get')
    def test_score_capped_at_75(self, mock_get):
        # Build a page with ALL keyword categories
        kw_text = ' '.join(
            kw for cat in WEBSITE_KEYWORDS.values() for kw in cat['keywords']
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = f'<html><body>{kw_text}</body></html>'
        mock_get.return_value = mock_resp

        score, _ = _score_website('https://example.com')
        assert score <= 75

    @patch('tools.score_firms.requests.get')
    def test_request_exception(self, mock_get):
        import requests as req
        mock_get.side_effect = req.RequestException('timeout')
        score, reasons = _score_website('https://example.com')
        assert score == 0
        assert any('error' in r.lower() for r in reasons)


# --- score_firm integration tests ---

class TestScoreFirm:
    """Integration tests for score_firm using the database."""

    def test_scores_and_stores(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        score, reasons = score_firm(sample_firm['crd'], db_path=tmp_db)
        assert 0 <= score <= 100
        assert len(reasons) > 0

        # Verify it was stored in the DB
        firm = get_firm_by_crd(sample_firm['crd'], db_path=tmp_db)
        assert firm['fit_score'] is not None
        assert firm['fit_reasons'] is not None

    def test_firm_not_found(self, tmp_db):
        score, reasons = score_firm(0, db_path=tmp_db)
        assert score == 0
        assert 'Firm not found' in reasons

    def test_with_form_adv(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        score, reasons = score_firm(sample_firm['crd'], db_path=tmp_db)
        # With form_adv multi-state data, should score higher
        assert score > 0

    @patch('tools.score_firms.requests.get')
    def test_deep_scoring_includes_website(self, mock_get, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '<html><body>compliance fiduciary wealth management</body></html>'
        mock_get.return_value = mock_resp

        score_deep, reasons_deep = score_firm(sample_firm['crd'], deep=True, db_path=tmp_db)
        # Deep scoring should include website-related reasons
        assert score_deep > 0
        assert any('reachable' in r.lower() or 'keyword' in r.lower() for r in reasons_deep)
        # Verify the mock was called (website was fetched)
        mock_get.assert_called_once()


# --- score_batch tests ---

class TestScoreBatch:
    def test_batch_counts(self, tmp_db, sample_firm, sample_firm_120day):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        results = score_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db,
        )
        assert results['scored'] == 2
        assert results['errors'] == 0

    def test_batch_handles_missing(self, tmp_db):
        results = score_batch([0, 1, 2], db_path=tmp_db)
        # score_firm returns (0, ['Firm not found']) for missing — still counts as scored
        assert results['scored'] == 3
