"""Shared pytest fixtures for SEC & Contact Research tests."""

import os
import tempfile
import pytest
from tools.cache_db import init_db, get_connection


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test_surge.db"
    init_db(db_path)
    return db_path


@pytest.fixture
def sample_firm():
    """A sample firm record matching SEC FOIA CSV structure."""
    return {
        'crd': 999001,
        'company': 'Test Wealth Management LLC',
        'legal_name': 'Test Wealth Management LLC',
        'status': 'Approved',
        'status_date': '2026-01-15',
        'filing_date': '2026-01-10',
        'city': 'New York',
        'state': 'NY',
        'phone': '212-555-1234',
        'website': 'https://www.testwm.com',
        'sec_registered': 'Y',
        'era': 'N',
        'employees': 12,
        'clients': 85,
        'aum': 150000000,
        'aum_discretionary': 120000000,
        'aum_nondiscretionary': 30000000,
        'track': 'A',
    }


@pytest.fixture
def sample_firm_120day():
    """A firm in 120-day approval status."""
    return {
        'crd': 999002,
        'company': 'Central Wealth Advisors',
        'legal_name': 'Central Wealth Advisors Inc',
        'status': '120-Day Approval',
        'status_date': '2026-02-01',
        'filing_date': '2026-01-28',
        'city': 'Chicago',
        'state': 'IL',
        'phone': '312-555-9876',
        'website': 'https://www.centralwealth.com',
        'sec_registered': 'Y',
        'era': 'N',
        'employees': 5,
        'clients': 42,
        'aum': 95000000,
        'aum_discretionary': 80000000,
        'aum_nondiscretionary': 15000000,
        'track': 'A',
    }


@pytest.fixture
def sample_firm_near_threshold():
    """A state-registered firm near the SEC threshold."""
    return {
        'crd': 999003,
        'company': 'Growing Advisory Partners',
        'legal_name': 'Growing Advisory Partners LLC',
        'status': 'Approved',
        'status_date': '2025-06-15',
        'filing_date': '2025-06-10',
        'city': 'Dallas',
        'state': 'TX',
        'phone': '214-555-4567',
        'website': 'https://www.growingadvisory.com',
        'sec_registered': 'N',
        'era': 'N',
        'employees': 8,
        'clients': 65,
        'aum': 92000000,
        'aum_discretionary': 75000000,
        'aum_nondiscretionary': 17000000,
        'track': 'B',
    }


@pytest.fixture
def sample_contact():
    """A sample enriched contact."""
    return {
        'contact_name': 'Eric Heiting',
        'contact_email': 'eheiting@testwm.com',
        'contact_title': 'Managing Principal / CCO',
        'contact_phone': '212-555-1235',
        'contact_linkedin': 'https://linkedin.com/in/ericheiting',
        'source': 'hunter_io',
        'confidence': 85.0,
    }


@pytest.fixture
def sample_form_adv():
    """Sample Form ADV details."""
    return {
        'cco_name': 'Eric Heiting',
        'cco_email': 'eheiting@testwm.com',
        'cco_phone': '212-555-1235',
        'state_registrations': 'IL,IA,TX,WI',
        'state_count': 4,
        'aum_breakdown': '{"discretionary": 120000000, "non_discretionary": 30000000}',
    }
