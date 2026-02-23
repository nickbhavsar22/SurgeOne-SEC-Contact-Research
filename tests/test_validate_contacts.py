"""Tests for validate_contacts.py — contact data validation."""

import pytest
from datetime import datetime, timedelta

from tools.validate_contacts import validate_contact, validate_batch, EMAIL_PATTERN, PHONE_PATTERN
from tools.cache_db import upsert_firms, upsert_contact, upsert_form_adv, get_connection


class TestEmailPattern:
    def test_valid_emails(self):
        assert EMAIL_PATTERN.match('john@example.com')
        assert EMAIL_PATTERN.match('john.doe@example.com')
        assert EMAIL_PATTERN.match('john+tag@example.co.uk')

    def test_invalid_emails(self):
        assert not EMAIL_PATTERN.match('notanemail')
        assert not EMAIL_PATTERN.match('@example.com')
        assert not EMAIL_PATTERN.match('john@')


class TestPhonePattern:
    def test_valid_phones(self):
        assert PHONE_PATTERN.match('212-555-1234')
        assert PHONE_PATTERN.match('(212) 555-1234')
        assert PHONE_PATTERN.match('+1-212-555-1234')
        assert PHONE_PATTERN.match('2125551234')

    def test_invalid_phones(self):
        assert not PHONE_PATTERN.match('123')
        assert not PHONE_PATTERN.match('abc-def-ghij')


class TestValidateContact:
    def test_valid_contact(self, tmp_db, sample_firm, sample_contact, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)

        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'valid'
        assert len(issues) == 0

    def test_no_contact(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'invalid'
        assert 'No contact found' in issues

    def test_invalid_email(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        bad_contact = {
            'contact_name': 'Test', 'contact_email': 'not-an-email',
            'source': 'test', 'confidence': 50.0,
        }
        upsert_contact(sample_firm['crd'], bad_contact, db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'invalid'
        assert any('Invalid email' in i for i in issues)

    def test_domain_mismatch(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        mismatch_contact = {
            'contact_name': 'Test', 'contact_email': 'test@wrongdomain.com',
            'source': 'test', 'confidence': 50.0,
        }
        upsert_contact(sample_firm['crd'], mismatch_contact, db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'suspect'
        assert any('domain' in i.lower() for i in issues)

    def test_name_mismatch(self, tmp_db, sample_firm, sample_form_adv):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_form_adv(sample_firm['crd'], sample_form_adv, db_path=tmp_db)
        wrong_name_contact = {
            'contact_name': 'Totally Different Person',
            'contact_email': 'tdp@testwm.com',
            'source': 'test', 'confidence': 50.0,
        }
        upsert_contact(sample_firm['crd'], wrong_name_contact, db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert any('does not match' in i for i in issues)

    def test_stale_contact(self, tmp_db, sample_firm, sample_contact):
        upsert_firms([sample_firm], db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)
        # Set enriched_at to 100 days ago
        conn = get_connection(tmp_db)
        old_date = (datetime.utcnow() - timedelta(days=100)).isoformat()
        conn.execute("UPDATE contacts SET enriched_at=? WHERE crd=?",
                     (old_date, sample_firm['crd']))
        conn.commit()
        conn.close()

        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert any('stale' in i.lower() for i in issues)

    def test_no_email_is_invalid(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        no_email = {
            'contact_name': 'Test Person', 'contact_email': None,
            'source': 'test', 'confidence': 20.0,
        }
        upsert_contact(sample_firm['crd'], no_email, db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'invalid'

    def test_generic_email_is_suspect(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        generic = {
            'contact_name': 'Test Person',
            'contact_email': 'info@testwm.com',
            'source': 'test', 'confidence': 60.0,
        }
        upsert_contact(sample_firm['crd'], generic, db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'suspect'
        assert any('generic' in i.lower() or 'role-based' in i.lower() for i in issues)

    def test_reporting_email_is_suspect(self, tmp_db, sample_firm):
        upsert_firms([sample_firm], db_path=tmp_db)
        generic = {
            'contact_name': 'Test Person',
            'contact_email': 'reporting@testwm.com',
            'source': 'test', 'confidence': 60.0,
        }
        upsert_contact(sample_firm['crd'], generic, db_path=tmp_db)
        status, issues = validate_contact(sample_firm['crd'], db_path=tmp_db)
        assert status == 'suspect'


class TestValidateBatch:
    def test_batch_counts(self, tmp_db, sample_firm, sample_firm_120day,
                          sample_contact):
        upsert_firms([sample_firm, sample_firm_120day], db_path=tmp_db)
        upsert_contact(sample_firm['crd'], sample_contact, db_path=tmp_db)

        results = validate_batch(
            [sample_firm['crd'], sample_firm_120day['crd']],
            db_path=tmp_db
        )
        assert results['no_contact'] == 1
        assert results['valid'] + results['suspect'] == 1
