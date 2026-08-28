"""Тесты для getmail_filter (фильтрация писем)."""
import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import email
from email.policy import default
import getmail_filter

def test_get_rules_from_db(temp_db):
    conn, path = temp_db
    # Вставляем правило
    conn.execute("""
        INSERT INTO filter_rules (id, from_pattern, to_pattern, subject_pattern, parser_script, enabled)
        VALUES (1, 'test@example.com', 'me@example.com', 'Test', 'parser.py', 1)
    """)
    conn.commit()
    with patch('getmail_filter.get_db_connection', return_value=conn):
        rules = getmail_filter.get_rules_from_db()
        assert len(rules) == 1
        assert rules[0]['from_pattern'] == 'test@example.com'

def test_is_already_imported(temp_db):
    conn, path = temp_db
    conn.execute("INSERT INTO imported_emails (mail_id) VALUES ('msg123')")
    conn.commit()
    with patch('getmail_filter.get_db_connection', return_value=conn):
        assert getmail_filter.is_already_imported('msg123') is True
        assert getmail_filter.is_already_imported('msg456') is False

def test_mark_imported(temp_db):
    conn, path = temp_db
    with patch('getmail_filter.get_db_connection', return_value=conn):
        getmail_filter.mark_imported('new_msg', 1)
        cur = conn.execute("SELECT * FROM imported_emails WHERE mail_id = 'new_msg'")
        row = cur.fetchone()
        assert row is not None
        assert row['rule_id'] == 1

def test_main_no_match():
    raw_email = b"From: test@example.com\nTo: other@example.com\nSubject: Hello\n\nBody"
    with patch('sys.stdin.buffer.read', return_value=raw_email), \
         patch('sys.stdout.buffer.write') as mock_write, \
         patch('getmail_filter.load_ini_config') as mock_config, \
         patch('getmail_filter.get_rules_from_db', return_value=[]):
        getmail_filter.main()
        mock_write.assert_called_once_with(raw_email)

def test_main_matches_and_spawns():
    raw_email = b"From: test@example.com\nTo: me@example.com\nSubject: Test Subject\nMessage-ID: <abc@test>\n\nBody"
    with patch('sys.stdin.buffer.read', return_value=raw_email), \
         patch('sys.stdout.buffer.write') as mock_write, \
         patch('getmail_filter.load_ini_config') as mock_config, \
         patch('getmail_filter.get_rules_from_db') as mock_rules, \
         patch('getmail_filter.mark_imported') as mock_mark, \
         patch('subprocess.Popen') as mock_popen, \
         patch('time.time', return_value=12345), \
         patch('hashlib.md5') as mock_md5:
        mock_md5.return_value.hexdigest.return_value = 'abcd1234'
        mock_rules.return_value = [
            {'id': 1, 'from_pattern': 'test@example.com', 'to_pattern': 'me@example.com',
             'subject_pattern': 'Test', 'parser_script': 'parser.py'}
        ]
        mock_config.return_value = {'paths': {'email_temp_dir': '/tmp'}}
        getmail_filter.main()
        mock_write.assert_called_once_with(raw_email)
        mock_mark.assert_called()
        mock_popen.assert_called()
