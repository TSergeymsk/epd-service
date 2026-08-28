"""Фикстуры и общие настройки для тестов."""
import sys
from pathlib import Path
import pytest
import sqlite3
import tempfile
import os

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import get_db_connection, load_ini_config

@pytest.fixture
def temp_db():
    """Создаёт временную БД и возвращает путь и соединение."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # Создаём структуру БД (минимум для тестов)
    conn.executescript('''
        CREATE TABLE accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, account_number TEXT UNIQUE, address TEXT);
        CREATE TABLE periods (id INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER, month INTEGER, start_date TEXT);
        CREATE TABLE services (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, unit TEXT);
        CREATE TABLE charges (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER, period_id INTEGER, service_id INTEGER, quantity REAL, tariff REAL, accrued_by_tariff REAL, benefit REAL, recalculation REAL, amount_due REAL);
        CREATE TABLE account_period_info (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER, period_id INTEGER, total_area REAL, living_area REAL, rooms INTEGER, residents INTEGER, last_payment_date TEXT, meter_readings TEXT);
        CREATE TABLE raw_imports (id INTEGER PRIMARY KEY AUTOINCREMENT, source_type TEXT, source_identifier TEXT, raw_content TEXT, processed_at TIMESTAMP, account_id INTEGER);
        CREATE TABLE llm_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT, period_id INTEGER, model TEXT, provider TEXT, temperature REAL, max_tokens INTEGER, prompt_template TEXT, request_payload TEXT, response_text TEXT, tokens_used INTEGER, status TEXT, attempts INTEGER, last_error TEXT, created_at TIMESTAMP, updated_at TIMESTAMP);
        CREATE TABLE telegram_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT, period_id INTEGER, message_text TEXT, parse_mode TEXT, sent_at TIMESTAMP, status TEXT, attempts INTEGER, last_error TEXT, created_at TIMESTAMP);
        CREATE TABLE filter_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, from_pattern TEXT, to_pattern TEXT, subject_pattern TEXT, parser_script TEXT, enabled BOOLEAN, priority INTEGER, created_at TIMESTAMP);
        CREATE TABLE imported_emails (mail_id TEXT PRIMARY KEY, rule_id INTEGER, imported_at TIMESTAMP, status TEXT, parsed_at TIMESTAMP, error_text TEXT);
        CREATE TABLE address_analysis (id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT, period_id INTEGER, prompt TEXT, response TEXT, model TEXT, tokens_used INTEGER, created_at TIMESTAMP);
    ''')
    conn.commit()
    yield conn, path
    conn.close()
    os.unlink(path)

@pytest.fixture
def mock_config():
    """Возвращает конфиг для тестов (без реальных ключей)."""
    config = {
        'paths': {'db_path': ':memory:', 'email_temp_dir': '/tmp'},
        'openrouter': {'api_key': 'test_key', 'model': 'test-model', 'url': 'https://test.com/v1', 'timeout': '10'},
        'telegram': {'bot_token': 'test_token', 'chat_id': '12345'},
        'logging': {'log_dir': '/tmp/logs'},
        'getmail_filter': {'from_pattern': 'test@example.com', 'to_pattern': 'me@example.com', 'subject_pattern': 'Test'}
    }
    return config
