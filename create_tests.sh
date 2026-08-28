#!/bin/bash
# create_tests.sh – создаёт структуру tests/ со всеми тестами и CI/CD

set -e

# Создаём каталог tests
mkdir -p tests

# Создаём каталог для GitHub Actions
mkdir -p .github/workflows

cat > tests/conftest.py << 'EOF'
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
EOF

cat > tests/test_utils.py << 'EOF'
"""Тесты для модуля utils."""
import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open
from utils import load_ini_config, load_ai_config, get_db_path, get_db_connection, setup_logging

def test_load_ini_config_success(tmp_path):
    config_content = """
[paths]
db_path = test.db
email_temp_dir = /tmp

[openrouter]
api_key = key123
    """
    config_file = tmp_path / "config.ini"
    config_file.write_text(config_content)
    with patch('utils.get_config_path', return_value=str(config_file)):
        config = load_ini_config()
        assert config.get('paths', 'db_path') == 'test.db'
        assert config.get('openrouter', 'api_key') == 'key123'

def test_load_ini_config_fallback():
    with patch('utils.get_config_path', return_value='/nonexistent.ini'):
        config = load_ini_config()
        assert config is not None
        assert config.has_section('paths') is False  # или пустой

def test_load_ai_config_missing(tmp_path):
    with patch('utils.get_project_root', return_value=tmp_path):
        ai_config = load_ai_config()
        assert ai_config == {}  # если файла нет, возвращаем пустой словарь

def test_get_db_path():
    with patch('utils.load_ini_config') as mock_load:
        mock_load.return_value = {'paths': {'db_path': '/test/db.sqlite'}}
        assert get_db_path() == '/test/db.sqlite'

def test_get_db_connection():
    with patch('utils.get_db_path', return_value=':memory:'):
        conn = get_db_connection()
        assert conn is not None
        conn.close()

def test_setup_logging(tmp_path):
    log_dir = tmp_path / 'logs'
    logger = setup_logging('test_logger', str(log_dir))
    assert logger.name == 'test_logger'
    assert log_dir.exists()
EOF

cat > tests/test_parsers.py << 'EOF'
"""Тесты для парсеров (base_parser и mos_parser)."""
import pytest
import os
import email
from email.policy import default
from pathlib import Path
from unittest.mock import patch, MagicMock
from parsers.base_parser import (
    normalize_service_name,
    get_or_create_account,
    get_or_create_period,
    get_or_create_service,
    save_charges
)
from parsers.mos_parser import parse_html_email, parse_text_epd, process_email_file

def test_normalize_service_name():
    assert normalize_service_name('ХВС') == 'ХВС'
    assert normalize_service_name('ГВС КПУ') == 'ГВС'
    assert normalize_service_name('Водоотведение') == 'Водоотведение'
    assert normalize_service_name('Отопление') == 'Отопление'
    assert normalize_service_name('Сод.жил.') == 'Содержание жилья'
    assert normalize_service_name('Капитальный ремонт') == 'Капитальный ремонт'
    assert normalize_service_name('ТКО') == 'ТКО'
    assert normalize_service_name('Запирающее устройство') == 'Запирающее устройство'
    assert normalize_service_name('шлагбаум') == 'Шлагбаум'
    assert normalize_service_name('Неизвестная услуга') == 'Неизвестная услуга'

def test_get_or_create_account(temp_db):
    conn, path = temp_db
    account_id = get_or_create_account(conn, '12345', 'Test Address')
    assert account_id == 1
    # Повторный вызов возвращает тот же id
    account_id2 = get_or_create_account(conn, '12345', 'Test Address')
    assert account_id2 == 1
    # С другим номером создаёт новый
    account_id3 = get_or_create_account(conn, '67890', 'Another')
    assert account_id3 == 2

def test_get_or_create_period(temp_db):
    conn, path = temp_db
    period_id = get_or_create_period(conn, 2024, 1)
    assert period_id == 1
    period_id2 = get_or_create_period(conn, 2024, 1)
    assert period_id2 == 1
    period_id3 = get_or_create_period(conn, 2024, 2)
    assert period_id3 == 2

def test_get_or_create_service(temp_db):
    conn, path = temp_db
    service_id = get_or_create_service(conn, 'Тестовая услуга', 'ед')
    assert service_id == 1
    service_id2 = get_or_create_service(conn, 'Тестовая услуга')
    assert service_id2 == 1
    service_id3 = get_or_create_service(conn, 'Другая')
    assert service_id3 == 2

def test_save_charges(temp_db):
    conn, path = temp_db
    account_id = get_or_create_account(conn, '123', 'addr')
    period_id = get_or_create_period(conn, 2024, 1)
    services = [
        {'name': 'ХВС', 'unit': 'м3', 'quantity': 10.0, 'tariff': 50.0, 'amount_due': 500.0, 'benefit': 0, 'recalculation': 0}
    ]
    save_charges(conn, account_id, period_id, services)
    cur = conn.execute("SELECT * FROM charges WHERE account_id = ? AND period_id = ?", (account_id, period_id))
    row = cur.fetchone()
    assert row['amount_due'] == 500.0
    # service_id создан автоматически
    service_id = row['service_id']
    cur2 = conn.execute("SELECT name FROM services WHERE id = ?", (service_id,))
    assert cur2.fetchone()['name'] == 'ХВС'

def test_parse_html_email_empty():
    # Если HTML нет или не распознан, возвращаем None
    result = parse_html_email('<html><body>Нет таблицы</body></html>')
    assert result is None

def test_parse_text_epd_basic():
    text = """
ФЛС №12345
АДРЕС: ул. Тестовая, д.1 кв.2
ПЕРИОД: 1 месяц 2024 год
Итого к оплате: 1500.50 руб.
ХВС 10.0 500.00
ГВС 5.0 600.00
"""
    data = parse_text_epd(text)
    assert data['account_number'] == '12345'
    assert data['address'] == 'ул. Тестовая, д.1 кв.2'
    assert data['year'] == 2024
    assert data['month'] == 1
    assert data['total_due'] == 1500.50
    assert len(data['services']) >= 2
    # Проверяем, что услуги нормализованы
    service_names = [s['name'] for s in data['services']]
    assert 'ХВС' in service_names
    assert 'ГВС' in service_names

@patch('parsers.mos_parser.get_db_connection')
def test_process_email_file(mock_db_conn, tmp_path):
    # Создаём временный email файл
    email_content = """From: test@example.com
To: me@example.com
Subject: Test

ФЛС №12345
АДРЕС: ул. Тестовая, д.1 кв.2
ПЕРИОД: 1 месяц 2024 год
Итого к оплате: 1500.50 руб.
ХВС 10.0 500.00
ГВС 5.0 600.00
"""
    email_file = tmp_path / "test.eml"
    email_file.write_text(email_content)
    mock_conn = MagicMock()
    mock_db_conn.return_value = mock_conn
    # Мокаем get_or_create_account и т.д.
    with patch('parsers.mos_parser.get_or_create_account', return_value=1), \
         patch('parsers.mos_parser.get_or_create_period', return_value=1), \
         patch('parsers.mos_parser.save_charges') as mock_save:
        success, err = process_email_file(str(email_file), 'test_mail_id')
        assert success is True
        mock_save.assert_called()
EOF

cat > tests/test_orchestrator.py << 'EOF'
"""Тесты для orchestrator (AI-запросы и генерация сообщений)."""
import pytest
from unittest.mock import patch, MagicMock
import json
import time
from orchestrator import (
    clean_markdown,
    is_ai_configured,
    get_aggregated_data,
    format_prompt,
    call_ai_api,
    process_llm_requests,
    generate_telegram_message_for_period,
    process_telegram_messages,
    create_telegram_messages_for_successful_llm
)

def test_clean_markdown():
    text = "**жирный** *курсив* _подчеркнутый_ ~~зачеркнутый~~ ### Заголовок `код` [ссылка](url) ---"
    cleaned = clean_markdown(text)
    assert "жирный" in cleaned
    assert "курсив" in cleaned
    assert "подчеркнутый" in cleaned
    assert "зачеркнутый" in cleaned
    assert "Заголовок" in cleaned
    assert "код" in cleaned
    assert "ссылка" in cleaned
    assert "---" not in cleaned

def test_is_ai_configured_with_config(mock_config):
    with patch('orchestrator.INI_CONFIG', mock_config), \
         patch('orchestrator.AI_CONFIG', {'ai': {'url': 'http://test', 'model': 'test'}}):
        assert is_ai_configured() is True

def test_is_ai_configured_missing_key():
    with patch('orchestrator.INI_CONFIG', {'openrouter': {'api_key': ''}}):
        assert is_ai_configured() is False

def test_get_aggregated_data_no_accounts(temp_db):
    conn, path = temp_db
    address = "No such address"
    data, prev, prev_ym, last_12 = get_aggregated_data(conn, address, 1)
    assert data == []
    assert prev is None
    assert prev_ym is None
    assert last_12 == {}

def test_format_prompt_without_template():
    with patch('orchestrator.AI_CONFIG', {'prompts': {}}):
        result = format_prompt("addr", "2024-01", [], None, None, {})
        assert result is None  # должен вернуть None

def test_call_ai_api_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'choices': [{'message': {'content': 'Test response'}}],
        'usage': {'total_tokens': 100}
    }
    with patch('requests.post', return_value=mock_response), \
         patch('orchestrator.AI_CONFIG', {'ai': {'url': 'http://test', 'model': 'test', 'timeout': 10, 'temperature': 0.7, 'max_tokens': 100}}), \
         patch('orchestrator.INI_CONFIG', {'openrouter': {'api_key': 'key'}}):
        answer, tokens = call_ai_api("prompt")
        assert answer == 'Test response'
        assert tokens == 100

def test_call_ai_api_429_retry():
    # Первый запрос 429, второй успешный
    response_429 = MagicMock()
    response_429.status_code = 429
    response_429.headers = {'Retry-After': '1'}
    response_ok = MagicMock()
    response_ok.status_code = 200
    response_ok.json.return_value = {'choices': [{'message': {'content': 'OK'}}], 'usage': {'total_tokens': 50}}
    with patch('requests.post', side_effect=[response_429, response_ok]), \
         patch('time.sleep', return_value=None), \
         patch('orchestrator.AI_CONFIG', {'ai': {'url': 'http://test', 'model': 'test', 'timeout': 10, 'temperature': 0.7, 'max_tokens': 100}}), \
         patch('orchestrator.INI_CONFIG', {'openrouter': {'api_key': 'key'}}):
        answer, tokens = call_ai_api("prompt", retries=3)
        assert answer == 'OK'
        assert tokens == 50

def test_generate_telegram_message_for_period_no_ai(temp_db):
    conn, path = temp_db
    # Создаём тестовые данные: account, period, charges
    cur = conn.cursor()
    cur.execute("INSERT INTO accounts (id, account_number, address) VALUES (1, '123', 'Test addr')")
    cur.execute("INSERT INTO periods (id, year, month, start_date) VALUES (1, 2024, 1, '2024-01-01')")
    cur.execute("INSERT INTO services (id, name) VALUES (1, 'Test service')")
    cur.execute("INSERT INTO charges (account_id, period_id, service_id, quantity, amount_due) VALUES (1, 1, 1, 10, 500)")
    conn.commit()
    # Нет LLM-запроса
    with patch('orchestrator.is_ai_configured', return_value=True):
        msg = generate_telegram_message_for_period(conn, "Test addr", 1)
        assert "🏠 Анализ ЕПД" in msg
        assert "Итого: 500.00 руб." in msg
        assert "AI-анализ не выполнен" in msg  # потому что нет успешного LLM

def test_process_telegram_messages_no_messages():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    with patch('orchestrator.get_pending_telegram_messages', return_value=[]):
        process_telegram_messages(conn)
        # ничего не происходит
        conn.execute.assert_not_called()
EOF

cat > tests/test_frontend.py << 'EOF'
"""Тесты для frontend API."""
import pytest
import json
from flask import Flask
from frontend import app
from unittest.mock import patch

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_index(client):
    rv = client.get('/')
    assert rv.status_code == 200
    assert b'Анализ ЕПД' in rv.data

def test_api_addresses_empty(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = []
        rv = client.get('/api/addresses')
        assert rv.status_code == 200
        assert rv.json == []

def test_api_accounts_by_address(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = [
            {'id': 1, 'account_number': '123'}
        ]
        rv = client.get('/api/accounts_by_address?address=test')
        assert rv.status_code == 200
        assert rv.json == [{'id': 1, 'account_number': '123'}]

def test_api_services(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = [
            {'id': 1, 'name': 'Service1', 'unit': 'ед'}
        ]
        rv = client.get('/api/services')
        assert rv.status_code == 200
        assert rv.json == [{'id': 1, 'name': 'Service1', 'unit': 'ед'}]

def test_api_periods(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchall.return_value = [
            {'year': 2024, 'month': 1}
        ]
        rv = client.get('/api/periods?account_ids=1')
        assert rv.status_code == 200
        assert rv.json == [{'year': 2024, 'month': 1}]

def test_api_data_missing_params(client):
    rv = client.get('/api/data')
    assert rv.status_code == 400
    assert 'Missing parameters' in rv.json['error']

def test_api_analysis_for_month_not_found(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        mock_conn.execute.return_value.fetchone.return_value = None
        rv = client.get('/api/analysis_for_month?address=test&year=2024&month=1')
        assert rv.status_code == 404
        assert rv.json['error'] == 'No analysis found'

def test_api_llm_details_not_found(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        # period_id не найден
        mock_conn.execute.return_value.fetchone.return_value = None
        rv = client.get('/api/llm_details?address=test&year=2024&month=1')
        assert rv.status_code == 404
        assert rv.json['error'] == 'Period not found'

def test_api_retry_ai(client):
    with patch('frontend.get_db') as mock_db:
        mock_conn = mock_db.return_value
        # Имитируем существующий период и запись
        mock_conn.execute.return_value.fetchone.side_effect = [
            {'id': 1},  # период
            {'id': 10}  # существующий llm_requests
        ]
        rv = client.post('/api/retry_ai', json={'address': 'test', 'year': 2024, 'month': 1})
        assert rv.status_code == 200
        assert rv.json['status'] == 'ok'
EOF

cat > tests/test_getmail_filter.py << 'EOF'
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
EOF

cat > tests/test_db_init.py << 'EOF'
"""Тесты для инициализации БД и миграций."""
import pytest
import sqlite3
import os
from init_db import check_db, init_db, check_and_migrate
from pathlib import Path

def test_check_db_nonexistent(tmp_path):
    db_path = tmp_path / 'nonexistent.db'
    assert check_db(db_path) is False

def test_init_db_creates_tables(tmp_path):
    db_path = str(tmp_path / 'test.db')
    with patch('init_db.DB_PATH', db_path), \
         patch('init_db.get_config_path', return_value=str(tmp_path / 'config.ini')):
        # Принудительно удаляем, если есть
        if os.path.exists(db_path):
            os.remove(db_path)
        init_db()
        assert os.path.exists(db_path)
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
        assert cur.fetchone() is not None
        # Проверяем наличие новых таблиц (filter_rules, llm_requests, etc.)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='filter_rules'")
        assert cur.fetchone() is not None
        conn.close()

def test_check_and_migrate_existing_db(tmp_path):
    db_path = str(tmp_path / 'existing.db')
    # Создаём старую БД без новых таблиц
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY)")
    conn.close()
    with patch('init_db.DB_PATH', db_path), \
         patch('init_db.get_config_path', return_value=str(tmp_path / 'config.ini')):
        check_and_migrate()
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='filter_rules'")
        assert cur.fetchone() is not None
        conn.close()
EOF

cat > tests/pytest.ini << 'EOF'
[pytest]
pythonpath = . ..
testpaths = tests
addopts = -v --tb=short
EOF

cat > requirements-test.txt << 'EOF'
pytest>=7.0
pytest-cov
requests-mock
Flask
beautifulsoup4
EOF

cat > .github/workflows/tests.yml << 'EOF'
name: Run tests

on:
  push:
    branches: [ main, master ]
  pull_request:
    branches: [ main, master ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.9", "3.10", "3.11"]
    steps:
    - uses: actions/checkout@v4
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v5
      with:
        python-version: ${{ matrix.python-version }}
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements-test.txt
        pip install -e .
    - name: Run tests with coverage
      run: |
        pytest --cov=. --cov-report=xml --cov-report=term
    - name: Upload coverage to Codecov (optional)
      uses: codecov/codecov-action@v4
      with:
        file: ./coverage.xml
        fail_ci_if_error: false
EOF

echo "✅ Все тесты и CI/CD файлы созданы."
echo "📁 Структура создана:"
echo "  - tests/        - все тесты и конфигурация pytest"
echo "  - .github/workflows/tests.yml - GitHub Actions workflow"
echo ""
echo "Для запуска тестов локально:"
echo "  pip install -r requirements-test.txt"
echo "  pytest"
echo ""
echo "Для запуска с покрытием:"
echo "  pytest --cov=. --cov-report=html"
