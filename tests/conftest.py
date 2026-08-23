import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import tempfile
import sqlite3
import frontend

@pytest.fixture(scope='function')
def temp_db():
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    # Инициализация схемы (можно вызвать init_database, но чтобы не зависеть, создаём сами)
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            account_number TEXT NOT NULL,
            month TEXT,
            year INTEGER,
            UNIQUE(address, account_number, month, year)
        );
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER,
            service_name TEXT,
            charge REAL,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        );
        CREATE INDEX IF NOT EXISTS idx_accounts_address ON accounts(address);
        CREATE INDEX IF NOT EXISTS idx_accounts_month_year ON accounts(month, year);
    ''')
    conn.commit()
    conn.close()
    yield db_path
    os.close(db_fd)
    os.unlink(db_path)

@pytest.fixture
def app(temp_db):
    frontend.DB_PATH = temp_db
    frontend.app.config['TESTING'] = True
    frontend.app.config['DATABASE'] = temp_db
    return frontend.app

@pytest.fixture
def client(app):
    return app.test_client()

# Моки для внешних API (если нужны)
@pytest.fixture
def mock_ai_response():
    from unittest.mock import patch, MagicMock
    with patch('ai_analyzer.requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Анализ выполнен: отклонений нет."}}]
        }
        mock_post.return_value = mock_response
        yield mock_post

@pytest.fixture
def mock_telegram():
    from unittest.mock import patch, MagicMock
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        yield mock_post