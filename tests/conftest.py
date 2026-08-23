import pytest
import tempfile
import os
import sqlite3
from unittest.mock import patch, MagicMock
from flask import Flask

# Импортируем модули приложения (предполагаем, что они доступны)
from email_parser import parse_email, save_charges_to_db
from ai_analyzer import analyze_address_month
from frontend import app as flask_app
from telegram_notifier import send_telegram_message

# Фикстура временной БД
@pytest.fixture
def temp_db():
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    # Создаём схему (скопируйте из init_db.py или вынесите в отдельную функцию)
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
        -- Добавьте остальные таблицы, если есть
    ''')
    conn.commit()
    conn.close()
    yield db_path
    os.close(db_fd)
    os.unlink(db_path)

# Фикстура приложения Flask для тестирования
@pytest.fixture
def client():
    flask_app.config['TESTING'] = True
    flask_app.config['DATABASE'] = temp_db  # подмена пути БД
    with flask_app.test_client() as client:
        yield client

# Мок для AI API
@pytest.fixture
def mock_ai_response():
    with patch('ai_analyzer.requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Анализ выполнен: отклонений нет."}}]
        }
        mock_post.return_value = mock_response
        yield mock_post

# Мок для Telegram
@pytest.fixture
def mock_telegram():
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        yield mock_post