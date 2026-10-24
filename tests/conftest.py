"""Общие фикстуры для тестов epd-service."""
import configparser
import os
import sqlite3
import sys
from pathlib import Path

import pytest

# Добавляем корень проекта в sys.path, чтобы `import orchestrator` работал
# независимо от того, откуда запущен pytest.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# -------- Схема БД для тестов (минимально достаточная) --------

SCHEMA_SQL = """
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT NOT NULL UNIQUE,
    address TEXT NOT NULL
);
CREATE TABLE periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    UNIQUE(year, month)
);
CREATE TABLE services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    unit TEXT,
    code TEXT
);
CREATE TABLE charges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    quantity REAL,
    tariff REAL,
    accrued_by_tariff REAL,
    benefit REAL DEFAULT 0,
    recalculation REAL DEFAULT 0,
    amount_due REAL,
    UNIQUE(account_id, period_id, service_id)
);
CREATE TABLE llm_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    period_id INTEGER NOT NULL,
    model TEXT NOT NULL,
    provider TEXT,
    temperature REAL DEFAULT 0.7,
    max_tokens INTEGER DEFAULT 2000,
    prompt_template TEXT,
    request_payload TEXT,
    response_text TEXT,
    tokens_used INTEGER,
    status TEXT DEFAULT 'pending',
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    notifications_enqueued INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE telegram_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    period_id INTEGER NOT NULL,
    message_text TEXT NOT NULL,
    parse_mode TEXT DEFAULT 'HTML',
    sent_at TIMESTAMP,
    status TEXT DEFAULT 'pending',
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    channel TEXT DEFAULT 'telegram'
);
"""


@pytest.fixture
def db():
    """In-memory SQLite с минимальной схемой."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    yield conn
    conn.close()


@pytest.fixture
def db_file(tmp_path):
    """Файловая БД (нужна для тестов миграций init_db.py)."""
    path = tmp_path / "test.db"
    return str(path)


@pytest.fixture
def ini_config():
    """ConfigParser со всеми секциями, нужными новой логике.

    По умолчанию оба канала включены — так проще тестировать.
    В конкретных тестах флаги переопределяются через config.set().
    """
    cfg = configparser.ConfigParser()
    cfg.add_section('notifications')
    cfg.set('notifications', 'telegram_enabled', 'true')
    cfg.set('notifications', 'vk_enabled', 'true')

    cfg.add_section('telegram')
    cfg.set('telegram', 'bot_token', 'test_bot_token')
    cfg.set('telegram', 'chat_id', 'test_chat_id')

    cfg.add_section('vk')
    cfg.set('vk', 'access_token', 'test_vk_token')
    cfg.set('vk', 'peer_id', '123456')

    cfg.add_section('gigachat')
    cfg.set('gigachat', 'credentials', 'test_credentials')
    cfg.set('gigachat', 'scope', 'GIGACHAT_API_PERS')
    cfg.set('gigachat', 'base_url', 'https://api.giga.chat/v1')

    cfg.add_section('ssl')
    cfg.set('ssl', 'ca_bundle_path', '')

    cfg.add_section('paths')
    cfg.set('paths', 'db_path', ':memory:')

    return cfg


@pytest.fixture
def mock_logger(mocker):
    """Замоканный logger с методами info/warning/error/exception."""
    logger = mocker.MagicMock()
    return logger


@pytest.fixture
def no_sleep(mocker):
    """Отключает time.sleep в orchestrator, чтобы тесты не тормозили."""
    import orchestrator
    mocker.patch.object(orchestrator.time, 'sleep', return_value=None)
    return True


@pytest.fixture
def fake_gigachat_module(mocker):
    """Подменяет модули gigachat и gigachat.models, чтобы не требовать установки.

    Возвращает (fake_module, fake_models_module, fake_client_class).
    fake_client_class поддерживает контекстный менеджер и метод chat().
    """
    # --- gigachat.models ---
    fake_models = mocker.MagicMock()

    class FakeMessages:
        def __init__(self, role, content):
            self.role = role
            self.content = content

    class FakeChat:
        def __init__(self, messages, temperature, max_tokens):
            self.messages = messages
            self.temperature = temperature
            self.max_tokens = max_tokens

    class FakeMessagesRole:
        USER = "user"
        SYSTEM = "system"
        ASSISTANT = "assistant"

    fake_models.Messages = FakeMessages
    fake_models.Chat = FakeChat
    fake_models.MessagesRole = FakeMessagesRole

    # --- gigachat (корневой модуль) ---
    fake_gigachat = mocker.MagicMock()
    fake_gigachat.models = fake_models

    class FakeGigaChat:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.chat = mocker.MagicMock()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_gigachat.GigaChat = FakeGigaChat

    mocker.patch.dict(sys.modules, {
        'gigachat': fake_gigachat,
        'gigachat.models': fake_models,
    })

    return fake_gigachat, fake_models, FakeGigaChat