#!/usr/bin/env python3
"""
Инициализация и миграция базы данных.
Создаёт недостающие таблицы и индексы, не удаляя существующие данные.
"""
import sqlite3
import os
import sys
import configparser
from pathlib import Path

def get_config_path():
    env_path = os.environ.get('EPD_CONFIG')
    if env_path:
        return env_path
    return str(Path(__file__).parent / 'config.ini')

def get_db_path():
    """Возвращает путь к БД из конфига, или ':memory:' если секция отсутствует."""
    config = configparser.ConfigParser()
    config.read(get_config_path())
    if config.has_section('paths') and config.has_option('paths', 'db_path'):
        return config.get('paths', 'db_path')
    # fallback для тестов
    return ':memory:'

# Схема с новыми таблицами (добавляем только отсутствующие)
NEW_TABLES = """
CREATE TABLE IF NOT EXISTS filter_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    from_pattern TEXT NOT NULL,
    to_pattern TEXT NOT NULL,
    subject_pattern TEXT NOT NULL,
    parser_script TEXT NOT NULL,
    enabled BOOLEAN DEFAULT 1,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS imported_emails (
    mail_id TEXT PRIMARY KEY,
    rule_id INTEGER,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'imported',
    parsed_at TIMESTAMP,
    error_text TEXT,
    FOREIGN KEY (rule_id) REFERENCES filter_rules(id)
);

CREATE TABLE IF NOT EXISTS llm_requests (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (period_id) REFERENCES periods(id)
);

CREATE TABLE IF NOT EXISTS telegram_messages (
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
    FOREIGN KEY (period_id) REFERENCES periods(id)
);
"""

NEW_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_llm_requests_address_period ON llm_requests(address, period_id);
CREATE INDEX IF NOT EXISTS idx_llm_requests_status ON llm_requests(status);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_status ON telegram_messages(status);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_period ON telegram_messages(period_id);
CREATE INDEX IF NOT EXISTS idx_imported_emails_status ON imported_emails(status);
"""

BASE_TABLES = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT NOT NULL UNIQUE,
    address TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    start_date TEXT NOT NULL,
    UNIQUE(year, month)
);

CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    unit TEXT,
    code TEXT
);

CREATE TABLE IF NOT EXISTS charges (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (period_id) REFERENCES periods(id),
    FOREIGN KEY (service_id) REFERENCES services(id),
    UNIQUE(account_id, period_id, service_id)
);

CREATE TABLE IF NOT EXISTS account_period_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,
    total_area REAL,
    living_area REAL,
    rooms INTEGER,
    residents INTEGER,
    last_payment_date TEXT,
    meter_readings TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (period_id) REFERENCES periods(id),
    UNIQUE(account_id, period_id)
);

CREATE TABLE IF NOT EXISTS raw_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_identifier TEXT,
    raw_content TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    account_id INTEGER,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);
"""

def check_db(db_path=None):
    """Проверяет существование БД и наличие таблицы accounts."""
    if db_path is None:
        db_path = get_db_path()
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts';")
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except sqlite3.Error:
        return False

def init_db():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA busy_timeout = 5000;")
    
    cursor.executescript(BASE_TABLES)
    cursor.executescript(NEW_TABLES)
    cursor.executescript(NEW_INDEXES)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_charges_account_period ON charges(account_id, period_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_charges_service ON charges(service_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_period_info_account ON account_period_info(account_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_period_info_period ON account_period_info(period_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_imports_account ON raw_imports(account_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_imports_date ON raw_imports(processed_at);")
    
    conn.commit()
    conn.close()
    print(f"База данных инициализирована/обновлена: {db_path}")
    print("Все таблицы и индексы созданы (если отсутствовали).")

def check_and_migrate():
    db_path = get_db_path()
    if not os.path.exists(db_path):
        print("База данных не существует. Будет создана новая.")
        init_db()
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='filter_rules';")
    if not cursor.fetchone():
        print("Обнаружена старая БД. Выполняется миграция (добавление новых таблиц).")
        cursor.executescript(NEW_TABLES)
        cursor.executescript(NEW_INDEXES)
        conn.commit()
        print("Миграция выполнена.")
    conn.close()

if __name__ == "__main__":
    check_and_migrate()