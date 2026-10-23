#!/usr/bin/env python3
""" Инициализация и миграция базы данных.
Создаёт недостающие таблицы и индексы, безопасно добавляет новые колонки. """
import sqlite3
import os
import sys
import shutil
import configparser
from datetime import datetime
from pathlib import Path

SCHEMA_VERSION = 2  # 1 - оригинальная схема, 2 - + channel/provider + новые таблицы


def get_config_path():
    env_path = os.environ.get('EPD_CONFIG')
    if env_path:
        return env_path
    return str(Path(__file__).parent / 'config.ini')


def get_db_path():
    config = configparser.ConfigParser()
    config.read(get_config_path())
    if config.has_section('paths') and config.has_option('paths', 'db_path'):
        return config.get('paths', 'db_path')
    return ':memory:'


# -------- Базовые хелперы --------

def table_exists(cursor, name):
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return cursor.fetchone() is not None


def column_exists(cursor, table, column):
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def index_exists(cursor, name):
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (name,),
    )
    return cursor.fetchone() is not None


def get_user_version(cursor):
    cursor.execute("PRAGMA user_version")
    return cursor.fetchone()[0]


def set_user_version(cursor, version):
    # PRAGMA не поддерживает параметры — только литералы, но version у нас int
    cursor.execute(f"PRAGMA user_version = {int(version)}")


def backup_db(db_path):
    """Полноценная копия БД через VACUUM INTO (SQLite >= 3.27)."""
    backup_path = f"{db_path}.bak-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(f"VACUUM INTO '{backup_path}'")
        conn.close()
        print(f"[migrate] Резервная копия: {backup_path}")
        return backup_path
    except sqlite3.OperationalError:
        # fallback для старых SQLite
        shutil.copy2(db_path, backup_path)
        print(f"[migrate] Резервная копия (copy): {backup_path}")
        return backup_path


# -------- Схема (только CREATE IF NOT EXISTS — для новых установок) --------

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
-- channel добавлен сразу в CREATE для новых установок
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
    channel TEXT DEFAULT 'telegram',
    FOREIGN KEY (period_id) REFERENCES periods(id)
);
"""

NEW_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_charges_account_period ON charges(account_id, period_id);
CREATE INDEX IF NOT EXISTS idx_charges_service ON charges(service_id);
CREATE INDEX IF NOT EXISTS idx_account_period_info_account ON account_period_info(account_id);
CREATE INDEX IF NOT EXISTS idx_account_period_info_period ON account_period_info(period_id);
CREATE INDEX IF NOT EXISTS idx_raw_imports_account ON raw_imports(account_id);
CREATE INDEX IF NOT EXISTS idx_raw_imports_date ON raw_imports(processed_at);
CREATE INDEX IF NOT EXISTS idx_llm_requests_address_period ON llm_requests(address, period_id);
CREATE INDEX IF NOT EXISTS idx_llm_requests_status ON llm_requests(status);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_status ON telegram_messages(status);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_period ON telegram_messages(period_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel ON telegram_messages(channel);
CREATE INDEX IF NOT EXISTS idx_imported_emails_status ON imported_emails(status);
"""


# -------- Собственно миграция --------

def migrate_schema(cursor):
    """Идемпотентно приводит схему к SCHEMA_VERSION."""
    current = get_user_version(cursor)
    print(f"[migrate] Текущая версия схемы: {current}, целевая: {SCHEMA_VERSION}")
    if current >= SCHEMA_VERSION:
        print("[migrate] Схема уже актуальна.")
        return

    # --- v1 -> v2 ---
    if current < 2:
        # 1) telegram_messages.channel
        if table_exists(cursor, 'telegram_messages'):
            if not column_exists(cursor, 'telegram_messages', 'channel'):
                cursor.execute(
                    "ALTER TABLE telegram_messages "
                    "ADD COLUMN channel TEXT DEFAULT 'telegram'"
                )
                print("[migrate] + telegram_messages.channel")
            else:
                print("[migrate] = telegram_messages.channel уже есть")
        else:
            print("[migrate] ! telegram_messages отсутствует — будет создана ниже")

        # 2) llm_requests.provider
        if table_exists(cursor, 'llm_requests'):
            if not column_exists(cursor, 'llm_requests', 'provider'):
                cursor.execute(
                    "ALTER TABLE llm_requests ADD COLUMN provider TEXT"
                )
                print("[migrate] + llm_requests.provider")
            else:
                print("[migrate] = llm_requests.provider уже есть")
        else:
            print("[migrate] ! llm_requests отсутствует — будет создана ниже")

        set_user_version(cursor, 2)

    print("[migrate] Готово.")


def init_db():
    db_path = get_db_path()
    is_new = not os.path.exists(db_path)

    if not is_new:
        backup_db(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA busy_timeout = 5000;")
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Миграция существующей схемы (до CREATE — чтобы ALTER применился к старым таблицам)
    if not is_new:
        migrate_schema(cursor)

    # 2. Создание недостающих таблиц (для новых установок и на всякий случай)
    cursor.executescript(BASE_TABLES)
    cursor.executescript(NEW_TABLES)

    # 3. Индексы
    cursor.executescript(NEW_INDEXES)

    # 4. Если БД была новой — сразу помечаем как актуальную версию
    if is_new:
        set_user_version(cursor, SCHEMA_VERSION)

    conn.commit()
    conn.close()
    print(f"База данных готова: {db_path}")


def check_db(db_path=None):
    if db_path is None:
        db_path = get_db_path()
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'"
        )
        result = cur.fetchone()
        conn.close()
        return result is not None
    except sqlite3.Error:
        return False


if __name__ == "__main__":
    init_db()