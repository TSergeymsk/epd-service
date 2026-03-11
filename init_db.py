#!/usr/bin/env python3
"""
Инициализация базы данных для системы учёта платежей ЖКХ (ЕПД).
Создаёт таблицы, индексы и включает режим WAL.
"""
import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "epd.db")

# Определение схемы базы данных (SQL)
SCHEMA = """
-- Таблица лицевых счетов
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT NOT NULL UNIQUE,
    address TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица периодов (месяц/год)
CREATE TABLE IF NOT EXISTS periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    start_date TEXT NOT NULL,
    UNIQUE(year, month)
);

-- Справочник услуг
CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    unit TEXT,
    code TEXT
);

-- Основные начисления
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

-- Дополнительная информация по счёту за период
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

-- Сырые данные импорта
CREATE TABLE IF NOT EXISTS raw_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_identifier TEXT,
    raw_content TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    account_id INTEGER,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

-- Анализ ИИ по адресам (новая таблица)
CREATE TABLE IF NOT EXISTS address_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    period_id INTEGER NOT NULL,
    prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    model TEXT NOT NULL,
    tokens_used INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (period_id) REFERENCES periods(id),
    UNIQUE(address, period_id)
);
"""

# Индексы для ускорения запросов
INDEXES = """
CREATE INDEX IF NOT EXISTS idx_charges_account_period ON charges(account_id, period_id);
CREATE INDEX IF NOT EXISTS idx_charges_service ON charges(service_id);
CREATE INDEX IF NOT EXISTS idx_account_period_info_account ON account_period_info(account_id);
CREATE INDEX IF NOT EXISTS idx_account_period_info_period ON account_period_info(period_id);
CREATE INDEX IF NOT EXISTS idx_raw_imports_account ON raw_imports(account_id);
CREATE INDEX IF NOT EXISTS idx_raw_imports_date ON raw_imports(processed_at);
CREATE INDEX IF NOT EXISTS idx_address_analysis_address ON address_analysis(address);
CREATE INDEX IF NOT EXISTS idx_address_analysis_period ON address_analysis(period_id);
"""

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA busy_timeout = 5000;")
    cursor.executescript(SCHEMA)
    cursor.executescript(INDEXES)
    conn.commit()
    conn.close()
    print(f"База данных успешно инициализирована: {DB_PATH}")
    print("Режим WAL включён, таймаут ожидания установлен в 5000 мс.")

def check_db():
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts';")
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except sqlite3.Error:
        return False

if __name__ == "__main__":
    if check_db():
        print(f"База данных {DB_PATH} уже существует и содержит таблицы.")
        response = input("Пересоздать базу? Все данные будут потеряны! (y/N): ").strip().lower()
        if response != 'y':
            print("Операция отменена.")
            sys.exit(0)
        os.remove(DB_PATH)
        print("Старая база удалена.")
    init_db()
