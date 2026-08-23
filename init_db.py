#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
DB_PATH = config.get('database', 'db_path', fallback='epd.db')

def init_database(db_path=None):
    """Инициализирует структуру БД."""
    if db_path is None:
        db_path = DB_PATH
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Таблица счетов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            account_number TEXT NOT NULL,
            month TEXT,
            year INTEGER,
            UNIQUE(address, account_number, month, year)
        )
    ''')
    
    # Таблица начислений
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER,
            service_name TEXT,
            charge REAL,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        )
    ''')
    
    # Индексы
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_accounts_address ON accounts(address)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_accounts_month_year ON accounts(month, year)')
    
    conn.commit()
    conn.close()
    print(f"База данных {db_path} инициализирована")

if __name__ == "__main__":
    init_database()