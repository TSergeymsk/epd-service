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
