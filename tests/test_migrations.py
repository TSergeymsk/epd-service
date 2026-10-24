"""Тесты миграций init_db.py."""
import sqlite3
import sys
from pathlib import Path

import pytest


def _make_v2_db(path):
    """Создаёт БД «версии 2»: есть channel/provider, но нет notifications_enqueued."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE llm_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            period_id INTEGER NOT NULL,
            model TEXT NOT NULL,
            provider TEXT,
            status TEXT DEFAULT 'pending',
            response_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE telegram_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            period_id INTEGER NOT NULL,
            message_text TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            channel TEXT DEFAULT 'telegram'
        );
        INSERT INTO llm_requests (address, period_id, model, status, response_text)
            VALUES ('old_a', 1, 'm', 'success', 'text1');
        INSERT INTO llm_requests (address, period_id, model, status, response_text)
            VALUES ('old_b', 2, 'm', 'success', 'text2');
        INSERT INTO llm_requests (address, period_id, model, status)
            VALUES ('new_c', 3, 'm', 'pending');
    """)
    conn.commit()
    conn.execute("PRAGMA user_version = 2")
    conn.commit()
    conn.close()


def _reload_init_db():
    """Перезагружает init_db, чтобы сбросить возможные кэши модуля."""
    if 'init_db' in sys.modules:
        del sys.modules['init_db']
    import init_db
    return init_db


def test_new_db_has_notifications_enqueued_column(db_file, monkeypatch):
    monkeypatch.setenv('EPD_CONFIG', '/nonexistent')  # get_db_path вернёт ':memory:'

    init_db = _reload_init_db()
    # Подменяем get_db_path на файл в tmp_path
    monkeypatch.setattr(init_db, 'get_db_path', lambda: db_file)

    init_db.init_db()

    conn = sqlite3.connect(db_file)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(llm_requests)").fetchall()]
    assert 'notifications_enqueued' in cols
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == init_db.SCHEMA_VERSION
    conn.close()


def test_migration_v2_to_v3_marks_existing_rows(db_file, monkeypatch):
    _make_v2_db(db_file)

    init_db = _reload_init_db()
    monkeypatch.setattr(init_db, 'get_db_path', lambda: db_file)
    monkeypatch.setattr(init_db, 'backup_db', lambda p: p)  # ускоряем

    init_db.init_db()

    conn = sqlite3.connect(db_file)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(llm_requests)").fetchall()]
    assert 'notifications_enqueued' in cols

    # Все существующие записи должны быть помечены как «уже обработанные»
    flags = dict(conn.execute(
        "SELECT address, notifications_enqueued FROM llm_requests"
    ).fetchall())
    assert flags == {'old_a': 1, 'old_b': 1, 'new_c': 1}

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 3
    conn.close()


def test_migration_is_idempotent(db_file, monkeypatch):
    _make_v2_db(db_file)

    init_db = _reload_init_db()
    monkeypatch.setattr(init_db, 'get_db_path', lambda: db_file)
    monkeypatch.setattr(init_db, 'backup_db', lambda p: p)

    # Первый запуск
    init_db.init_db()
    # Второй — не должен падать и не должен дублировать колонку
    init_db.init_db()

    conn = sqlite3.connect(db_file)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(llm_requests)").fetchall()]
    assert cols.count('notifications_enqueued') == 1
    conn.close()


def test_new_db_gets_schema_version(db_file, monkeypatch):
    init_db = _reload_init_db()
    monkeypatch.setattr(init_db, 'get_db_path', lambda: db_file)
    monkeypatch.setattr(init_db, 'backup_db', lambda p: p)

    init_db.init_db()

    conn = sqlite3.connect(db_file)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == init_db.SCHEMA_VERSION
    conn.close()