"""Тесты для frontend.py (Flask)."""
import sqlite3

import pytest


@pytest.fixture
def client(db_file, monkeypatch):
    """Создаёт Flask test_client с временной БД и полной схемой."""
    # Готовим БД: используем init_db для полной актуальной схемы
    monkeypatch.setenv('EPD_CONFIG', '/nonexistent')

    if 'init_db' in __import__('sys').modules:
        del __import__('sys').modules['init_db']
    import init_db
    monkeypatch.setattr(init_db, 'get_db_path', lambda: db_file)
    monkeypatch.setattr(init_db, 'backup_db', lambda p: p)
    init_db.init_db()

    # Настраиваем конфиг
    import configparser
    cfg = configparser.ConfigParser()
    cfg.add_section('paths'); cfg.set('paths', 'db_path', db_file)
    cfg.add_section('frontend'); cfg.set('frontend', 'port', '5000')
    cfg.add_section('notifications')
    cfg.set('notifications', 'telegram_enabled', 'true')
    cfg.set('notifications', 'vk_enabled', 'true')
    cfg.add_section('telegram'); cfg.set('telegram', 'bot_token', 'x'); cfg.set('telegram', 'chat_id', 'y')
    cfg.add_section('vk'); cfg.set('vk', 'access_token', 'x'); cfg.set('vk', 'peer_id', '1')
    cfg.add_section('gigachat'); cfg.set('gigachat', 'credentials', 'x')
    cfg.add_section('ssl'); cfg.set('ssl', 'ca_bundle_path', '')
    cfg.add_section('logging'); cfg.set('logging', 'log_dir', '/tmp')

    if 'frontend' in __import__('sys').modules:
        del __import__('sys').modules['frontend']
    import frontend
    monkeypatch.setattr(frontend, 'config', cfg)
    monkeypatch.setattr(frontend, 'DB_PATH', db_file)

    frontend.app.config['TESTING'] = True
    with frontend.app.test_client() as c:
        yield c, db_file


def _insert_period_and_success(db_file, year=2025, month=6):
    conn = sqlite3.connect(db_file)
    conn.execute(
        "INSERT INTO periods (year, month, start_date) VALUES (?, ?, '2025-06-01')",
        (year, month),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute("""
        INSERT INTO llm_requests (address, period_id, model, status, response_text,
                                  notifications_enqueued)
        VALUES ('addr1', ?, 'm', 'success', 'analysis', 1)
    """, (pid,))
    conn.commit()
    conn.close()
    return pid


def test_retry_telegram_creates_both_channels(client):
    c, db_file = client
    _insert_period_and_success(db_file)

    resp = c.post('/api/retry_telegram', json={
        'address': 'addr1', 'year': 2025, 'month': 6
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['status'] == 'ok'
    assert data['channels'] == {'telegram': 'created', 'vk': 'created'}

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT channel, status FROM telegram_messages ORDER BY channel"
    ).fetchall()
    assert len(rows) == 2
    assert {r['channel'] for r in rows} == {'telegram', 'vk'}
    assert all(r['status'] == 'pending' for r in rows)
    conn.close()


def test_retry_telegram_resets_existing(client):
    c, db_file = client
    _insert_period_and_success(db_file)

    # Первый вызов — создаёт
    c.post('/api/retry_telegram', json={'address': 'addr1', 'year': 2025, 'month': 6})

    # Помечаем как отправленные
    conn = sqlite3.connect(db_file)
    conn.execute("UPDATE telegram_messages SET status='sent', attempts=5")
    conn.commit()
    conn.close()

    # Второй вызов — должен сбросить
    resp = c.post('/api/retry_telegram', json={'address': 'addr1', 'year': 2025, 'month': 6})
    data = resp.get_json()
    assert data['channels'] == {'telegram': 'reset', 'vk': 'reset'}

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT status, attempts FROM telegram_messages").fetchall()
    for r in rows:
        assert r['status'] == 'pending'
        assert r['attempts'] == 0
    conn.close()


def test_retry_telegram_requires_successful_llm(client):
    c, db_file = client
    # period без llm_requests
    conn = sqlite3.connect(db_file)
    conn.execute("INSERT INTO periods (year, month, start_date) VALUES (2025, 6, '2025-06-01')")
    conn.commit()
    conn.close()

    resp = c.post('/api/retry_telegram', json={'address': 'addr1', 'year': 2025, 'month': 6})
    assert resp.status_code == 400
    assert 'No successful LLM' in resp.get_json()['error']


def test_retry_telegram_missing_params(client):
    c, _ = client
    resp = c.post('/api/retry_telegram', json={'address': 'addr1'})
    assert resp.status_code == 400


def test_queue_status_separates_channels(client):
    c, db_file = client
    conn = sqlite3.connect(db_file)
    conn.execute("""
        INSERT INTO telegram_messages (address, period_id, message_text, status, channel)
        VALUES ('a', 1, 'x', 'pending', 'telegram'),
               ('a', 1, 'x', 'pending', 'telegram'),
               ('a', 1, 'x', 'pending', 'vk'),
               ('a', 1, 'x', 'sent', 'vk')
    """)
    conn.commit()
    conn.close()

    resp = c.get('/api/queue_status')
    data = resp.get_json()
    assert data['tg_pending'] == 2
    assert data['vk_pending'] == 1
    assert data['notifications_pending'] == 3