"""Тесты для notifications.enqueue_notifications."""
import pytest

from notifications import (
    enqueue_notifications,
    get_enabled_channels,
    MESSENGER_CHANNELS,
)


# -------- get_enabled_channels --------

def test_get_enabled_channels_both(ini_config):
    assert get_enabled_channels(ini_config) == ['telegram', 'vk']


def test_get_enabled_channels_only_telegram(ini_config):
    ini_config.set('notifications', 'vk_enabled', 'false')
    assert get_enabled_channels(ini_config) == ['telegram']


def test_get_enabled_channels_only_vk(ini_config):
    ini_config.set('notifications', 'telegram_enabled', 'false')
    assert get_enabled_channels(ini_config) == ['vk']


def test_get_enabled_channels_none(ini_config):
    ini_config.set('notifications', 'telegram_enabled', 'false')
    ini_config.set('notifications', 'vk_enabled', 'false')
    assert get_enabled_channels(ini_config) == []


def test_get_enabled_channels_missing_section():
    """Без секции [notifications] все каналы считаются выключенными."""
    import configparser
    cfg = configparser.ConfigParser()
    assert get_enabled_channels(cfg) == []


# -------- enqueue_notifications: создание --------

def test_enqueue_both_channels_creates_two_rows(db, ini_config, mock_logger):
    result = enqueue_notifications(
        db, address='addr1', period_id=1,
        message_text='test message',
        ini_config=ini_config, logger=mock_logger,
    )
    assert result == {'telegram': 'created', 'vk': 'created'}

    rows = db.execute(
        "SELECT channel, status, message_text FROM telegram_messages "
        "WHERE address='addr1' AND period_id=1 ORDER BY channel"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]['channel'] == 'telegram'
    assert rows[1]['channel'] == 'vk'
    assert all(r['status'] == 'pending' for r in rows)
    assert all(r['message_text'] == 'test message' for r in rows)


def test_enqueue_only_telegram_enabled(db, ini_config, mock_logger):
    ini_config.set('notifications', 'vk_enabled', 'false')
    result = enqueue_notifications(
        db, address='a', period_id=1, message_text='m',
        ini_config=ini_config, logger=mock_logger,
    )
    assert result == {'telegram': 'created'}
    channels = {r['channel'] for r in db.execute(
        "SELECT channel FROM telegram_messages"
    ).fetchall()}
    assert channels == {'telegram'}


def test_enqueue_only_vk_enabled(db, ini_config, mock_logger):
    ini_config.set('notifications', 'telegram_enabled', 'false')
    result = enqueue_notifications(
        db, address='a', period_id=1, message_text='m',
        ini_config=ini_config, logger=mock_logger,
    )
    assert result == {'vk': 'created'}
    channels = {r['channel'] for r in db.execute(
        "SELECT channel FROM telegram_messages"
    ).fetchall()}
    assert channels == {'vk'}


def test_enqueue_no_channels_enabled_returns_empty(db, ini_config, mock_logger):
    ini_config.set('notifications', 'telegram_enabled', 'false')
    ini_config.set('notifications', 'vk_enabled', 'false')
    result = enqueue_notifications(
        db, address='a', period_id=1, message_text='m',
        ini_config=ini_config, logger=mock_logger,
    )
    assert result == {}
    count = db.execute("SELECT COUNT(*) FROM telegram_messages").fetchone()[0]
    assert count == 0


def test_enqueue_message_text_none_writes_empty_string(db, ini_config, mock_logger):
    enqueue_notifications(
        db, address='a', period_id=1, message_text=None,
        ini_config=ini_config, logger=mock_logger,
    )
    rows = db.execute("SELECT message_text FROM telegram_messages").fetchall()
    assert len(rows) == 2
    assert all(r['message_text'] == '' for r in rows)


# -------- enqueue_notifications: существующие записи --------

def test_enqueue_skips_existing_when_reset_false(db, ini_config, mock_logger):
    enqueue_notifications(
        db, address='a', period_id=1, message_text='first',
        ini_config=ini_config, logger=mock_logger,
    )
    # Помечаем как отправленные, чтобы проверить, что их не трогают
    db.execute("UPDATE telegram_messages SET status='sent', attempts=2 WHERE channel='telegram'")
    db.commit()

    result = enqueue_notifications(
        db, address='a', period_id=1, message_text='second',
        ini_config=ini_config, logger=mock_logger,
        reset_existing=False,
    )
    assert result == {'telegram': 'exists', 'vk': 'exists'}

    tg = db.execute(
        "SELECT status, message_text, attempts FROM telegram_messages "
        "WHERE channel='telegram'"
    ).fetchone()
    assert tg['status'] == 'sent'
    assert tg['message_text'] == 'first'
    assert tg['attempts'] == 2


def test_enqueue_reset_existing_updates_all_fields(db, ini_config, mock_logger):
    enqueue_notifications(
        db, address='a', period_id=1, message_text='old',
        ini_config=ini_config, logger=mock_logger,
    )
    db.execute(
        "UPDATE telegram_messages SET status='sent', attempts=3, "
        "last_error='oops', sent_at='2025-01-01'"
    )
    db.commit()

    result = enqueue_notifications(
        db, address='a', period_id=1, message_text='new',
        ini_config=ini_config, logger=mock_logger,
        reset_existing=True,
    )
    assert result == {'telegram': 'reset', 'vk': 'reset'}

    rows = db.execute("SELECT * FROM telegram_messages").fetchall()
    for r in rows:
        assert r['status'] == 'pending'
        assert r['attempts'] == 0
        assert r['last_error'] is None
        assert r['sent_at'] is None
        assert r['message_text'] == 'new'


def test_enqueue_no_duplicates_after_second_call(db, ini_config, mock_logger):
    """Второй вызов с reset_existing=False не создаёт дублей."""
    enqueue_notifications(
        db, address='a', period_id=1, message_text='m',
        ini_config=ini_config, logger=mock_logger,
    )
    enqueue_notifications(
        db, address='a', period_id=1, message_text='m',
        ini_config=ini_config, logger=mock_logger,
    )
    count = db.execute("SELECT COUNT(*) FROM telegram_messages").fetchone()[0]
    assert count == 2  # один telegram + один vk, без повторов


# -------- Изоляция по (address, period_id) --------

def test_enqueue_isolates_by_address_and_period(db, ini_config, mock_logger):
    enqueue_notifications(db, address='a1', period_id=1, message_text='x',
                          ini_config=ini_config, logger=mock_logger)
    enqueue_notifications(db, address='a2', period_id=1, message_text='y',
                          ini_config=ini_config, logger=mock_logger)
    enqueue_notifications(db, address='a1', period_id=2, message_text='z',
                          ini_config=ini_config, logger=mock_logger)

    total = db.execute("SELECT COUNT(*) FROM telegram_messages").fetchone()[0]
    assert total == 6  # 3 пары × 2 канала