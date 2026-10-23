#!/usr/bin/env python3
"""Централизованная постановка задач на отправку уведомлений в мессенджеры.

Единственное место, где принимается решение «в какие каналы ставить задачу».
Используется и оркестратором (автоматически после анализа), и frontend'ом
(при ручном создании задачи на отправку).
"""
from utils import load_ini_config

# Каналы, которые поддерживаются в системе.
# В таблице telegram_messages они различаются полем channel.
MESSENGER_CHANNELS = ('telegram', 'vk')

# Какие ключи в секции [notifications] config.ini включают/выключают канал.
CHANNEL_CONFIG_KEYS = {
    'telegram': 'telegram_enabled',
    'vk': 'vk_enabled',
}


def get_enabled_channels(ini_config=None):
    """Возвращает список включённых в конфиге каналов (в порядке MESSENGER_CHANNELS)."""
    if ini_config is None:
        ini_config = load_ini_config()
    enabled = []
    for channel in MESSENGER_CHANNELS:
        key = CHANNEL_CONFIG_KEYS[channel]
        try:
            if ini_config.getboolean('notifications', key, fallback=False):
                enabled.append(channel)
        except Exception:
            # Битый конфиг — считаем канал выключенным, не роняем задачу
            pass
    return enabled


def enqueue_notifications(conn, address, period_id, message_text=None,
                          ini_config=None, logger=None, reset_existing=False):
    """Ставит в очередь задачи на отправку во ВСЕ включённые каналы.

    Для каждой пары (address, period_id, channel):
      * если записи ещё нет — создаётся pending-задача;
      * если запись уже есть и reset_existing=False — пропускается;
      * если запись уже есть и reset_existing=True — сбрасывается в pending
        (attempts=0, last_error=NULL, sent_at=NULL, message_text обновляется).

    Параметры:
      message_text   — готовый текст сообщения. Если None, в БД пишется пустая
                       строка, и текст будет сформирован позже в процессоре
                       (process_telegram_messages / process_vk_messages).
      ini_config     — ConfigParser; по умолчанию читается из config.ini.
      logger         — опциональный логгер для трассировки.
      reset_existing — сбрасывать ли существующие записи в pending.

    Возвращает dict {channel: 'created'|'reset'|'exists'}, где значение —
    что произошло с записью для данного канала.
    """
    if ini_config is None:
        ini_config = load_ini_config()
    enabled = get_enabled_channels(ini_config)
    if not enabled:
        if logger:
            logger.info("Нет включённых каналов — задачи не поставлены.")
        return {}

    existing = {
        row['channel']: row['id'] for row in conn.execute(
            "SELECT id, channel FROM telegram_messages "
            "WHERE address = ? AND period_id = ?",
            (address, period_id),
        ).fetchall()
    }

    text_for_db = message_text if message_text is not None else ''
    result = {}

    for channel in enabled:
        if channel in existing:
            if reset_existing:
                conn.execute("""
                    UPDATE telegram_messages
                    SET status = 'pending',
                        attempts = 0,
                        last_error = NULL,
                        sent_at = NULL,
                        message_text = ?
                    WHERE id = ?
                """, (text_for_db, existing[channel]))
                result[channel] = 'reset'
            else:
                result[channel] = 'exists'
        else:
            conn.execute("""
                INSERT INTO telegram_messages
                    (address, period_id, message_text, status, channel)
                VALUES (?, ?, ?, 'pending', ?)
            """, (address, period_id, text_for_db, channel))
            result[channel] = 'created'

    conn.commit()

    if logger:
        summary = ', '.join(f"{ch}={state}" for ch, state in result.items())
        logger.info(f"Очередь уведомлений {address} / {period_id}: {summary}")
    return result