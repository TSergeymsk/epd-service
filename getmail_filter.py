#!/usr/bin/env python3
"""
Фильтр для getmail. Проверяет входящие письма по правилам из БД.
При совпадении запускает соответствующий парсер асинхронно,
избегая дублирования по Message-ID.
"""
import os
import sys
import email
from email.policy import default
import logging
import configparser
import subprocess
import time
import hashlib
from pathlib import Path

# Добавляем путь к корню проекта
sys.path.insert(0, str(Path(__file__).parent))
from utils import get_db_connection, setup_logging, load_ini_config

logger = logging.getLogger(__name__)

def get_rules_from_db():
    """Загружает активные правила фильтрации из БД."""
    conn = get_db_connection()
    cur = conn.execute("""
        SELECT id, from_pattern, to_pattern, subject_pattern, parser_script
        FROM filter_rules
        WHERE enabled = 1
        ORDER BY priority DESC
    """)
    rules = cur.fetchall()
    conn.close()
    return rules

def is_already_imported(mail_id):
    """Проверяет, было ли письмо с данным mail_id уже импортировано."""
    conn = get_db_connection()
    cur = conn.execute("SELECT 1 FROM imported_emails WHERE mail_id = ?", (mail_id,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists

def mark_imported(mail_id, rule_id):
    """Записывает факт импорта письма."""
    conn = get_db_connection()
    conn.execute(
        "INSERT OR IGNORE INTO imported_emails (mail_id, rule_id, status) VALUES (?, ?, 'imported')",
        (mail_id, rule_id)
    )
    conn.commit()
    conn.close()

def update_import_status(mail_id, status, error_text=None):
    """Обновляет статус обработки письма."""
    conn = get_db_connection()
    if error_text:
        conn.execute(
            "UPDATE imported_emails SET status = ?, error_text = ? WHERE mail_id = ?",
            (status, error_text, mail_id)
        )
    else:
        conn.execute(
            "UPDATE imported_emails SET status = ? WHERE mail_id = ?",
            (status, mail_id)
        )
    conn.commit()
    conn.close()

def main():
    raw_email = sys.stdin.buffer.read()
    if not raw_email:
        sys.exit(0)

    # Настройка логирования (только в файл)
    config = load_ini_config()
    log_dir = config.get('logging', 'log_dir')
    logger = setup_logging('getmail_filter', log_dir)
    logger.info("Получено письмо, начинаем обработку фильтром")

    msg = email.message_from_bytes(raw_email, policy=default)
    from_header = msg.get('From', '')
    to_header = msg.get('To', '')
    subject_header = msg.get('Subject', '')
    message_id = msg.get('Message-ID', '')

    # Если Message-ID нет, генерируем хеш от содержимого
    if not message_id:
        message_id = hashlib.md5(raw_email).hexdigest()
        logger.warning(f"Message-ID отсутствует, сгенерирован хеш: {message_id}")

    logger.debug(f"From: {from_header}, To: {to_header}, Subject: {subject_header}, Message-ID: {message_id}")

    # Проверяем, не импортировано ли уже
    if is_already_imported(message_id):
        logger.info(f"Письмо с Message-ID {message_id} уже импортировано, пропускаем")
        sys.stdout.buffer.write(raw_email)
        sys.exit(0)

    # Загружаем правила
    rules = get_rules_from_db()
    if not rules:
        logger.info("Нет активных правил фильтрации, письмо пропущено")
        sys.stdout.buffer.write(raw_email)
        sys.exit(0)

    matched_rule = None
    for rule in rules:
        if (rule['from_pattern'] in from_header and
            rule['to_pattern'] in to_header and
            rule['subject_pattern'] in subject_header):
            matched_rule = rule
            break

    if not matched_rule:
        logger.info("Письмо не соответствует ни одному правилу")
        sys.stdout.buffer.write(raw_email)
        sys.exit(0)

    logger.info(f"Письмо соответствует правилу ID={matched_rule['id']}, запускаем парсер {matched_rule['parser_script']}")

    # Сохраняем письмо во временную папку
    temp_dir = config.get('paths', 'email_temp_dir')
    os.makedirs(temp_dir, exist_ok=True)
    timestamp = int(time.time())
    filename = f"email_{timestamp}_{message_id[:8]}.eml"
    filepath = os.path.join(temp_dir, filename)
    with open(filepath, 'wb') as f:
        f.write(raw_email)
    logger.info(f"Письмо сохранено во временный файл: {filepath}")

    # Отмечаем импорт
    mark_imported(message_id, matched_rule['id'])

    # Запускаем парсер в фоновом режиме (не ждём завершения)
    parser_script = matched_rule['parser_script']
    # Если путь относительный, делаем абсолютным относительно корня проекта
    if not os.path.isabs(parser_script):
        parser_script = str(Path(__file__).parent / parser_script)

    try:
        subprocess.Popen(
            [sys.executable, parser_script, filepath, message_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )
        logger.info(f"Парсер {parser_script} запущен в фоне для {filepath}")
    except Exception as e:
        logger.error(f"Ошибка запуска парсера: {e}")
        update_import_status(message_id, 'parsing_error', str(e))

    sys.stdout.buffer.write(raw_email)

if __name__ == '__main__':
    main()
