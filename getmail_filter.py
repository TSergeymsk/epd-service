#!/usr/bin/env python3
"""
Фильтр для getmail, который проверяет входящие письма по заданным шаблонам,
сохраняет подходящие письма во временный каталог и запускает email_parser.py.
Письмо передаётся на stdin и должно быть выведено в stdout без изменений.
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

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'getmail_filter.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stderr)
        ]
    )
    return logging.getLogger(__name__)

def load_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def main():
    raw_email = sys.stdin.buffer.read()
    if not raw_email:
        sys.exit(0)

    script_dir = Path(__file__).parent.absolute()
    config_path = script_dir / 'config.ini'
    config = load_config(str(config_path))

    log_dir = config.get('logging', 'log_dir')
    logger = setup_logging(log_dir)
    logger.info("Получено письмо, начинаем обработку фильтром")

    msg = email.message_from_bytes(raw_email, policy=default)
    from_header = msg.get('From', '')
    to_header = msg.get('To', '')
    subject_header = msg.get('Subject', '')

    logger.debug(f"From: {from_header}")
    logger.debug(f"To: {to_header}")
    logger.debug(f"Subject: {subject_header}")

    try:
        from_pattern = config.get('getmail_filter', 'from_pattern')
        to_pattern = config.get('getmail_filter', 'to_pattern')
        subject_pattern = config.get('getmail_filter', 'subject_pattern')
        temp_dir = config.get('paths', 'email_temp_dir')
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.error(f"Ошибка чтения конфигурации фильтра: {e}")
        sys.stdout.buffer.write(raw_email)
        sys.exit(0)

    from_match = from_pattern in from_header
    to_match = to_pattern in to_header
    subject_match = subject_pattern in subject_header

    logger.info(f"from_match={from_match}, to_match={to_match}, subject_match={subject_match}")

    if from_match and to_match and subject_match:
        logger.info("Письмо соответствует критериям, начинаем обработку")
        os.makedirs(temp_dir, exist_ok=True)

        timestamp = int(time.time())
        content_hash = hashlib.md5(raw_email).hexdigest()[:8]
        filename = f"email_{timestamp}_{content_hash}.eml"
        filepath = os.path.join(temp_dir, filename)

        try:
            with open(filepath, 'wb') as f:
                f.write(raw_email)
            logger.info(f"Письмо сохранено во временный файл: {filepath}")
        except Exception as e:
            logger.error(f"Не удалось сохранить письмо: {e}")
            sys.stdout.buffer.write(raw_email)
            sys.exit(0)

        parser_script = script_dir / 'email_parser.py'
        try:
            # Увеличиваем таймаут до 180 секунд
            result = subprocess.run(
                [sys.executable, str(parser_script), filepath],
                capture_output=True,
                text=True,
                timeout=180
            )
            if result.returncode == 0:
                logger.info(f"Парсер успешно обработал {filepath}, удаляем временный файл")
                os.remove(filepath)
            else:
                logger.error(f"Парсер завершился с ошибкой (код {result.returncode}): {result.stderr}")
                # Оставляем файл для отладки
        except subprocess.TimeoutExpired:
            logger.error(f"Парсер превысил время ожидания (180 сек) для {filepath}")
        except Exception as e:
            logger.exception(f"Ошибка при запуске парсера: {e}")
    else:
        logger.info("Письмо не соответствует критериям фильтрации")

    sys.stdout.buffer.write(raw_email)

if __name__ == '__main__':
    main()
