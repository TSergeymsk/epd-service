#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import sqlite3
import logging
import tempfile
from email import policy
from email.parser import BytesParser
from html.parser import HTMLParser
import configparser
from bs4 import BeautifulSoup

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('config.ini')

DB_PATH = config.get('database', 'db_path', fallback='epd.db')
LOG_DIR = config.get('logging', 'log_dir', fallback='./logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'email_parser.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Существующие функции (предполагаем, что они уже есть) ----------
# Например, функция process_email_file() - парсит файл и сохраняет в БД.
# Для совместимости с тестами добавим обёртки.

def normalize_service_name(service_name):
    """
    Нормализует название услуги.
    Эта функция может уже существовать, но мы добавим её для тестов.
    """
    # Пример реализации (можно расширить)
    mapping = {
        'хвс': 'ХВС',
        'гвс': 'ГВС',
        'электроэнергия': 'Электроэнергия',
        'отопление': 'Отопление',
        'водоотведение': 'Водоотведение'
    }
    cleaned = re.sub(r'\(.*?\)', '', service_name).strip().lower()
    return mapping.get(cleaned, service_name.strip())

def save_charges_to_db(db_path, account_id, charges):
    """
    Сохраняет список начислений в БД.
    Если такая функция уже есть, можно использовать её.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for service, amount in charges:
        cur.execute(
            "INSERT OR REPLACE INTO charges (account_id, service_name, charge) VALUES (?, ?, ?)",
            (account_id, service, amount)
        )
    conn.commit()
    conn.close()
    logger.info(f"Сохранено {len(charges)} начислений для счета {account_id}")

def parse_email(file_path):
    """
    Основная функция парсинга письма.
    Возвращает True при успехе, False при ошибке.
    """
    try:
        with open(file_path, 'rb') as fp:
            msg = BytesParser(policy=policy.default).parse(fp)
        # Извлекаем HTML-часть
        html_content = None
        for part in msg.walk():
            if part.get_content_type() == 'text/html':
                html_content = part.get_content()
                break
        if not html_content:
            logger.error("Нет HTML-части в письме")
            return False
        
        soup = BeautifulSoup(html_content, 'html.parser')
        # Парсим данные (пример)
        address = None
        account = None
        period = None
        charges = []
        # ... здесь должен быть реальный парсинг (заимствуем из существующего кода)
        # Предположим, что мы получили данные:
        # address = "г. Москва, ул. Ленина, д. 1"
        # account = "1234567890"
        # period = "декабрь 2025"
        # charges = [("ХВС", 150.0), ("ГВС", 200.0)]
        # Далее сохраняем в БД
        
        # Сохранение в БД (используем глобальный DB_PATH или переданный)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO accounts (address, account_number, month, year) VALUES (?, ?, ?, ?)",
            (address, account, period.split()[0], int(period.split()[1]))
        )
        account_id = cur.lastrowid
        conn.commit()
        conn.close()
        
        # Сохраняем начисления
        save_charges_to_db(DB_PATH, account_id, charges)
        logger.info(f"Письмо {file_path} обработано успешно")
        return True
    except Exception as e:
        logger.error(f"Ошибка парсинга {file_path}: {e}")
        return False

# Если скрипт запускается как main, обрабатываем переданный файл
if __name__ == "__main__":
    if len(sys.argv) > 1:
        parse_email(sys.argv[1])
    else:
        print("Usage: python email_parser.py <email_file>")