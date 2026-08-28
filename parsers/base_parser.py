#!/usr/bin/env python3
"""
Базовые функции для всех парсеров: работа с БД, нормализация названий.
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import get_db_connection

def normalize_service_name(raw_name):
    if not raw_name:
        return raw_name
    raw_lower = raw_name.lower()
    clean_name = re.sub(r'\s+пр\.?\s*д\.?$', '', raw_lower).strip()

    if 'хвс' in clean_name:
        return 'ХВС'
    if 'гвс' in clean_name:
        return 'ГВС'
    if 'водоотв' in clean_name:
        return 'Водоотведение'
    if 'отоп' in clean_name:
        return 'Отопление'
    if 'сод.жил' in clean_name or 'сод жил' in clean_name or 'содержание жилья' in clean_name:
        return 'Содержание жилья'
    if 'кап. ремонт' in clean_name or 'кап ремонт' in clean_name or 'капитальный ремонт' in clean_name:
        return 'Капитальный ремонт'
    if 'тко' in clean_name:
        return 'ТКО'
    if 'запирающее' in clean_name:
        return 'Запирающее устройство'
    if 'шлагбаум' in clean_name:
        return 'Шлагбаум'
    return raw_name.strip()

def get_or_create_account(conn, account_number, address):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM accounts WHERE account_number = ?", (account_number,))
    row = cursor.fetchone()
    if row:
        return row['id']
    cursor.execute(
        "INSERT INTO accounts (account_number, address) VALUES (?, ?)",
        (account_number, address)
    )
    conn.commit()
    return cursor.lastrowid

def get_or_create_period(conn, year, month):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM periods WHERE year = ? AND month = ?", (year, month))
    row = cursor.fetchone()
    if row:
        return row['id']
    start_date = f"{year:04d}-{month:02d}-01"
    cursor.execute(
        "INSERT INTO periods (year, month, start_date) VALUES (?, ?, ?)",
        (year, month, start_date)
    )
    conn.commit()
    return cursor.lastrowid

def get_or_create_service(conn, name, unit=None):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM services WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row:
        return row['id']
    cursor.execute(
        "INSERT INTO services (name, unit) VALUES (?, ?)",
        (name, unit)
    )
    conn.commit()
    return cursor.lastrowid

def save_charges(conn, account_id, period_id, services_data):
    """
    services_data: список словарей с полями name, unit, quantity, tariff, amount_due, benefit, recalculation
    """
    cursor = conn.cursor()
    for svc in services_data:
        normalized = normalize_service_name(svc['name'])
        service_id = get_or_create_service(conn, normalized, svc.get('unit'))
        cursor.execute("""
            INSERT OR REPLACE INTO charges
            (account_id, period_id, service_id, quantity, tariff, accrued_by_tariff, benefit, recalculation, amount_due)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account_id,
            period_id,
            service_id,
            svc.get('quantity', 0),
            svc.get('tariff', 0),
            svc.get('quantity', 0) * svc.get('tariff', 0),
            svc.get('benefit', 0),
            svc.get('recalculation', 0),
            svc.get('amount_due', 0)
        ))
    conn.commit()
