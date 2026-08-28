#!/usr/bin/env python3
"""
Парсер для писем от uslugi@mos.ru.
Извлекает данные из HTML или текста и сохраняет в БД.
"""
import os
import sys
import re
import email
import logging
from email.policy import default
from bs4 import BeautifulSoup
from pathlib import Path

# Добавляем путь к корню проекта
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import get_db_connection, setup_logging
from parsers.base_parser import (
    get_or_create_account, get_or_create_period, save_charges,
    normalize_service_name
)

logger = logging.getLogger(__name__)

def parse_html_email(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {
        'account_number': None,
        'address': None,
        'year': None,
        'month': None,
        'total_due': None,
        'services': [],
        'period_info': {}
    }

    full_text = soup.get_text(separator=' ')
    full_text = ' '.join(full_text.split())

    # Лицевой счёт
    account_match = re.search(r'(?:ФЛС|ФАС)\s*№?\s*(\d+)', full_text)
    if not account_match:
        account_match = re.search(r'код\s+плательщика\s*[:\s]*(\d+)', full_text, re.IGNORECASE)
    if not account_match:
        account_match = re.search(r'Плательщик\s+№\s*(\d+)', full_text, re.IGNORECASE)
    if account_match:
        data['account_number'] = account_match.group(1)

    # Адрес
    addr_match = re.search(r'АДРЕС\s*[:\s]*([^\n\r]+?)(?=\s+(?:ПЕРИОД|код|на\s+\d{4}|$))', full_text, re.IGNORECASE)
    if addr_match:
        data['address'] = addr_match.group(1).strip()

    # Период
    period_match = re.search(r'ПЕРИОД\s*[:\s]*(\d{1,2})\s+месяц\s+(\d{4})\s+год', full_text, re.IGNORECASE)
    if period_match:
        data['month'] = int(period_match.group(1))
        data['year'] = int(period_match.group(2))
    else:
        period_match = re.search(r'на\s+(\d{4})-(\d{2})-(\d{2})', full_text)
        if period_match:
            data['year'] = int(period_match.group(1))
            data['month'] = int(period_match.group(2))

    # Итоговая сумма
    total_match = re.search(r'Итого к оплате\s*[:\s]*([\d\.,]+)', full_text, re.IGNORECASE)
    if total_match:
        total_str = total_match.group(1).replace(',', '.')
        data['total_due'] = float(total_str)

    # Доп. информация
    info_match = re.search(
        r'Тип кв\.:\s*(.+?)[,.]?\s+К-во комнат:\s*(\d+)\s+Площадь общая:\s*([\d\.]+),?\s+жилая:\s*([\d\.]+)',
        full_text, re.IGNORECASE
    )
    if info_match:
        data['period_info']['type'] = info_match.group(1).strip()
        data['period_info']['rooms'] = int(info_match.group(2))
        data['period_info']['total_area'] = float(info_match.group(3).replace(',', '.'))
        data['period_info']['living_area'] = float(info_match.group(4).replace(',', '.'))

    residents_match = re.search(r'(?:К-во проживающих|проживающих):\s*(\d+)', full_text, re.IGNORECASE)
    if residents_match:
        data['period_info']['residents'] = int(residents_match.group(1))

    paydate_match = re.search(r'Дата последней оплаты:\s*([\d\.-]+)', full_text, re.IGNORECASE)
    if paydate_match:
        data['period_info']['last_payment_date'] = paydate_match.group(1)

    # ---- Поиск таблицы с услугами ----
    tables = soup.find_all('table')
    services_found = False
    service_keywords = ['ХВС', 'ГВС', 'Водоотв', 'Отоп', 'Сод.жил', 'кап. ремонт', 'ТКО', 'Запирающее', 'шлагбаумов']

    def is_service_row(cells):
        if len(cells) < 3:
            return False
        first_cell_text = cells[0].get_text(strip=True)
        if not any(kw in first_cell_text for kw in service_keywords):
            return False
        second_text = cells[1].get_text(strip=True)
        third_text = cells[2].get_text(strip=True)
        numbers_second = re.findall(r'[\d\.,]+', second_text)
        numbers_third = re.findall(r'[\d\.,]+', third_text)
        if not numbers_second or not numbers_third:
            return False
        return True

    best_table = None
    max_service_rows = 0
    for table in tables:
        rows = table.find_all('tr')
        service_rows_count = 0
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if is_service_row(cells):
                service_rows_count += 1
        if service_rows_count > max_service_rows:
            max_service_rows = service_rows_count
            best_table = table

    if best_table and max_service_rows > 0:
        logger.info(f"Найдена таблица с {max_service_rows} строками услуг")
        rows = best_table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if not is_service_row(cells):
                continue
            service_name_raw = cells[0].get_text(strip=True)
            second_text = cells[1].get_text(strip=True)
            third_text = cells[2].get_text(strip=True)
            quantity_match = re.search(r'([\d\.,]+)', second_text)
            if not quantity_match:
                continue
            quantity_str = quantity_match.group(1).replace(',', '.')
            try:
                quantity = float(quantity_str)
            except ValueError:
                continue
            amount_match = re.search(r'([\d\.,]+)', third_text)
            if not amount_match:
                continue
            amount_str = amount_match.group(1).replace(',', '.')
            try:
                amount_due = float(amount_str)
            except ValueError:
                continue

            service_data = {
                'name': service_name_raw,
                'unit': '?',
                'quantity': quantity,
                'tariff': 0.0,
                'amount_due': amount_due,
                'benefit': 0.0,
                'recalculation': 0.0
            }
            data['services'].append(service_data)
            services_found = True

    if services_found:
        logger.info(f"Найдено услуг в HTML-таблице: {len(data['services'])}")
        return data
    else:
        logger.info("Не удалось найти таблицу с услугами в HTML")
        return None

def parse_text_epd(text):
    data = {
        'account_number': None,
        'address': None,
        'year': None,
        'month': None,
        'total_due': None,
        'services': [],
        'period_info': {}
    }

    original_text = text
    text = ' '.join(text.split())

    # Лицевой счёт
    account_match = re.search(r'(?:ФЛС|ФАС)\s*№?\s*(\d+)', text)
    if not account_match:
        account_match = re.search(r'код\s+плательщика\s*[:\s]*(\d+)', text, re.IGNORECASE)
    if not account_match:
        account_match = re.search(r'Плательщик\s+№\s*(\d+)', text, re.IGNORECASE)
    if account_match:
        data['account_number'] = account_match.group(1)

    # Адрес
    addr_match = re.search(r'АДРЕС\s*[:\s]*([^\n\r]+?)(?=\s+(?:ПЕРИОД|код|на\s+\d{4}|$))', text, re.IGNORECASE)
    if addr_match:
        data['address'] = addr_match.group(1).strip()

    # Период
    period_match = re.search(r'ПЕРИОД\s*[:\s]*(\d{1,2})\s+месяц\s+(\d{4})\s+год', text, re.IGNORECASE)
    if period_match:
        data['month'] = int(period_match.group(1))
        data['year'] = int(period_match.group(2))
    else:
        period_match = re.search(r'на\s+(\d{4})-(\d{2})-(\d{2})', text)
        if period_match:
            data['year'] = int(period_match.group(1))
            data['month'] = int(period_match.group(2))

    # Итог
    total_match = re.search(r'Итого к оплате\s*[:\s]*([\d\.,]+)', text, re.IGNORECASE)
    if total_match:
        total_str = total_match.group(1).replace(',', '.')
        data['total_due'] = float(total_str)

    # Доп. информация
    info_match = re.search(
        r'Тип кв\.:\s*(.+?)[,.]?\s+К-во комнат:\s*(\d+)\s+Площадь общая:\s*([\d\.]+),?\s+жилая:\s*([\d\.]+)',
        text, re.IGNORECASE
    )
    if info_match:
        data['period_info']['type'] = info_match.group(1).strip()
        data['period_info']['rooms'] = int(info_match.group(2))
        data['period_info']['total_area'] = float(info_match.group(3).replace(',', '.'))
        data['period_info']['living_area'] = float(info_match.group(4).replace(',', '.'))

    residents_match = re.search(r'(?:К-во проживающих|проживающих):\s*(\d+)', text, re.IGNORECASE)
    if residents_match:
        data['period_info']['residents'] = int(residents_match.group(1))

    paydate_match = re.search(r'Дата последней оплаты:\s*([\d\.-]+)', text, re.IGNORECASE)
    if paydate_match:
        data['period_info']['last_payment_date'] = paydate_match.group(1)

    # Парсинг услуг построчно
    lines = original_text.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue
        keywords = [
            ('ХВС', 'ХВС КПУ'),
            ('ГВС', 'ГВС КПУ'),
            ('Водоотв', 'Водоотведение'),
            ('Отоп', 'Отопление'),
            ('Сод.жил', 'Содержание жилья'),
            ('кап. ремонт', 'Капремонт'),
            ('ТКО', 'ТКО'),
            ('Запирающее', 'Запирающее устройство'),
            ('шлагбаумов', 'Шлагбаум')
        ]
        found_keyword = None
        service_name = None
        for kw, name in keywords:
            if kw in line:
                found_keyword = kw
                service_name = name
                break

        if found_keyword:
            numbers = re.findall(r'([\d\.,]+)', line)
            if len(numbers) >= 2:
                cleaned_numbers = []
                for n in numbers:
                    n_clean = n.replace(' ', '').replace(',', '.')
                    try:
                        cleaned_numbers.append(float(n_clean))
                    except ValueError:
                        continue
                if len(cleaned_numbers) >= 2:
                    quantity = cleaned_numbers[-2]
                    amount_due = cleaned_numbers[-1]
                    service_data = {
                        'name': service_name,
                        'unit': '?',
                        'quantity': quantity,
                        'amount_due': amount_due,
                        'tariff': 0.0,
                        'benefit': 0.0,
                        'recalculation': 0.0
                    }
                    data['services'].append(service_data)

    logger.info(f"Текстовый парсер нашёл услуг: {len(data['services'])}")
    return data

def process_email_file(filepath, mail_id):
    """
    Основная функция обработки файла письма.
    Возвращает (success, error_message).
    """
    logger.info(f"Начинаем парсинг файла: {filepath}, mail_id: {mail_id}")
    with open(filepath, 'rb') as f:
        raw_email = f.read()

    msg = email.message_from_bytes(raw_email, policy=default)

    html_parts = []
    text_parts = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == 'text/html':
                html_parts.append(part)
            elif content_type == 'text/plain':
                text_parts.append(part)
    else:
        content_type = msg.get_content_type()
        if content_type == 'text/html':
            html_parts.append(msg)
        elif content_type == 'text/plain':
            text_parts.append(msg)

    def decode_part(part):
        charset = part.get_content_charset() or 'utf-8'
        payload = part.get_payload(decode=True)
        if payload is None:
            return ''
        try:
            return payload.decode(charset, errors='replace')
        except LookupError:
            return payload.decode('utf-8', errors='replace')

    parsed_data = None
    for part in html_parts:
        html_content = decode_part(part)
        if html_content:
            data = parse_html_email(html_content)
            if data and data['account_number'] and data['year'] and data['month'] and data['services']:
                parsed_data = data
                break

    if not parsed_data:
        for part in text_parts:
            text_content = decode_part(part)
            if text_content:
                data = parse_text_epd(text_content)
                if data and data['account_number'] and data['year'] and data['month']:
                    parsed_data = data
                    break

    if not parsed_data:
        error = f"Не удалось извлечь данные из письма {filepath}"
        logger.error(error)
        return False, error

    # Сохраняем в БД
    try:
        conn = get_db_connection()
        account_id = get_or_create_account(conn, parsed_data['account_number'], parsed_data['address'] or '')
        period_id = get_or_create_period(conn, parsed_data['year'], parsed_data['month'])

        # Сохраняем доп. информацию
        if parsed_data.get('period_info'):
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO account_period_info
                (account_id, period_id, total_area, living_area, rooms, residents, last_payment_date, meter_readings)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account_id,
                period_id,
                parsed_data['period_info'].get('total_area'),
                parsed_data['period_info'].get('living_area'),
                parsed_data['period_info'].get('rooms'),
                parsed_data['period_info'].get('residents'),
                parsed_data['period_info'].get('last_payment_date'),
                None
            ))
            conn.commit()

        # Сохраняем услуги
        save_charges(conn, account_id, period_id, parsed_data['services'])

        # Сохраняем запись о сыром импорте
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO raw_imports (source_type, source_identifier, raw_content, account_id)
            VALUES (?, ?, ?, ?)
        """, ('email', mail_id, filepath, account_id))
        conn.commit()
        conn.close()

        logger.info(f"Данные из письма {filepath} успешно сохранены в БД")
        return True, None
    except Exception as e:
        error = f"Ошибка сохранения в БД: {e}"
        logger.exception(error)
        return False, error

if __name__ == "__main__":
    # Для тестирования: python3 parsers/mos_parser.py <email_file> <mail_id>
    if len(sys.argv) >= 2:
        filepath = sys.argv[1]
        mail_id = sys.argv[2] if len(sys.argv) > 2 else 'test'
        # Настройка логирования
        from utils import setup_logging
        logger = setup_logging('mos_parser')
        success, err = process_email_file(filepath, mail_id)
        if success:
            print("OK")
        else:
            print("ERROR:", err)
            sys.exit(1)
