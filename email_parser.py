#!/usr/bin/env python3
"""
Парсер email-писем, содержащих данные ЕПД.
После успешного сохранения запускает анализ ИИ и отправляет результат в Telegram.
"""
import os
import sys
import re
import sqlite3
import logging
import configparser
import email
import html
import requests
from email.policy import default
from bs4 import BeautifulSoup

from ai_analyzer import analyze_address_month, get_accounts_for_address

def setup_logging(config):
    log_dir = config.get('logging', 'log_dir')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'email_parser.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = logging.getLogger(__name__)

def load_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

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
    logger.debug(f"Не удалось нормализовать название услуги: {raw_name}")
    return raw_name.strip()

def get_or_create_account(conn, account_number, address):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM accounts WHERE account_number = ?", (account_number,))
    row = cursor.fetchone()
    if row:
        return row[0]
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
        return row[0]
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
        return row[0]
    cursor.execute(
        "INSERT INTO services (name, unit) VALUES (?, ?)",
        (name, unit)
    )
    conn.commit()
    return cursor.lastrowid

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

    account_match = re.search(r'(?:ФЛС|ФАС)\s*№?\s*(\d+)', full_text)
    if not account_match:
        account_match = re.search(r'код\s+плательщика\s*[:\s]*(\d+)', full_text, re.IGNORECASE)
    if not account_match:
        account_match = re.search(r'Плательщик\s+№\s*(\d+)', full_text, re.IGNORECASE)
    if account_match:
        data['account_number'] = account_match.group(1)

    addr_match = re.search(r'АДРЕС\s*[:\s]*([^\n\r]+?)(?=\s+(?:ПЕРИОД|код|на\s+\d{4}|$))', full_text, re.IGNORECASE)
    if addr_match:
        data['address'] = addr_match.group(1).strip()

    period_match = re.search(r'ПЕРИОД\s*[:\s]*(\d{1,2})\s+месяц\s+(\d{4})\s+год', full_text, re.IGNORECASE)
    if period_match:
        data['month'] = int(period_match.group(1))
        data['year'] = int(period_match.group(2))
    else:
        period_match = re.search(r'на\s+(\d{4})-(\d{2})-(\d{2})', full_text)
        if period_match:
            data['year'] = int(period_match.group(1))
            data['month'] = int(period_match.group(2))

    total_match = re.search(r'Итого к оплате\s*[:\s]*([\d\.,]+)', full_text, re.IGNORECASE)
    if total_match:
        total_str = total_match.group(1).replace(',', '.')
        data['total_due'] = float(total_str)

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
            logger.debug(f"Найдена услуга в таблице: {service_data}")

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

    account_match = re.search(r'(?:ФЛС|ФАС)\s*№?\s*(\d+)', text)
    if not account_match:
        account_match = re.search(r'код\s+плательщика\s*[:\s]*(\d+)', text, re.IGNORECASE)
    if not account_match:
        account_match = re.search(r'Плательщик\s+№\s*(\d+)', text, re.IGNORECASE)
    if account_match:
        data['account_number'] = account_match.group(1)

    addr_match = re.search(r'АДРЕС\s*[:\s]*([^\n\r]+?)(?=\s+(?:ПЕРИОД|код|на\s+\d{4}|$))', text, re.IGNORECASE)
    if addr_match:
        data['address'] = addr_match.group(1).strip()

    period_match = re.search(r'ПЕРИОД\s*[:\s]*(\d{1,2})\s+месяц\s+(\d{4})\s+год', text, re.IGNORECASE)
    if period_match:
        data['month'] = int(period_match.group(1))
        data['year'] = int(period_match.group(2))
    else:
        period_match = re.search(r'на\s+(\d{4})-(\d{2})-(\d{2})', text)
        if period_match:
            data['year'] = int(period_match.group(1))
            data['month'] = int(period_match.group(2))

    total_match = re.search(r'Итого к оплате\s*[:\s]*([\d\.,]+)', text, re.IGNORECASE)
    if total_match:
        total_str = total_match.group(1).replace(',', '.')
        data['total_due'] = float(total_str)

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
                        'tariff': 0.0,
                        'amount_due': amount_due,
                        'benefit': 0.0,
                        'recalculation': 0.0
                    }
                    data['services'].append(service_data)

    logger.info(f"Текстовый парсер нашёл услуг: {len(data['services'])}")
    return data

def process_email(raw_content, source_identifier):
    msg = email.message_from_bytes(raw_content, policy=default)

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

    for part in html_parts:
        html_content = decode_part(part)
        if html_content:
            data = parse_html_email(html_content)
            if data and data['account_number'] and data['year'] and data['month'] and data['services']:
                logger.info("Данные успешно извлечены из HTML")
                return data
            else:
                logger.debug("HTML не дал полных данных, пробуем следующий HTML или текст")

    for part in text_parts:
        text_content = decode_part(part)
        if text_content:
            data = parse_text_epd(text_content)
            if data and data['account_number'] and data['year'] and data['month']:
                logger.info("Данные успешно извлечены из текста")
                return data

    return None

def save_to_db(conn, data, source_identifier):
    if not data['account_number'] or not data['year'] or not data['month']:
        logger.error(f"Не удалось определить обязательные поля в письме {source_identifier}")
        return False

    account_id = get_or_create_account(conn, data['account_number'], data['address'] or '')
    period_id = get_or_create_period(conn, data['year'], data['month'])

    if data['period_info']:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO account_period_info
            (account_id, period_id, total_area, living_area, rooms, residents, last_payment_date, meter_readings)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account_id,
            period_id,
            data['period_info'].get('total_area'),
            data['period_info'].get('living_area'),
            data['period_info'].get('rooms'),
            data['period_info'].get('residents'),
            data['period_info'].get('last_payment_date'),
            None
        ))
        conn.commit()

    for svc in data['services']:
        normalized_name = normalize_service_name(svc['name'])
        service_id = get_or_create_service(conn, normalized_name, svc.get('unit'))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO charges
            (account_id, period_id, service_id, quantity, tariff, accrued_by_tariff, benefit, recalculation, amount_due)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account_id,
            period_id,
            service_id,
            svc['quantity'],
            svc['tariff'],
            svc['quantity'] * svc['tariff'],
            svc['benefit'],
            svc['recalculation'],
            svc['amount_due']
        ))
        conn.commit()

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO raw_imports (source_type, source_identifier, raw_content, account_id)
        VALUES (?, ?, ?, ?)
    """, ('email', source_identifier, source_identifier, account_id))
    conn.commit()

    return True

def send_telegram_message(bot_token, chat_id, message_html):
    MAX_LEN = 4000
    if len(message_html) > MAX_LEN:
        logger.warning(f"Сообщение слишком длинное ({len(message_html)} символов), обрезаем до {MAX_LEN}")
        message_html = message_html[:MAX_LEN] + "... (обрезано)"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message_html,
        'parse_mode': 'HTML'
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 400:
            logger.error(f"Telegram API вернул 400. Тело ответа: {response.text}")
            logger.info("Повторная попытка отправки без parse_mode...")
            payload.pop('parse_mode')
            response2 = requests.post(url, json=payload, timeout=10)
            response2.raise_for_status()
            logger.info("Сообщение отправлено как обычный текст")
        else:
            response.raise_for_status()
            logger.info("Сообщение в Telegram отправлено успешно")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")

def convert_markdown_to_telegram_html(text):
    """
    Преобразует простую markdown-разметку в HTML, допустимый в Telegram.
    """
    # Экранируем специальные символы HTML
    text = html.escape(text)

    # Заголовки ### -> жирный текст
    text = re.sub(r'^### (.*?)$', r'<b>\1</b>', text, flags=re.MULTILINE)

    # Жирный текст **
    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)

    # Курсив *
    text = re.sub(r'(?<!\*)\*(?!\*)(.*?)(?<!\*)\*(?!\*)', r'<i>\1</i>', text)

    # Маркированные списки: строки, начинающиеся с * или - (после возможных пробелов)
    lines = text.split('\n')
    new_lines = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('* ') or stripped.startswith('- '):
            indent = len(line) - len(stripped)
            bullet = '• '
            new_lines.append(' ' * indent + bullet + stripped[2:])
        else:
            new_lines.append(line)
    text = '\n'.join(new_lines)

    return text

def format_telegram_message(address, account_number, year, month, aggregated_data, ai_response):
    lines = []
    lines.append("<b>Анализ ЕПД</b>")
    lines.append(f"🏠 <b>Адрес:</b> {html.escape(address)}")
    lines.append(f"🆔 <b>Лицевой счет:</b> {html.escape(account_number)}")
    lines.append(f"📅 <b>Период:</b> {year}-{month:02d}")
    lines.append("")

    if aggregated_data:
        total = sum(item['amount_due'] for item in aggregated_data)
        lines.append(f"💰 <b>Итого: {total:,.2f} руб.</b>, в т.ч.:")
        sorted_data = sorted(aggregated_data, key=lambda x: x['amount_due'], reverse=True)
        for item in sorted_data:
            quantity_str = f"{item['quantity']:.3f} {item.get('unit', 'ед.')}".replace('.', ',')
            lines.append(f"🔹 <b>{html.escape(item['name'])}:</b> {item['amount_due']:,.2f} руб. <i>{quantity_str}</i>")
    else:
        lines.append("💰 <b>Итого: данные не найдены</b>")
    lines.append("")

    lines.append("<b>Анализ:</b>")
    if ai_response:
        formatted_response = convert_markdown_to_telegram_html(ai_response)
        lines.append(formatted_response)
    else:
        lines.append("<i>Анализ не проведен (ошибка сервиса ИИ)</i>")

    return "\n".join(lines)

def main():
    if len(sys.argv) != 2:
        print("Использование: python3 email_parser.py <путь_к_файлу_письма>")
        sys.exit(1)

    email_path = sys.argv[1]
    if not os.path.isfile(email_path):
        print(f"Файл не найден: {email_path}")
        sys.exit(1)

    config = load_config()
    global logger
    logger = setup_logging(config)

    db_path = config.get('paths', 'db_path')

    with open(email_path, 'rb') as f:
        raw_email = f.read()

    data = process_email(raw_email, os.path.basename(email_path))

    if not data:
        logger.error(f"Не удалось извлечь данные из письма {email_path}")
        sys.exit(1)

    debug_dir = os.path.join(os.path.dirname(email_path), 'debug')
    os.makedirs(debug_dir, exist_ok=True)
    debug_path = os.path.join(debug_dir, os.path.basename(email_path) + '.raw')
    with open(debug_path, 'wb') as f:
        f.write(raw_email)
    logger.info(f"Raw-email сохранён в {debug_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    success = save_to_db(conn, data, os.path.basename(email_path))
    conn.close()

    if success:
        logger.info(f"Письмо {email_path} успешно обработано.")
        conn2 = sqlite3.connect(db_path)
        conn2.row_factory = sqlite3.Row
        try:
            account_ids = get_accounts_for_address(conn2, data['address'])
            logger.info(f"Найдены account_ids: {account_ids}")
            if account_ids:
                cur = conn2.cursor()
                cur.execute("SELECT id FROM periods WHERE year = ? AND month = ?", (data['year'], data['month']))
                period_row = cur.fetchone()
                if period_row:
                    period_id = period_row['id']
                    placeholders = ','.join('?' * len(account_ids))
                    query = f"""
                        SELECT s.name, s.unit, SUM(c.quantity) as quantity, SUM(c.amount_due) as amount_due
                        FROM charges c
                        JOIN services s ON c.service_id = s.id
                        WHERE c.account_id IN ({placeholders}) AND c.period_id = ?
                        GROUP BY s.name, s.unit
                        ORDER BY amount_due DESC
                    """
                    params = account_ids + [period_id]
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    agg_data = []
                    for row in rows:
                        agg_data.append({
                            'name': row['name'],
                            'unit': row['unit'] or 'ед.',
                            'quantity': row['quantity'] or 0,
                            'amount_due': row['amount_due'] or 0
                        })
                    logger.info(f"Получено aggregated_data: {agg_data}")
                else:
                    agg_data = []
                    logger.warning("Период не найден в БД")
            else:
                agg_data = []
                logger.warning("Не найдены счета для адреса")
        except Exception as e:
            logger.exception(f"Ошибка получения агрегированных данных: {e}")
            agg_data = []
        conn2.close()

        ai_response_text = None
        try:
            analysis_result = analyze_address_month(data['address'], data['year'], data['month'], config)
            if analysis_result:
                ai_response_text = analysis_result['response']
                logger.info("Анализ ИИ успешно получен")
            else:
                logger.warning("Не удалось получить анализ от ИИ")
        except Exception as e:
            logger.exception(f"Ошибка при вызове analyze_address_month: {e}")

        message = format_telegram_message(
            address=data['address'],
            account_number=data['account_number'],
            year=data['year'],
            month=data['month'],
            aggregated_data=agg_data,
            ai_response=ai_response_text
        )
        send_telegram_message(
            config.get('telegram', 'bot_token'),
            config.get('telegram', 'chat_id'),
            message
        )
    else:
        logger.error(f"Не удалось обработать письмо {email_path}")
        sys.exit(1)

if __name__ == '__main__':
    main()
