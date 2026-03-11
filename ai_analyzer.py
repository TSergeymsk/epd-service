#!/usr/bin/env python3
"""
Модуль для анализа ЕПД с помощью AI API.
Поддерживает OpenAI-совместимые эндпоинты (OpenRouter, Groq, Ollama и др.).
Запуск: python3 ai_analyzer.py [--limit N]
"""
import os
import sys
import sqlite3
import logging
import configparser
import argparse
import requests
import json
import time
from datetime import datetime

def setup_logging(config):
    log_dir = config.get('logging', 'log_dir')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'ai_analyzer.log')
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

def get_db_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_addresses_missing_analysis(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT a.address, p.id as period_id, p.year, p.month
        FROM accounts a
        CROSS JOIN periods p
        WHERE EXISTS (
            SELECT 1 FROM charges c
            JOIN accounts acc ON c.account_id = acc.id
            WHERE acc.address = a.address AND c.period_id = p.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM address_analysis aa
            WHERE aa.address = a.address AND aa.period_id = p.id
        )
        ORDER BY p.year DESC, p.month DESC, a.address
    """)
    return cursor.fetchall()

def get_accounts_for_address(conn, address):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM accounts WHERE address = ?", (address,))
    return [row['id'] for row in cursor.fetchall()]

def get_aggregated_month_data(conn, account_ids, year, month):
    if not account_ids:
        return []
    placeholders = ','.join('?' * len(account_ids))
    cursor = conn.cursor()
    query = f"""
        SELECT s.name,
               SUM(c.amount_due) as total_amount,
               SUM(c.quantity) as total_quantity
        FROM charges c
        JOIN services s ON c.service_id = s.id
        JOIN periods p ON c.period_id = p.id
        WHERE c.account_id IN ({placeholders})
          AND p.year = ? AND p.month = ?
        GROUP BY s.name
        ORDER BY s.name
    """
    params = account_ids + [year, month]
    cursor.execute(query, params)
    rows = cursor.fetchall()
    result = []
    for row in rows:
        total_q = row['total_quantity'] or 0
        avg_tariff = row['total_amount'] / total_q if total_q > 0 else 0
        result.append({
            'name': row['name'],
            'quantity': total_q,
            'tariff': avg_tariff,
            'amount_due': row['total_amount']
        })
    return result

def get_previous_month_aggregated(conn, account_ids, year, month):
    cursor = conn.cursor()
    placeholders = ','.join('?' * len(account_ids))
    query = f"""
        SELECT p.year, p.month
        FROM periods p
        JOIN charges c ON c.period_id = p.id
        WHERE c.account_id IN ({placeholders})
          AND (p.year < ? OR (p.year = ? AND p.month < ?))
        ORDER BY p.year DESC, p.month DESC
        LIMIT 1
    """
    params = account_ids + [year, year, month]
    cursor.execute(query, params)
    row = cursor.fetchone()
    if row:
        return get_aggregated_month_data(conn, account_ids, row['year'], row['month']), (row['year'], row['month'])
    return None, None

def get_last_12_months_aggregated(conn, account_ids, year, month):
    cursor = conn.cursor()
    placeholders = ','.join('?' * len(account_ids))
    
    months_query = f"""
        SELECT DISTINCT p.year, p.month
        FROM periods p
        JOIN charges c ON c.period_id = p.id
        WHERE c.account_id IN ({placeholders})
          AND (p.year < ? OR (p.year = ? AND p.month <= ?))
        ORDER BY p.year DESC, p.month DESC
        LIMIT 12
    """
    months_params = account_ids + [year, year, month]
    cursor.execute(months_query, months_params)
    months = cursor.fetchall()
    
    if not months:
        return {}
    
    month_conditions = " OR ".join(["(p.year = ? AND p.month = ?)"] * len(months))
    month_params = []
    for m in months:
        month_params.append(m['year'])
        month_params.append(m['month'])
    
    query = f"""
        SELECT p.year, p.month, s.name,
               SUM(c.amount_due) as total_amount
        FROM charges c
        JOIN periods p ON c.period_id = p.id
        JOIN services s ON c.service_id = s.id
        WHERE c.account_id IN ({placeholders})
          AND ({month_conditions})
        GROUP BY p.year, p.month, s.name
        ORDER BY p.year DESC, p.month DESC, s.name
    """
    params = account_ids + month_params
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    months_data = {}
    for row in rows:
        ym = f"{row['year']}-{row['month']:02d}"
        if ym not in months_data:
            months_data[ym] = {}
        months_data[ym][row['name']] = row['total_amount']
    
    logger.info(f"Получено {len(months_data)} месяцев для динамики (за последние 12 месяцев)")
    return months_data

def build_prompt(address, current_data, prev_data, last_12_data, current_ym, prev_ym):
    prompt = f"Ты аналитик ЖКХ. Проведи анализ ЕПД для адреса: {address} за период {current_ym}.\n\n"
    total_current = sum(r['amount_due'] for r in current_data)
    prompt += f"Данные за {current_ym} (агрегированы по всем лицевым счетам):\n"
    prompt += f"- Итого к оплате: {total_current:.2f} руб.\n"
    prompt += "- Структура услуг:\n"
    for r in current_data:
        prompt += f"  * {r['name']}: количество {r['quantity']:.3f}, средний тариф {r['tariff']:.2f} руб., сумма {r['amount_due']:.2f} руб.\n"
    prompt += "\n"

    if prev_data:
        total_prev = sum(r['amount_due'] for r in prev_data)
        prompt += f"Данные за предыдущий месяц {prev_ym}:\n"
        prompt += f"- Итого к оплате: {total_prev:.2f} руб.\n"
        prompt += "- Структура услуг:\n"
        for r in prev_data:
            prompt += f"  * {r['name']}: количество {r['quantity']:.3f}, средний тариф {r['tariff']:.2f} руб., сумма {r['amount_due']:.2f} руб.\n"
    else:
        prompt += "Данные за предыдущий месяц отсутствуют.\n"
    prompt += "\n"

    if last_12_data:
        prompt += f"Динамика за последние 12 месяцев (доступно {len(last_12_data)} месяцев):\n"
        for ym, services in sorted(last_12_data.items()):
            prompt += f"  {ym}:\n"
            for name, total in services.items():
                prompt += f"    {name}: {total:.2f} руб.\n"
    else:
        prompt += "Данных за последние 12 месяцев недостаточно.\n"
    prompt += "\n"

    prompt += """Требуется:
1. Анализ текущего месяца: структура потребления и стоимости, выделение основных услуг.
2. Сравнение с предыдущим месяцем: изменения в потреблении и стоимости, основные причины роста/снижения (если предыдущий месяц есть).
3. Анализ динамики за последние 12 месяцев (если достаточно данных): тренды, сезонность, структурные изменения. Если данных меньше 12, проанализируй доступные месяцы и укажи, что данных недостаточно для полноценного годового анализа, но дай характеристику имеющейся динамики.

Ответ дай в свободной форме, но структурированно, с цифрами и выводами. Будь краток, но информативен.
"""
    return prompt

def call_ai(config, prompt):
    api_key = config.get('openrouter', 'api_key')
    model = config.get('openrouter', 'model')
    url = config.get('openrouter', 'url')
    timeout = config.getint('openrouter', 'timeout', fallback=120)
    
    headers = {
        "Content-Type": "application/json"
    }
    
    if api_key and api_key.strip() and api_key != "local_ollama":
        headers["Authorization"] = f"Bearer {api_key}"
    
    if "openrouter.ai" in url:
        headers["HTTP-Referer"] = "http://localhost:5000"
        headers["X-Title"] = "EPD Analyzer"
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant specialized in analyzing utility bills."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 2000,
        "temperature": 0.7
    }
    
    logger.info(f"Отправка запроса к {url}, модель {model}, таймаут {timeout}с, длина промпта: {len(prompt)} символов")
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        
        if response.status_code != 200:
            logger.error(f"HTTP {response.status_code}: {response.text}")
        
        response.raise_for_status()
        data = response.json()
        
        if 'choices' in data and len(data['choices']) > 0:
            answer = data['choices'][0]['message']['content']
            tokens = data.get('usage', {}).get('total_tokens')
            logger.info(f"Получен ответ, токенов: {tokens}")
            return answer, tokens
        else:
            logger.error(f"Неожиданный формат ответа: {data}")
            return None, None
            
    except requests.exceptions.Timeout:
        logger.error(f"Таймаут при запросе к AI API (лимит {timeout}с)")
        return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к AI API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Тело ответа: {e.response.text}")
        return None, None
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        return None, None

def save_address_analysis(conn, address, period_id, prompt, response, model, tokens):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO address_analysis
        (address, period_id, prompt, response, model, tokens_used)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (address, period_id, prompt, response, model, tokens))
    conn.commit()

def analyze_address_month(address, year, month, config, conn=None):
    should_close = False
    if conn is None:
        conn = get_db_connection(config.get('paths', 'db_path'))
        should_close = True

    try:
        account_ids = get_accounts_for_address(conn, address)
        if not account_ids:
            logger.warning(f"Нет счетов для адреса {address}")
            return None

        current_data = get_aggregated_month_data(conn, account_ids, year, month)
        if not current_data:
            logger.warning(f"Нет данных за {year}-{month:02d} для адреса {address}")
            return None

        prev_data, prev_ym_tuple = get_previous_month_aggregated(conn, account_ids, year, month)
        prev_ym = f"{prev_ym_tuple[0]}-{prev_ym_tuple[1]:02d}" if prev_ym_tuple else None

        last_12_data = get_last_12_months_aggregated(conn, account_ids, year, month)

        current_ym = f"{year}-{month:02d}"
        prompt = build_prompt(address, current_data, prev_data, last_12_data, current_ym, prev_ym)

        response, tokens = call_ai(config, prompt)
        if response is None:
            logger.error(f"Не удалось получить ответ для адреса {address} за {current_ym}")
            return None

        cursor = conn.cursor()
        cursor.execute("SELECT id FROM periods WHERE year = ? AND month = ?", (year, month))
        period_row = cursor.fetchone()
        if not period_row:
            logger.error(f"Период {year}-{month:02d} не найден")
            return None
        period_id = period_row['id']

        save_address_analysis(conn, address, period_id, prompt, response, config.get('openrouter', 'model'), tokens)
        logger.info(f"Анализ для адреса {address} за {current_ym} сохранён (токенов: {tokens})")

        return {
            'address': address,
            'year': year,
            'month': month,
            'response': response,
            'model': config.get('openrouter', 'model'),
            'tokens': tokens
        }
    finally:
        if should_close:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Анализ ЕПД по адресам с помощью ИИ')
    parser.add_argument('--limit', type=int, help='Ограничить количество обрабатываемых адресо-месяцев')
    args = parser.parse_args()

    config = load_config()
    global logger
    logger = setup_logging(config)

    conn = get_db_connection(config.get('paths', 'db_path'))

    missing = get_addresses_missing_analysis(conn)
    if not missing:
        logger.info("Нет адресов и месяцев, требующих анализа.")
        conn.close()
        return

    logger.info(f"Найдено {len(missing)} адресо-месяцев для анализа.")
    if args.limit:
        missing = missing[:args.limit]
        logger.info(f"Обрабатываем первые {args.limit} (самых новых).")

    for row in missing:
        address = row['address']
        year = row['year']
        month = row['month']
        logger.info(f"Обработка адреса '{address}' за {year}-{month:02d}...")
        result = analyze_address_month(address, year, month, config, conn=conn)
        if result:
            logger.info(f"Анализ для адреса {address} за {year}-{month:02d} выполнен успешно")
        else:
            logger.error(f"Ошибка анализа для адреса {address} за {year}-{month:02d}")
        time.sleep(2)

    conn.close()
    logger.info("Обработка завершена.")

if __name__ == '__main__':
    main()
