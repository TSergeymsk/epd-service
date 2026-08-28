#!/usr/bin/env python3
"""
Оркестратор для AI-анализа и отправки Telegram-сообщений.
Отправляет сообщение как обычный текст (без форматирования).
"""
import os
import sys
import time
import json
import requests
import yaml
import re
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_db_connection, setup_logging, load_ai_config, load_ini_config

logger = setup_logging('orchestrator')

# Конфигурация
INI_CONFIG = load_ini_config()
AI_CONFIG = load_ai_config()

MAX_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 5  # увеличен для предотвращения 429

def clean_markdown(text):
    """Удаляет Markdown-разметку из текста."""
    if not text:
        return text
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'\*(.*?)\*', r'\1', text)
    text = re.sub(r'_(.*?)_', r'\1', text)
    text = re.sub(r'~(.*?)~', r'\1', text)
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'`(.*?)`', r'\1', text)
    text = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', text)
    text = re.sub(r'^(\s*[-*_]{3,}\s*)$', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def is_ai_configured():
    ai_cfg = AI_CONFIG.get('ai', {})
    if not ai_cfg.get('url') or not ai_cfg.get('model'):
        return False
    api_key = INI_CONFIG.get('openrouter', 'api_key')
    if not api_key or api_key.strip() == '' or api_key == 'local_ollama':
        return False
    return True

def get_pending_llm_requests(conn):
    cur = conn.execute("""
        SELECT id, address, period_id, attempts
        FROM llm_requests
        WHERE status IN ('pending', 'failed') AND attempts < ?
        ORDER BY created_at
    """, (MAX_RETRIES,))
    return cur.fetchall()

def get_pending_telegram_messages(conn):
    cur = conn.execute("""
        SELECT id, address, period_id, message_text, attempts
        FROM telegram_messages
        WHERE status IN ('pending', 'failed') AND attempts < ?
        ORDER BY created_at
    """, (MAX_RETRIES,))
    return cur.fetchall()

def get_period_details(conn, period_id):
    cur = conn.execute("SELECT year, month FROM periods WHERE id = ?", (period_id,))
    return cur.fetchone()

def get_aggregated_data(conn, address, period_id):
    cur = conn.execute("SELECT id FROM accounts WHERE address = ?", (address,))
    account_ids = [row['id'] for row in cur.fetchall()]
    if not account_ids:
        return [], None, None, {}

    placeholders = ','.join('?' * len(account_ids))
    query = f"""
        SELECT s.name,
               SUM(c.amount_due) as total_amount,
               SUM(c.quantity) as total_quantity
        FROM charges c
        JOIN services s ON c.service_id = s.id
        WHERE c.account_id IN ({placeholders}) AND c.period_id = ?
        GROUP BY s.name
        ORDER BY s.name
    """
    cur = conn.execute(query, account_ids + [period_id])
    rows = cur.fetchall()
    current = []
    for row in rows:
        total_q = row['total_quantity'] or 0
        avg_tariff = row['total_amount'] / total_q if total_q > 0 else 0
        current.append({
            'name': row['name'],
            'quantity': total_q,
            'tariff': avg_tariff,
            'amount_due': row['total_amount']
        })

    cur = conn.execute(f"""
        SELECT p.id, p.year, p.month
        FROM periods p
        JOIN charges c ON c.period_id = p.id
        WHERE c.account_id IN ({placeholders}) AND p.id < ?
        ORDER BY p.year DESC, p.month DESC
        LIMIT 1
    """, account_ids + [period_id])
    prev_period = cur.fetchone()
    prev_data = None
    prev_ym = None
    if prev_period:
        cur2 = conn.execute(f"""
            SELECT s.name,
                   SUM(c.amount_due) as total_amount,
                   SUM(c.quantity) as total_quantity
            FROM charges c
            JOIN services s ON c.service_id = s.id
            WHERE c.account_id IN ({placeholders}) AND c.period_id = ?
            GROUP BY s.name
        """, account_ids + [prev_period['id']])
        prev_rows = cur2.fetchall()
        prev_data = []
        for row in prev_rows:
            total_q = row['total_quantity'] or 0
            avg_tariff = row['total_amount'] / total_q if total_q > 0 else 0
            prev_data.append({
                'name': row['name'],
                'quantity': total_q,
                'tariff': avg_tariff,
                'amount_due': row['total_amount']
            })
        prev_ym = f"{prev_period['year']}-{prev_period['month']:02d}"

    last_12_data = {}
    months_query = f"""
        SELECT DISTINCT p.year, p.month
        FROM periods p
        JOIN charges c ON c.period_id = p.id
        WHERE c.account_id IN ({placeholders}) AND p.id < ?
        ORDER BY p.year DESC, p.month DESC
        LIMIT 12
    """
    cur = conn.execute(months_query, account_ids + [period_id])
    months = cur.fetchall()
    if months:
        month_conditions = " OR ".join(["(p.year = ? AND p.month = ?)"] * len(months))
        month_params = []
        for m in months:
            month_params.append(m['year'])
            month_params.append(m['month'])
        query_dyn = f"""
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
        cur = conn.execute(query_dyn, account_ids + month_params)
        rows = cur.fetchall()
        for row in rows:
            ym = f"{row['year']}-{row['month']:02d}"
            if ym not in last_12_data:
                last_12_data[ym] = {}
            last_12_data[ym][row['name']] = row['total_amount']

    return current, prev_data, prev_ym, last_12_data

def format_prompt(address, period_ym, current_data, prev_data, prev_ym, last_12_data):
    prompts_conf = AI_CONFIG.get('prompts', {})
    template = prompts_conf.get('analysis')
    if not template:
        logger.error("Шаблон промта (prompts.analysis) не задан в ai_config.yaml")
        return None

    total_current = sum(item['amount_due'] for item in current_data) if current_data else 0
    current_lines = [f"- Итого к оплате: {total_current:.2f} руб."]
    current_lines.append("- Структура услуг:")
    for r in current_data:
        current_lines.append(f"  * {r['name']}: количество {r['quantity']:.3f}, средний тариф {r['tariff']:.2f} руб., сумма {r['amount_due']:.2f} руб.")
    current_str = "\n".join(current_lines)

    if prev_data:
        total_prev = sum(item['amount_due'] for item in prev_data)
        prev_lines = [f"Данные за предыдущий месяц {prev_ym}:"]
        prev_lines.append(f"- Итого к оплате: {total_prev:.2f} руб.")
        prev_lines.append("- Структура услуг:")
        for r in prev_data:
            prev_lines.append(f"  * {r['name']}: количество {r['quantity']:.3f}, средний тариф {r['tariff']:.2f} руб., сумма {r['amount_due']:.2f} руб.")
        prev_str = "\n".join(prev_lines)
    else:
        prev_str = "Данные за предыдущий месяц отсутствуют."

    dyn_lines = []
    if last_12_data:
        dyn_lines.append(f"Динамика за последние 12 месяцев (доступно {len(last_12_data)} месяцев):")
        for ym, services in sorted(last_12_data.items()):
            dyn_lines.append(f"  {ym}:")
            for name, total in services.items():
                dyn_lines.append(f"    {name}: {total:.2f} руб.")
    else:
        dyn_lines.append("Данных за последние 12 месяцев недостаточно.")
    dyn_str = "\n".join(dyn_lines)

    return template.format(
        address=address,
        period=period_ym,
        current_data=current_str,
        prev_data=prev_str,
        months_count=len(last_12_data),
        dynamics=dyn_str
    )

def call_ai_api(prompt, retries=3):
    ai_cfg = AI_CONFIG.get('ai', {})
    url = ai_cfg.get('url')
    model = ai_cfg.get('model')
    api_key = INI_CONFIG.get('openrouter', 'api_key')
    timeout = ai_cfg.get('timeout')
    temperature = ai_cfg.get('temperature')
    max_tokens = ai_cfg.get('max_tokens')

    if not all([url, model, api_key, timeout is not None, temperature is not None, max_tokens is not None]):
        logger.error("Не все параметры AI заданы в конфиге")
        return None, None

    headers = {"Content-Type": "application/json"}
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
        "max_tokens": max_tokens,
        "temperature": temperature
    }

    extra = ai_cfg.get('extra_params')
    if extra:
        payload.update(extra)

    for attempt in range(retries):
        try:
            logger.info(f"Отправка запроса к {url}, модель {model}, длина промпта: {len(prompt)} символов (попытка {attempt+1}/{retries})")
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                wait = int(retry_after) if retry_after else 30
                logger.warning(f"Получен 429 Too Many Requests. Ожидание {wait} секунд перед повторной попыткой.")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            if 'choices' in data and data['choices']:
                answer = data['choices'][0]['message']['content']
                tokens = data.get('usage', {}).get('total_tokens')
                return answer, tokens
            else:
                logger.error(f"Неожиданный формат ответа: {data}")
                return None, None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                continue
            logger.exception(f"Ошибка при вызове AI API: {e}")
            return None, None
        except Exception as e:
            logger.exception(f"Ошибка при вызове AI API: {e}")
            return None, None

    logger.error(f"Не удалось получить ответ от AI после {retries} попыток")
    return None, None

def process_llm_requests(conn):
    if not is_ai_configured():
        logger.warning("AI не настроен. Пропускаем все запросы к LLM.")
        conn.execute("""
            UPDATE llm_requests
            SET status = 'failed', last_error = 'AI не настроен', updated_at = CURRENT_TIMESTAMP
            WHERE status = 'pending'
        """)
        conn.commit()
        return

    requests_list = get_pending_llm_requests(conn)
    if not requests_list:
        return
    logger.info(f"Найдено {len(requests_list)} запросов к LLM для обработки")
    for req in requests_list:
        req_id = req['id']
        address = req['address']
        period_id = req['period_id']
        attempts = req['attempts']

        period = get_period_details(conn, period_id)
        if not period:
            logger.error(f"Период {period_id} не найден, пропускаем")
            continue
        period_ym = f"{period['year']}-{period['month']:02d}"

        current, prev_data, prev_ym, last_12_data = get_aggregated_data(conn, address, period_id)
        if not current:
            logger.warning(f"Нет данных для адреса {address} за {period_ym}, пропускаем")
            conn.execute("UPDATE llm_requests SET status = 'failed', last_error = 'Нет данных для анализа' WHERE id = ?", (req_id,))
            conn.commit()
            continue

        prompt = format_prompt(address, period_ym, current, prev_data, prev_ym, last_12_data)
        if prompt is None:
            logger.error(f"Не удалось сформировать промт для {address} за {period_ym}, пропускаем")
            conn.execute("UPDATE llm_requests SET status = 'failed', last_error = 'Ошибка формирования промта' WHERE id = ?", (req_id,))
            conn.commit()
            continue

        conn.execute("UPDATE llm_requests SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?", (req_id,))
        conn.commit()

        response_text, tokens = call_ai_api(prompt, retries=3)
        if response_text is not None and response_text.strip() != '':
            model = AI_CONFIG.get('ai', {}).get('model', 'unknown')
            provider = AI_CONFIG.get('ai', {}).get('provider', 'unknown')
            conn.execute("""
                UPDATE llm_requests
                SET status = 'success',
                    response_text = ?,
                    tokens_used = ?,
                    model = ?,
                    provider = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    last_error = NULL
                WHERE id = ?
            """, (response_text, tokens, model, provider, req_id))
            conn.commit()
            logger.info(f"Запрос {req_id} успешно обработан, токенов: {tokens}, длина ответа: {len(response_text)}")
        else:
            new_attempts = attempts + 1
            error_msg = "Ответ от AI пустой" if response_text is None else "Получен пустой ответ"
            if new_attempts >= MAX_RETRIES:
                status = 'retry_limit'
                error = f"Достигнут лимит попыток ({MAX_RETRIES}): {error_msg}"
            else:
                status = 'failed'
                error = error_msg
            conn.execute("""
                UPDATE llm_requests
                SET status = ?, attempts = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (status, new_attempts, error, req_id))
            conn.commit()
            logger.error(f"Запрос {req_id} не удался: {error}, попытка {new_attempts}/{MAX_RETRIES}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

# --- Генерация сообщения как обычный текст (без HTML) ---
def generate_telegram_message_for_period(conn, address, period_id):
    period = get_period_details(conn, period_id)
    period_str = f"{period['year']}-{period['month']:02d}"

    current, _, _, _ = get_aggregated_data(conn, address, period_id)
    total = sum(item['amount_due'] for item in current) if current else 0

    # Шапка (обычный текст, без HTML)
    header_lines = []
    header_lines.append("🏠 Анализ ЕПД")
    header_lines.append(f"Адрес: {address}")
    header_lines.append(f"Период: {period_str}")
    header_lines.append("")
    header_lines.append(f"💰 Итого: {total:,.2f} руб.")
    if current:
        header_lines.append("В т.ч.:")
        for item in sorted(current, key=lambda x: x['amount_due'], reverse=True):
            header_lines.append(f"🔹 {item['name']}: {item['amount_due']:,.2f} руб. (кол-во: {item['quantity']:.3f})")
    header = "\n".join(header_lines)

    # Получаем AI-анализ
    cur = conn.execute("""
        SELECT response_text, status, last_error
        FROM llm_requests
        WHERE address = ? AND period_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (address, period_id))
    llm_row = cur.fetchone()
    ai_available = is_ai_configured() and llm_row and llm_row['status'] == 'success' and llm_row['response_text'] and llm_row['response_text'].strip() != ''

    if ai_available:
        analysis_text = llm_row['response_text']
        # Очищаем от Markdown
        analysis_text = clean_markdown(analysis_text)
        analysis = f"--- Анализ ---\n{analysis_text}"
    else:
        if not is_ai_configured():
            analysis = "AI-анализ не выполнен: отсутствуют настройки AI (провайдер, модель или API-ключ)."
        elif llm_row and llm_row['status'] in ('failed', 'retry_limit'):
            analysis = f"AI-анализ не выполнен: {llm_row['last_error']}."
        else:
            analysis = "AI-анализ не выполнен (причина неизвестна)."

    full_text = header + "\n\n" + analysis

    # Обрезаем до 4000 символов (Telegram лимит 4096)
    MAX_TOTAL = 4000
    if len(full_text) > MAX_TOTAL:
        logger.warning(f"Общее сообщение для {address} слишком длинное ({len(full_text)}), обрезаем до {MAX_TOTAL}")
        # Обрезаем по границе строки, чтобы не разрывать слова
        cut_point = full_text.rfind('\n', 0, MAX_TOTAL)
        if cut_point == -1:
            cut_point = MAX_TOTAL
        full_text = full_text[:cut_point] + "\n... (сообщение обрезано)"

    return full_text

def process_telegram_messages(conn):
    messages = get_pending_telegram_messages(conn)
    if not messages:
        return
    logger.info(f"Найдено {len(messages)} сообщений для отправки в Telegram")
    bot_token = INI_CONFIG.get('telegram', 'bot_token')
    chat_id = INI_CONFIG.get('telegram', 'chat_id')
    for msg in messages:
        msg_id = msg['id']
        address = msg['address']
        period_id = msg['period_id']
        message_text = msg['message_text']
        attempts = msg['attempts']

        if not message_text.startswith('🏠 Анализ ЕПД'):
            message_text = generate_telegram_message_for_period(conn, address, period_id)
            conn.execute("""
                UPDATE telegram_messages
                SET message_text = ?
                WHERE id = ?
            """, (message_text, msg_id))
            conn.commit()

        logger.info(f"Отправка сообщения {msg_id}, длина {len(message_text)} символов")

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message_text
            # Без parse_mode – отправляем как обычный текст
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            conn.execute("""
                UPDATE telegram_messages
                SET status = 'sent', sent_at = CURRENT_TIMESTAMP, attempts = attempts + 1, last_error = NULL
                WHERE id = ?
            """, (msg_id,))
            conn.commit()
            logger.info(f"Сообщение {msg_id} отправлено в Telegram")
        except Exception as e:
            new_attempts = attempts + 1
            if new_attempts >= MAX_RETRIES:
                status = 'failed'
                error = f"Лимит попыток, последняя ошибка: {e}"
            else:
                status = 'failed'
                error = str(e)
            conn.execute("""
                UPDATE telegram_messages
                SET status = ?, attempts = ?, last_error = ?, sent_at = NULL
                WHERE id = ?
            """, (status, new_attempts, error, msg_id))
            conn.commit()
            logger.error(f"Ошибка отправки сообщения {msg_id}: {e} (попытка {new_attempts}/{MAX_RETRIES})")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

def create_telegram_messages_for_successful_llm(conn):
    cur = conn.execute("""
        SELECT DISTINCT l.address, l.period_id
        FROM llm_requests l
        LEFT JOIN telegram_messages t ON t.address = l.address AND t.period_id = l.period_id AND t.status = 'sent'
        WHERE l.status = 'success' AND l.response_text IS NOT NULL AND l.response_text != '' AND t.id IS NULL
    """)
    rows = cur.fetchall()
    for row in rows:
        address = row['address']
        period_id = row['period_id']
        msg_text = generate_telegram_message_for_period(conn, address, period_id)
        conn.execute("""
            INSERT INTO telegram_messages (address, period_id, message_text, status)
            VALUES (?, ?, ?, 'pending')
        """, (address, period_id, msg_text))
        conn.commit()
        logger.info(f"Создано сообщение для Telegram для {address} за период {period_id}")

def main():
    conn = get_db_connection()
    try:
        create_telegram_messages_for_successful_llm(conn)
        process_llm_requests(conn)
        process_telegram_messages(conn)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
