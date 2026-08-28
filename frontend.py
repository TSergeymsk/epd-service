#!/usr/bin/env python3
"""
Веб-интерфейс для просмотра данных ЕПД и управления AI-анализом.
"""
import sqlite3
import configparser
import logging
import os
import json
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory

# Загрузка конфигурации
def get_config_path():
    env_path = os.environ.get('EPD_CONFIG')
    if env_path:
        return env_path
    return str(Path(__file__).parent / 'config.ini')

def load_config():
    config = configparser.ConfigParser()
    config.read(get_config_path())
    return config

def setup_logging():
    config = load_config()
    log_dir = '/tmp/logs'
    try:
        if config.has_section('logging') and config.has_option('logging', 'log_dir'):
            log_dir = config.get('logging', 'log_dir')
        else:
            log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    except Exception:
        pass
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'frontend.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

_logger = None
def get_logger():
    global _logger
    if _logger is None:
        _logger = setup_logging()
    return _logger

config = load_config()

def get_db_path():
    if config.has_section('paths') and config.has_option('paths', 'db_path'):
        return config.get('paths', 'db_path')
    return ':memory:'

def get_port():
    if config.has_section('frontend') and config.has_option('frontend', 'port'):
        return config.getint('frontend', 'port')
    return 5000

def get_static_dir():
    if config.has_section('frontend') and config.has_option('frontend', 'static_dir'):
        return config.get('frontend', 'static_dir')
    return 'static'

def get_debug():
    if config.has_section('frontend') and config.has_option('frontend', 'debug'):
        return config.getboolean('frontend', 'debug')
    return False

DB_PATH = get_db_path()
PORT = get_port()
STATIC_DIR = get_static_dir()
DEBUG = get_debug()

app = Flask(__name__, static_folder=STATIC_DIR)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------ Вспомогательные функции ------------------
def get_period_id(conn, year, month):
    cur = conn.execute("SELECT id FROM periods WHERE year = ? AND month = ?", (year, month))
    row = cur.fetchone()
    return row['id'] if row else None

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
    # Предыдущий месяц
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

    # Динамика за 12 месяцев
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

def generate_telegram_message(conn, address, period_id):
    cur = conn.execute("""
        SELECT response_text, model
        FROM llm_requests
        WHERE address = ? AND period_id = ? AND status = 'success'
        ORDER BY updated_at DESC
        LIMIT 1
    """, (address, period_id))
    llm_row = cur.fetchone()
    ai_response = llm_row['response_text'] if llm_row else None

    current, _, _, _ = get_aggregated_data(conn, address, period_id)
    total = sum(item['amount_due'] for item in current) if current else 0

    cur = conn.execute("SELECT year, month FROM periods WHERE id = ?", (period_id,))
    period = cur.fetchone()
    period_str = f"{period['year']}-{period['month']:02d}"

    lines = []
    lines.append("<b>Анализ ЕПД</b>")
    lines.append(f"🏠 <b>Адрес:</b> {address}")
    lines.append(f"📅 <b>Период:</b> {period_str}")
    lines.append("")
    lines.append(f"💰 <b>Итого: {total:,.2f} руб.</b>")
    if current:
        lines.append("В т.ч.:")
        for item in sorted(current, key=lambda x: x['amount_due'], reverse=True):
            lines.append(f"🔹 <b>{item['name']}:</b> {item['amount_due']:,.2f} руб. (кол-во: {item['quantity']:.3f})")
    lines.append("")
    if ai_response:
        lines.append("<b>Анализ:</b>")
        lines.append(ai_response)
    else:
        lines.append("<i>Анализ не проведён</i>")
    return "\n".join(lines)

# ------------------ API для очереди задач ------------------
@app.route('/api/queue_status')
def queue_status():
    conn = get_db()
    # AI запросы
    cur = conn.execute("SELECT COUNT(*) as count FROM llm_requests WHERE status = 'pending'")
    llm_pending = cur.fetchone()['count']
    # Telegram сообщения
    cur = conn.execute("SELECT COUNT(*) as count FROM telegram_messages WHERE status = 'pending'")
    tg_pending = cur.fetchone()['count']
    conn.close()
    return jsonify({
        'llm_pending': llm_pending,
        'tg_pending': tg_pending
    })

# ------------------ API для управления правилами фильтрации ------------------
@app.route('/api/filter_rules')
def filter_rules():
    conn = get_db()
    cur = conn.execute("SELECT * FROM filter_rules ORDER BY priority DESC")
    rules = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(rules)

@app.route('/api/filter_rules', methods=['POST'])
def add_filter_rule():
    data = request.get_json()
    required = ['name', 'from_pattern', 'to_pattern', 'subject_pattern', 'parser_script']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'Missing field: {field}'}), 400
    conn = get_db()
    cur = conn.execute("""
        INSERT INTO filter_rules (name, from_pattern, to_pattern, subject_pattern, parser_script, enabled, priority)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data['name'],
        data['from_pattern'],
        data['to_pattern'],
        data['subject_pattern'],
        data['parser_script'],
        data.get('enabled', True),
        data.get('priority', 0)
    ))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return jsonify({'id': new_id, 'status': 'ok'}), 201

@app.route('/api/filter_rules/<int:rule_id>', methods=['PUT'])
def update_filter_rule(rule_id):
    data = request.get_json()
    conn = get_db()
    cur = conn.execute("SELECT id FROM filter_rules WHERE id = ?", (rule_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({'error': 'Rule not found'}), 404
    conn.execute("""
        UPDATE filter_rules
        SET name = ?, from_pattern = ?, to_pattern = ?, subject_pattern = ?, parser_script = ?, enabled = ?, priority = ?
        WHERE id = ?
    """, (
        data.get('name'),
        data.get('from_pattern'),
        data.get('to_pattern'),
        data.get('subject_pattern'),
        data.get('parser_script'),
        data.get('enabled', True),
        data.get('priority', 0),
        rule_id
    ))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/filter_rules/<int:rule_id>', methods=['DELETE'])
def delete_filter_rule(rule_id):
    conn = get_db()
    cur = conn.execute("SELECT id FROM filter_rules WHERE id = ?", (rule_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({'error': 'Rule not found'}), 404
    conn.execute("DELETE FROM filter_rules WHERE id = ?", (rule_id,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})

# ------------------ Маршруты (существующие) ------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/addresses')
def addresses():
    conn = get_db()
    cur = conn.execute("SELECT DISTINCT TRIM(address) as address FROM accounts WHERE address IS NOT NULL AND TRIM(address) != '' ORDER BY address")
    addresses = [row['address'] for row in cur.fetchall()]
    conn.close()
    return jsonify(addresses)

@app.route('/api/accounts_by_address')
def accounts_by_address():
    address = request.args.get('address')
    if not address:
        return jsonify([])
    conn = get_db()
    cur = conn.execute(
        "SELECT id, account_number FROM accounts WHERE address = ? ORDER BY account_number",
        (address,)
    )
    accounts = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(accounts)

@app.route('/api/services')
def services():
    conn = get_db()
    cur = conn.execute("SELECT id, name, unit FROM services ORDER BY name")
    services = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(services)

@app.route('/api/periods')
def periods():
    account_ids = request.args.getlist('account_ids', type=int)
    if not account_ids:
        return jsonify([])

    placeholders = ','.join('?' * len(account_ids))
    conn = get_db()
    query = f"""
        SELECT DISTINCT p.year, p.month
        FROM periods p
        JOIN charges c ON c.period_id = p.id
        WHERE c.account_id IN ({placeholders})
        ORDER BY p.year DESC, p.month DESC
    """
    cur = conn.execute(query, account_ids)
    periods = [{"year": row["year"], "month": row["month"]} for row in cur.fetchall()]
    conn.close()
    return jsonify(periods)

@app.route('/api/data')
def data():
    account_ids = request.args.getlist('account_ids', type=int)
    start_year = request.args.get('start_year', type=int)
    start_month = request.args.get('start_month', type=int)
    end_year = request.args.get('end_year', type=int)
    end_month = request.args.get('end_month', type=int)
    service_ids = request.args.getlist('service_ids', type=int)

    if not account_ids or not all([start_year, start_month, end_year, end_month]):
        return jsonify({"error": "Missing parameters"}), 400

    conn = get_db()

    cur = conn.execute("""
        SELECT id, year, month
        FROM periods
        WHERE (year > ? OR (year = ? AND month >= ?))
          AND (year < ? OR (year = ? AND month <= ?))
        ORDER BY year, month
    """, (start_year, start_year, start_month, end_year, end_year, end_month))
    periods = cur.fetchall()
    period_ids = [p['id'] for p in periods]
    month_labels = [f"{p['year']}-{p['month']:02d}" for p in periods]

    if not period_ids:
        return jsonify({"months": [], "totals": [], "services": {}, "service_names": []})

    if not service_ids:
        cur = conn.execute("SELECT id FROM services")
        service_ids = [row['id'] for row in cur.fetchall()]
        if not service_ids:
            return jsonify({"months": month_labels, "totals": [], "services": {}, "service_names": []})

    account_placeholders = ','.join('?' * len(account_ids))
    period_placeholders = ','.join('?' * len(period_ids))
    service_placeholders = ','.join('?' * len(service_ids))

    query = f"""
        SELECT
            p.year,
            p.month,
            s.name,
            SUM(c.amount_due) as total_amount,
            SUM(c.quantity) as total_quantity
        FROM charges c
        JOIN periods p ON c.period_id = p.id
        JOIN services s ON c.service_id = s.id
        WHERE c.account_id IN ({account_placeholders})
          AND c.period_id IN ({period_placeholders})
          AND c.service_id IN ({service_placeholders})
        GROUP BY p.year, p.month, s.name
        ORDER BY p.year, p.month, s.name
    """
    params = account_ids + period_ids + service_ids
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    month_data = {label: {} for label in month_labels}
    for row in rows:
        label = f"{row['year']}-{row['month']:02d}"
        total_amount = row['total_amount']
        total_quantity = row['total_quantity'] or 0
        avg_tariff = total_amount / total_quantity if total_quantity else 0
        month_data[label][row['name']] = {
            'amount': total_amount,
            'quantity': total_quantity,
            'tariff': avg_tariff
        }

    totals = [sum(month_data[label][name]['amount'] for name in month_data[label]) for label in month_labels]

    service_names = sorted({row['name'] for row in rows})
    services_data = {}
    for name in service_names:
        amounts = []
        quantities = []
        tariffs = []
        for label in month_labels:
            item = month_data[label].get(name, {'amount': 0, 'quantity': 0, 'tariff': 0})
            amounts.append(item['amount'])
            quantities.append(item['quantity'])
            tariffs.append(item['tariff'])
        services_data[name] = {
            'amounts': amounts,
            'quantities': quantities,
            'tariffs': tariffs
        }

    return jsonify({
        "months": month_labels,
        "totals": totals,
        "services": services_data,
        "service_names": service_names
    })

@app.route('/api/analysis_for_month')
def analysis_for_month():
    address = request.args.get('address')
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)

    if not address or not year or not month:
        return jsonify({"error": "Missing parameters"}), 400

    conn = get_db()
    cur = conn.execute("""
        SELECT a.response_text as response, a.model, a.updated_at as created_at
        FROM llm_requests a
        JOIN periods p ON a.period_id = p.id
        WHERE a.address = ? AND p.year = ? AND p.month = ? AND a.status = 'success'
        ORDER BY a.updated_at DESC
        LIMIT 1
    """, (address, year, month))
    row = cur.fetchone()
    conn.close()
    if row:
        return jsonify({
            "response": row['response'],
            "model": row['model'],
            "created_at": row['created_at']
        })
    else:
        return jsonify({"error": "No analysis found"}), 404

@app.route('/api/llm_details')
def llm_details():
    address = request.args.get('address')
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    if not address or not year or not month:
        return jsonify({"error": "Missing parameters"}), 400

    conn = get_db()
    period_id = get_period_id(conn, year, month)
    if not period_id:
        conn.close()
        return jsonify({"error": "Period not found"}), 404

    cur = conn.execute("""
        SELECT id, model, provider, temperature, max_tokens, prompt_template,
               request_payload, response_text, tokens_used, status, attempts, last_error,
               created_at, updated_at
        FROM llm_requests
        WHERE address = ? AND period_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (address, period_id))
    row = cur.fetchone()
    conn.close()
    if row:
        return jsonify({
            "id": row['id'],
            "model": row['model'],
            "provider": row['provider'],
            "temperature": row['temperature'],
            "max_tokens": row['max_tokens'],
            "prompt_template": row['prompt_template'],
            "request_payload": row['request_payload'],
            "response_text": row['response_text'],
            "tokens_used": row['tokens_used'],
            "status": row['status'],
            "attempts": row['attempts'],
            "last_error": row['last_error'],
            "created_at": row['updated_at']
        })
    else:
        return jsonify({"error": "No LLM request found"}), 404

@app.route('/api/retry_ai', methods=['POST'])
def retry_ai():
    data = request.get_json()
    address = data.get('address')
    year = data.get('year')
    month = data.get('month')
    if not address or not year or not month:
        return jsonify({"error": "Missing parameters"}), 400

    conn = get_db()
    period_id = get_period_id(conn, year, month)
    if not period_id:
        conn.close()
        return jsonify({"error": "Period not found"}), 404

    cur = conn.execute("""
        SELECT id FROM llm_requests
        WHERE address = ? AND period_id = ?
        ORDER BY created_at DESC LIMIT 1
    """, (address, period_id))
    existing = cur.fetchone()
    if existing:
        conn.execute("""
            UPDATE llm_requests
            SET status = 'pending', attempts = 0, last_error = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (existing['id'],))
    else:
        conn.execute("""
            INSERT INTO llm_requests (address, period_id, model, status)
            VALUES (?, ?, ?, 'pending')
        """, (address, period_id, 'unknown'))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route('/api/retry_telegram', methods=['POST'])
def retry_telegram():
    data = request.get_json()
    address = data.get('address')
    year = data.get('year')
    month = data.get('month')
    if not address or not year or not month:
        return jsonify({"error": "Missing parameters"}), 400

    conn = get_db()
    period_id = get_period_id(conn, year, month)
    if not period_id:
        conn.close()
        return jsonify({"error": "Period not found"}), 404

    cur = conn.execute("""
        SELECT id FROM llm_requests
        WHERE address = ? AND period_id = ? AND status = 'success'
    """, (address, period_id))
    if not cur.fetchone():
        conn.close()
        return jsonify({"error": "No successful LLM response for this period"}), 400

    cur = conn.execute("""
        SELECT id FROM telegram_messages
        WHERE address = ? AND period_id = ?
        ORDER BY created_at DESC LIMIT 1
    """, (address, period_id))
    existing = cur.fetchone()
    if existing:
        conn.execute("""
            UPDATE telegram_messages
            SET status = 'pending', attempts = 0, last_error = NULL, sent_at = NULL
            WHERE id = ?
        """, (existing['id'],))
    else:
        msg_text = generate_telegram_message(conn, address, period_id)
        conn.execute("""
            INSERT INTO telegram_messages (address, period_id, message_text, status)
            VALUES (?, ?, ?, 'pending')
        """, (address, period_id, msg_text))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

# ------------------ favicon ------------------
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

# ------------------ Запуск ------------------
if __name__ == '__main__':
    logger = get_logger()
    logger.info("Запуск фронтенда")
    app.run(host='0.0.0.0', port=PORT, debug=DEBUG)
