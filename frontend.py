#!/usr/bin/env python3
import sqlite3
import configparser
import logging
import os
from flask import Flask, render_template, request, jsonify

def setup_logging(config):
    log_dir = config.get('logging', 'log_dir')
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

logger = logging.getLogger(__name__)

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('config.ini')
DB_PATH = config.get('paths', 'db_path')
PORT = config.getint('frontend', 'port')
STATIC_DIR = config.get('frontend', 'static_dir', fallback='static')
DEBUG = config.getboolean('frontend', 'debug', fallback=False)

# Настройка логирования (вызываем после загрузки конфига)
logger = setup_logging(config)

app = Flask(__name__, static_folder=STATIC_DIR)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

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

    # Получаем ID периодов в заданном диапазоне
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
        SELECT a.response, a.model, a.created_at
        FROM address_analysis a
        JOIN periods p ON a.period_id = p.id
        WHERE a.address = ? AND p.year = ? AND p.month = ?
        ORDER BY a.created_at DESC
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=DEBUG)
