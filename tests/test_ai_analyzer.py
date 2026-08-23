import pytest
import sqlite3
import configparser
from ai_analyzer import get_aggregated_month_data, analyze_address_month

def create_test_schema(conn):
    conn.executescript('''
        DROP TABLE IF EXISTS charges;
        DROP TABLE IF EXISTS services;
        DROP TABLE IF EXISTS periods;
        DROP TABLE IF EXISTS address_analysis;
        
        CREATE TABLE services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            month INTEGER,
            UNIQUE(year, month)
        );
        CREATE TABLE charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER,
            service_id INTEGER,
            period_id INTEGER,
            amount_due REAL,
            quantity REAL,
            FOREIGN KEY(account_id) REFERENCES accounts(id),
            FOREIGN KEY(service_id) REFERENCES services(id),
            FOREIGN KEY(period_id) REFERENCES periods(id)
        );
        CREATE TABLE address_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            period_id INTEGER,
            prompt TEXT,
            response TEXT,
            model TEXT,
            tokens_used INTEGER,
            FOREIGN KEY(period_id) REFERENCES periods(id)
        );
    ''')

def test_get_aggregated_month_data(temp_db):
    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    create_test_schema(conn)
    cur = conn.cursor()
    cur.execute("INSERT INTO services (name) VALUES ('ХВС')")
    cur.execute("INSERT INTO services (name) VALUES ('ГВС')")
    cur.execute("INSERT INTO periods (year, month) VALUES (2025, 1)")
    cur.execute("SELECT id FROM services WHERE name='ХВС'")
    service_id_hvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM services WHERE name='ГВС'")
    service_id_gvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM periods WHERE year=2025 AND month=1")
    period_id = cur.fetchone()[0]
    cur.execute("INSERT INTO accounts (address, account_number) VALUES (?,?)",
                ("addr1", "acc1"))
    account_id = cur.lastrowid
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_hvs, period_id, 150.0, 1))
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_gvs, period_id, 200.0, 1))
    conn.commit()
    data = get_aggregated_month_data(conn, [account_id], 2025, 1)
    conn.close()
    assert isinstance(data, list)
    names = [item['name'] for item in data]
    assert 'ХВС' in names
    assert 'ГВС' in names
    for item in data:
        if item['name'] == 'ХВС':
            assert item['amount_due'] == 150.0
        elif item['name'] == 'ГВС':
            assert item['amount_due'] == 200.0

def test_analyze_address_month(temp_db, mock_ai_response):
    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    create_test_schema(conn)
    cur = conn.cursor()
    cur.execute("INSERT INTO services (name) VALUES ('ХВС')")
    cur.execute("INSERT INTO services (name) VALUES ('ГВС')")
    cur.execute("INSERT INTO periods (year, month) VALUES (2025, 1)")
    cur.execute("SELECT id FROM services WHERE name='ХВС'")
    service_id_hvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM services WHERE name='ГВС'")
    service_id_gvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM periods WHERE year=2025 AND month=1")
    period_id = cur.fetchone()[0]
    cur.execute("INSERT INTO accounts (address, account_number) VALUES (?,?)",
                ("addr1", "acc1"))
    account_id = cur.lastrowid
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_hvs, period_id, 150.0, 1))
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_gvs, period_id, 200.0, 1))
    conn.commit()
    conn.close()

    config = configparser.ConfigParser()
    config['paths'] = {'db_path': temp_db}
    config['openrouter'] = {
        'api_key': 'test_key',
        'model': 'test_model',
        'url': 'https://api.openrouter.ai/v1/chat/completions'
    }
    config['ai'] = {'model': 'test_model'}

    result = analyze_address_month("addr1", 2025, 1, config, conn=None)
    assert result is not None