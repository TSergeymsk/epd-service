import pytest
import sqlite3
from ai_analyzer import get_aggregated_month_data, analyze_address_month

def create_test_schema(conn):
    """Создаёт таблицы, необходимые для тестов ai_analyzer."""
    conn.executescript('''
        -- Удаляем старые таблицы, чтобы пересоздать с правильной структурой
        DROP TABLE IF EXISTS charges;
        DROP TABLE IF EXISTS services;
        DROP TABLE IF EXISTS periods;
        
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
    ''')

def test_get_aggregated_month_data(temp_db):
    conn = sqlite3.connect(temp_db)
    # Пересоздаём таблицы с правильной схемой
    create_test_schema(conn)
    cur = conn.cursor()
    
    # Добавляем тестовые услуги и период
    cur.execute("INSERT INTO services (name) VALUES ('ХВС')")
    cur.execute("INSERT INTO services (name) VALUES ('ГВС')")
    cur.execute("INSERT INTO periods (year, month) VALUES (2025, 1)")
    
    # Получаем id
    cur.execute("SELECT id FROM services WHERE name='ХВС'")
    service_id_hvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM services WHERE name='ГВС'")
    service_id_gvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM periods WHERE year=2025 AND month=1")
    period_id = cur.fetchone()[0]
    
    # Создаём аккаунт (таблица accounts уже существует от init_db)
    cur.execute("INSERT INTO accounts (address, account_number) VALUES (?,?)",
                ("addr1", "acc1"))
    account_id = cur.lastrowid
    
    # Добавляем начисления
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_hvs, period_id, 150.0, 1))
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_gvs, period_id, 200.0, 1))
    conn.commit()
    
    # Вызов тестируемой функции
    data = get_aggregated_month_data(conn, ["acc1"], 2025, 1)
    conn.close()
    
    assert "addr1" in data
    assert data["addr1"]["total"] == 350.0
    assert data["addr1"]["services"]["ХВС"] == 150.0
    assert data["addr1"]["services"]["ГВС"] == 200.0

def test_analyze_address_month(temp_db):
    conn = sqlite3.connect(temp_db)
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
    
    # Подготавливаем конфиг для функции
    config = {'paths': {'db_path': temp_db}}
    # Вызов с правильными аргументами: address, year, month, config, conn=None
    result = analyze_address_month("addr1", 2025, 1, config, conn=None)
    conn.close()
    assert result is not None