import pytest
import sqlite3
from ai_analyzer import get_aggregated_month_data, analyze_address_month

def test_get_aggregated_month_data(temp_db):
    # Подготавливаем данные в таблицах (accounts, services, periods, charges)
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    # Добавляем тестовую услугу и период (если их нет)
    cur.execute("INSERT OR IGNORE INTO services (name) VALUES (?)", ("ХВС",))
    cur.execute("INSERT OR IGNORE INTO services (name) VALUES (?)", ("ГВС",))
    cur.execute("INSERT OR IGNORE INTO periods (year, month) VALUES (?,?)", (2025, 1))
    # Получаем id
    cur.execute("SELECT id FROM services WHERE name='ХВС'")
    service_id_hvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM services WHERE name='ГВС'")
    service_id_gvs = cur.fetchone()[0]
    cur.execute("SELECT id FROM periods WHERE year=2025 AND month=1")
    period_id = cur.fetchone()[0]
    # Создаём аккаунт
    cur.execute("INSERT INTO accounts (address, account_number) VALUES (?,?)",
                ("addr1", "acc1"))
    account_id = cur.lastrowid
    # Добавляем начисления
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_hvs, period_id, 150.0, 1))
    cur.execute("INSERT INTO charges (account_id, service_id, period_id, amount_due, quantity) VALUES (?,?,?,?,?)",
                (account_id, service_id_gvs, period_id, 200.0, 1))
    conn.commit()

    # Вызов функции с правильным порядком аргументов
    data = get_aggregated_month_data(conn, ["acc1"], 2025, 1)
    conn.close()
    
    assert "addr1" in data
    assert data["addr1"]["total"] == 350.0
    assert data["addr1"]["services"]["ХВС"] == 150.0
    assert data["addr1"]["services"]["ГВС"] == 200.0

def test_analyze_address_month(temp_db):
    # Подготовка данных (аналогично)
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO services (name) VALUES (?)", ("ХВС",))
    cur.execute("INSERT OR IGNORE INTO services (name) VALUES (?)", ("ГВС",))
    cur.execute("INSERT OR IGNORE INTO periods (year, month) VALUES (?,?)", (2025, 1))
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

    # Создаём конфиг для передачи в функцию
    config = {'paths': {'db_path': temp_db}}
    # Вызов с правильными аргументами: address, year, month, config, conn=None
    result = analyze_address_month("addr1", 2025, 1, config, conn=None)
    conn.close()
    assert result is not None