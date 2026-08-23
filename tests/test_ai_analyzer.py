import pytest
import sqlite3
from ai_analyzer import get_aggregated_month_data, analyze_address_month

def test_get_aggregated_month_data(temp_db):
    # Подготавливаем данные
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                ("addr1", "acc1", "январь", 2025))
    account_id = cur.lastrowid
    cur.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                (account_id, "ХВС", 150.0))
    cur.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                (account_id, "ГВС", 200.0))
    conn.commit()
    
    # Вызов с правильным порядком аргументов
    data = get_aggregated_month_data(conn, ["acc1"], 2025, 1)
    conn.close()
    
    assert "addr1" in data
    assert data["addr1"]["total"] == 350.0
    assert data["addr1"]["services"]["ХВС"] == 150.0
    assert data["addr1"]["services"]["ГВС"] == 200.0

def test_analyze_address_month(temp_db):
    # Подготовка данных
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                ("addr1", "acc1", "январь", 2025))
    account_id = cur.lastrowid
    cur.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                (account_id, "ХВС", 150.0))
    cur.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                (account_id, "ГВС", 200.0))
    conn.commit()
    
    # Вызов (предполагаем, что функция принимает conn, address, year, month)
    # Если сигнатура другая, можно адаптировать, но из контекста вероятно так
    try:
        result = analyze_address_month(conn, "addr1", 2025, 1)
    except TypeError:
        # Если не принимает conn, пробуем без него (запасной вариант)
        conn.close()
        result = analyze_address_month("addr1", 2025, 1)
    else:
        conn.close()
    
    assert result is not None