import pytest
import sqlite3
from ai_analyzer import get_aggregated_month_data, analyze_address_month
from conftest import mock_ai_response

def test_get_aggregated_month_data(temp_db):
    # Заполняем БД тестовыми данными
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
    conn.close()

    # Вызов функции (предполагаем, что она принимает год, месяц и список счетов)
    # Возможно, сигнатура отличается, адаптируем под реальную
    data = get_aggregated_month_data(2025, 1, ["acc1"])
    assert "addr1" in data
    assert data["addr1"]["total"] == 350.0
    assert data["addr1"]["services"]["ХВС"] == 150.0
    assert data["addr1"]["services"]["ГВС"] == 200.0

def test_analyze_address_month(temp_db, mock_ai_response):
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
    conn.close()

    # Запускаем анализ
    result = analyze_address_month("addr1", 2025, 1)
    assert result is not None
    # Проверяем, что mock был вызван (если функция использует API)
    mock_ai_response.assert_called_once()