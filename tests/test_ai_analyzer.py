import pytest
from ai_analyzer import get_aggregated_month_data, analyze_address_month
from conftest import temp_db, mock_ai_response
import sqlite3

def test_get_aggregated_month_data(temp_db):
    # Заполняем БД тестовыми данными
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc1", "январь", 2025))
    account_id = conn.lastrowid
    conn.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                 (account_id, "ХВС", 150.0))
    conn.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                 (account_id, "ГВС", 200.0))
    conn.commit()
    conn.close()

    # Вызов функции
    data = get_aggregated_month_data(2025, 1, ["acc1"])  # месяц январь = 1
    assert "addr1" in data
    assert data["addr1"]["total"] == 350.0
    assert data["addr1"]["services"]["ХВС"] == 150.0
    assert data["addr1"]["services"]["ГВС"] == 200.0

def test_analyze_address_month(temp_db, mock_ai_response):
    # Подготовка данных
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc1", "январь", 2025))
    account_id = conn.lastrowid
    conn.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                 (account_id, "ХВС", 150.0))
    conn.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (?,?,?)",
                 (account_id, "ГВС", 200.0))
    conn.commit()
    conn.close()

    # Запускаем анализ
    result = analyze_address_month("addr1", 2025, 1)
    assert result is not None
    assert "Анализ выполнен" in result  # или проверяем, что функция вернула текст

    # Проверяем, что mock был вызван с правильными параметрами
    mock_ai_response.assert_called_once()
    args, kwargs = mock_ai_response.call_args
    assert "api.openrouter.ai" in kwargs.get('url', '')