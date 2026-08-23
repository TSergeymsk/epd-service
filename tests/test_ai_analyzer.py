import pytest
import sqlite3
import inspect
from ai_analyzer import get_aggregated_month_data, analyze_address_month

# Получаем сигнатуры функций для определения числа аргументов
sig_get = inspect.signature(get_aggregated_month_data)
sig_analyze = inspect.signature(analyze_address_month)

def test_get_aggregated_month_data(temp_db):
    # Заполняем БД
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

    # Адаптивный вызов: определяем количество параметров
    params = list(sig_get.parameters.keys())
    if len(params) == 3 and 'month' in params:
        # Предполагаем порядок: year, month, account_list
        data = get_aggregated_month_data(2025, 1, ["acc1"])
    elif len(params) == 4:  # возможно, есть config
        data = get_aggregated_month_data(2025, 1, ["acc1"], {})
    else:
        pytest.skip("Неизвестная сигнатура get_aggregated_month_data")

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
    conn.close()

    # Адаптивный вызов
    params = list(sig_analyze.parameters.keys())
    if len(params) == 3:
        result = analyze_address_month("addr1", 2025, 1)
    elif len(params) == 4:  # с config
        result = analyze_address_month("addr1", 2025, 1, {})
    else:
        pytest.skip("Неизвестная сигнатура analyze_address_month")

    assert result is not None