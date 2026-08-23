import json
from conftest import client, temp_db
import sqlite3

def test_index(client):
    response = client.get('/')
    assert response.status_code == 200
    # Можно проверить наличие ключевых фраз в HTML

def test_get_addresses(client, temp_db):
    # Заполняем БД тестовыми адресами
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc1", "январь", 2025))
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr2", "acc2", "февраль", 2025))
    conn.commit()
    conn.close()

    response = client.get('/api/addresses')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "addr1" in data
    assert "addr2" in data

def test_get_periods(client, temp_db):
    # Добавляем данные
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc1", "январь", 2025))
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc2", "февраль", 2025))
    conn.commit()
    conn.close()

    response = client.get('/api/periods?address=addr1')
    assert response.status_code == 200
    data = json.loads(response.data)
    # Ожидаем список периодов с уникальными (месяц, год)
    assert len(data) == 2
    assert {"month": "январь", "year": 2025} in data
    assert {"month": "февраль", "year": 2025} in data

def test_get_charges(client, temp_db):
    # Добавляем данные
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

    response = client.get('/api/charges?address=addr1&month=январь&year=2025')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "charges" in data
    assert len(data["charges"]) == 2
    assert data["charges"][0]["service_name"] == "ХВС"