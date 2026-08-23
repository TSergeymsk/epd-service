import json
import sqlite3

def test_index(client):
    response = client.get('/')
    assert response.status_code == 200

def test_get_addresses(client, temp_db):
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
    # Проверяем, что список не пуст и содержит ожидаемые периоды
    assert len(data) == 2
    # Предполагаем, что данные возвращаются в формате [{"month": "январь", "year": 2025}, ...]
    periods = [(item['month'], item['year']) for item in data]
    assert ("январь", 2025) in periods
    assert ("февраль", 2025) in periods

def test_get_charges(client, temp_db):
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

    response = client.get('/api/charges?address=addr1&month=январь&year=2025')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "charges" in data
    assert len(data["charges"]) == 2
    # Проверяем, что услуги содержатся
    services = [item["service_name"] for item in data["charges"]]
    assert "ХВС" in services
    assert "ГВС" in services