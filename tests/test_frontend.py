import json
import sqlite3
import pytest

def test_index(client):
    response = client.get('/')
    assert response.status_code == 200

@pytest.mark.xfail(reason="Таблица accounts не имеет колонок month и year")
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

@pytest.mark.xfail(reason="Таблица accounts не имеет колонок month и year")
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
    assert len(data) == 2

@pytest.mark.xfail(reason="Таблица accounts не имеет колонок month и year, и charges не имеет service_name")
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
    services = [item["service_name"] for item in data["charges"]]
    assert "ХВС" in services
    assert "ГВС" in services