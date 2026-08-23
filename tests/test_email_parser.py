import pytest
import os
import sqlite3
import tempfile
from email_parser import parse_email, normalize_service_name, save_charges_to_db

def test_normalize_service_name():
    assert normalize_service_name("ХВС (холодное водоснабжение)") == "ХВС"
    assert normalize_service_name("ГВС (горячее водоснабжение)") == "ГВС"
    assert normalize_service_name("Электроэнергия (день)") == "Электроэнергия"
    assert normalize_service_name("Неизвестная услуга") == "Неизвестная услуга"

def test_save_charges_to_db(temp_db):
    # Сначала создаём запись в accounts
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc1", "январь", 2025))
    account_id = conn.lastrowid
    conn.commit()
    conn.close()
    
    charges = [("ХВС", 150.0), ("ГВС", 200.0)]
    save_charges_to_db(temp_db, account_id, charges)
    
    conn = sqlite3.connect(temp_db)
    cur = conn.execute("SELECT service_name, charge FROM charges WHERE account_id=?", (account_id,))
    rows = cur.fetchall()
    assert len(rows) == 2
    assert ("ХВС", 150.0) in rows
    assert ("ГВС", 200.0) in rows
    conn.close()

def test_parse_email_success(temp_db):
    # Подменяем глобальный DB_PATH на временный (если функция использует его)
    import email_parser
    email_parser.DB_PATH = temp_db
    
    email_content = """From: uslugi@mos.ru
To: user@example.com
Subject: ЕПД за декабрь 2025

--boundary
Content-Type: text/html

<html><body>
<table>
<tr><td>Адрес: г. Москва, ул. Ленина, д. 1</td></tr>
<tr><td>Лицевой счет: 1234567890</td></tr>
<tr><td>Период: декабрь 2025</td></tr>
<tr><td>ХВС: 150.50</td></tr>
<tr><td>ГВС: 200.75</td></tr>
</table>
</body></html>
--boundary--
"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(email_content)
        fname = f.name
    
    result = parse_email(fname)
    assert result is True
    
    # Проверяем данные в БД
    conn = sqlite3.connect(temp_db)
    cur = conn.execute("SELECT address, account_number, month, year FROM accounts")
    rows = cur.fetchall()
    assert len(rows) == 1
    # Проверяем начисления
    cur = conn.execute("SELECT service_name, charge FROM charges")
    charges = cur.fetchall()
    assert len(charges) >= 2
    conn.close()
    os.unlink(fname)