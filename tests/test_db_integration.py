import sqlite3
from conftest import temp_db
from init_db import init_database  # если есть функция инициализации

def test_db_schema(temp_db):
    # Проверяем наличие всех таблиц и индексов
    conn = sqlite3.connect(temp_db)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    assert "accounts" in tables
    assert "charges" in tables
    # Проверяем индексы
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = [row[0] for row in cur.fetchall()]
    assert "idx_accounts_address" in indexes
    conn.close()

def test_foreign_key_constraint(temp_db):
    # Проверяем, что нельзя вставить charge без существующего account_id
    conn = sqlite3.connect(temp_db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO charges (account_id, service_name, charge) VALUES (999, 'ХВС', 100)")
    conn.close()