"""Тесты для парсеров (base_parser и mos_parser)."""
import pytest
import os
import email
from email.policy import default
from pathlib import Path
from unittest.mock import patch, MagicMock
from parsers.base_parser import (
    normalize_service_name,
    get_or_create_account,
    get_or_create_period,
    get_or_create_service,
    save_charges
)
from parsers.mos_parser import parse_html_email, parse_text_epd, process_email_file

def test_normalize_service_name():
    assert normalize_service_name('ХВС') == 'ХВС'
    assert normalize_service_name('ГВС КПУ') == 'ГВС'
    assert normalize_service_name('Водоотведение') == 'Водоотведение'
    assert normalize_service_name('Отопление') == 'Отопление'
    assert normalize_service_name('Сод.жил.') == 'Содержание жилья'
    assert normalize_service_name('Капитальный ремонт') == 'Капитальный ремонт'
    assert normalize_service_name('ТКО') == 'ТКО'
    assert normalize_service_name('Запирающее устройство') == 'Запирающее устройство'
    assert normalize_service_name('шлагбаум') == 'Шлагбаум'
    assert normalize_service_name('Неизвестная услуга') == 'Неизвестная услуга'

def test_get_or_create_account(temp_db):
    conn, path = temp_db
    account_id = get_or_create_account(conn, '12345', 'Test Address')
    assert account_id == 1
    # Повторный вызов возвращает тот же id
    account_id2 = get_or_create_account(conn, '12345', 'Test Address')
    assert account_id2 == 1
    # С другим номером создаёт новый
    account_id3 = get_or_create_account(conn, '67890', 'Another')
    assert account_id3 == 2

def test_get_or_create_period(temp_db):
    conn, path = temp_db
    period_id = get_or_create_period(conn, 2024, 1)
    assert period_id == 1
    period_id2 = get_or_create_period(conn, 2024, 1)
    assert period_id2 == 1
    period_id3 = get_or_create_period(conn, 2024, 2)
    assert period_id3 == 2

def test_get_or_create_service(temp_db):
    conn, path = temp_db
    service_id = get_or_create_service(conn, 'Тестовая услуга', 'ед')
    assert service_id == 1
    service_id2 = get_or_create_service(conn, 'Тестовая услуга')
    assert service_id2 == 1
    service_id3 = get_or_create_service(conn, 'Другая')
    assert service_id3 == 2

def test_save_charges(temp_db):
    conn, path = temp_db
    account_id = get_or_create_account(conn, '123', 'addr')
    period_id = get_or_create_period(conn, 2024, 1)
    services = [
        {'name': 'ХВС', 'unit': 'м3', 'quantity': 10.0, 'tariff': 50.0, 'amount_due': 500.0, 'benefit': 0, 'recalculation': 0}
    ]
    save_charges(conn, account_id, period_id, services)
    cur = conn.execute("SELECT * FROM charges WHERE account_id = ? AND period_id = ?", (account_id, period_id))
    row = cur.fetchone()
    assert row['amount_due'] == 500.0
    # service_id создан автоматически
    service_id = row['service_id']
    cur2 = conn.execute("SELECT name FROM services WHERE id = ?", (service_id,))
    assert cur2.fetchone()['name'] == 'ХВС'

def test_parse_html_email_empty():
    # Если HTML нет или не распознан, возвращаем None
    result = parse_html_email('<html><body>Нет таблицы</body></html>')
    assert result is None

def test_parse_text_epd_basic():
    text = """
ФЛС №12345
АДРЕС: ул. Тестовая, д.1 кв.2
ПЕРИОД: 1 месяц 2024 год
Итого к оплате: 1500.50 руб.
ХВС 10.0 500.00
ГВС 5.0 600.00
"""
    data = parse_text_epd(text)
    assert data['account_number'] == '12345'
    assert data['address'] == 'ул. Тестовая, д.1 кв.2'
    assert data['year'] == 2024
    assert data['month'] == 1
    assert data['total_due'] == 1500.50
    assert len(data['services']) >= 2
    # Проверяем, что услуги нормализованы
    service_names = [s['name'] for s in data['services']]
    assert 'ХВС' in service_names
    assert 'ГВС' in service_names

@patch('parsers.mos_parser.get_db_connection')
def test_process_email_file(mock_db_conn, tmp_path):
    # Создаём временный email файл
    email_content = """From: test@example.com
To: me@example.com
Subject: Test

ФЛС №12345
АДРЕС: ул. Тестовая, д.1 кв.2
ПЕРИОД: 1 месяц 2024 год
Итого к оплате: 1500.50 руб.
ХВС 10.0 500.00
ГВС 5.0 600.00
"""
    email_file = tmp_path / "test.eml"
    email_file.write_text(email_content)
    mock_conn = MagicMock()
    mock_db_conn.return_value = mock_conn
    # Мокаем get_or_create_account и т.д.
    with patch('parsers.mos_parser.get_or_create_account', return_value=1), \
         patch('parsers.mos_parser.get_or_create_period', return_value=1), \
         patch('parsers.mos_parser.save_charges') as mock_save:
        success, err = process_email_file(str(email_file), 'test_mail_id')
        assert success is True
        mock_save.assert_called()
