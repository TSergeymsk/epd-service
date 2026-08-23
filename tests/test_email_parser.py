import pytest
from email_parser import parse_email, normalize_service_name, save_charges_to_db
from conftest import temp_db

def test_normalize_service_name():
    assert normalize_service_name("ХВС (холодное водоснабжение)") == "ХВС"
    assert normalize_service_name("ГВС (горячее водоснабжение)") == "ГВС"
    assert normalize_service_name("Электроэнергия (день)") == "Электроэнергия"
    assert normalize_service_name("Неизвестная услуга") == "Неизвестная услуга"

def test_parse_email_success(temp_db):
    # Подготовка тестового email-файла (можно сохранить в папке fixtures)
    email_content = """From: uslugi@mos.ru
To: user@example.com
Subject: ЕПД за декабрь 2025

--boundary
Content-Type: text/html

<html><body>
<table>
<tr><td>Адрес: г. Москва, ул. Ленина, д. 1, кв. 2</td></tr>
<tr><td>Лицевой счет: 1234567890</td></tr>
<tr><td>Период: декабрь 2025</td></tr>
<tr><td>ХВС: 150.50</td></tr>
<tr><td>ГВС: 200.75</td></tr>
</table>
</body></html>
--boundary--
"""
    # Сохраняем во временный файл
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(email_content)
        fname = f.name

    # Запускаем парсинг
    result = parse_email(fname)  # функция должна возвращать словарь с данными или код
    assert result is True  # или проверяем наличие данных в БД

    # Проверяем, что данные сохранены
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("SELECT address, account_number, month, year FROM accounts")
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == ("г. Москва, ул. Ленина, д. 1, кв. 2", "1234567890", "декабрь", 2025)
    cur.execute("SELECT service_name, charge FROM charges")
    charges = cur.fetchall()
    assert len(charges) == 2
    assert ("ХВС", 150.50) in charges
    assert ("ГВС", 200.75) in charges
    conn.close()
    os.unlink(fname)

def test_parse_email_missing_required_fields(temp_db):
    # Тест на отсутствие обязательных полей
    email_content = """From: uslugi@mos.ru
Subject: ЕПД
--boundary
Content-Type: text/html
<html><body>Нет данных</body></html>
--boundary--
"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(email_content)
        fname = f.name
    result = parse_email(fname)
    assert result is False  # или выбрасывается исключение
    os.unlink(fname)

def test_save_charges_to_db_duplicate(temp_db):
    # Проверка, что дубликаты не создаются
    conn = sqlite3.connect(temp_db)
    # Вставляем тестовую запись
    conn.execute("INSERT INTO accounts (address, account_number, month, year) VALUES (?,?,?,?)",
                 ("addr1", "acc1", "январь", 2025))
    account_id = conn.lastrowid
    conn.commit()
    # Пытаемся сохранить те же услуги
    charges = [("ХВС", 100), ("ГВС", 200)]
    save_charges_to_db(temp_db, account_id, charges)
    # Проверяем, что дубли не добавились (если есть уникальное ограничение)
    cur = conn.execute("SELECT COUNT(*) FROM charges WHERE account_id=?", (account_id,))
    count = cur.fetchone()[0]
    assert count == 2  # Должно быть 2, если уже были, то они перезаписываются или игнорируются
    conn.close()