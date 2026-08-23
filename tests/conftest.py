import pytest
import tempfile
import os
import sqlite3

# Импортируем приложение после того, как определим фикстуры? 
# Лучше импортировать внутри фикстур, чтобы избежать глобального состояния.
# Однако для удобства импортируем здесь, но путь к БД будем менять в фикстуре.
import frontend

@pytest.fixture(scope='function')
def temp_db():
    """Создаёт временную БД и возвращает её путь."""
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    # Инициализация схемы (скопируйте из init_db.py или вынесите в отдельную функцию)
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            account_number TEXT NOT NULL,
            month TEXT,
            year INTEGER,
            UNIQUE(address, account_number, month, year)
        );
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER,
            service_name TEXT,
            charge REAL,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        );
        CREATE INDEX IF NOT EXISTS idx_accounts_address ON accounts(address);
        CREATE INDEX IF NOT EXISTS idx_accounts_month_year ON accounts(month, year);
        -- добавьте другие таблицы, если есть
    ''')
    conn.commit()
    conn.close()
    yield db_path
    os.close(db_fd)
    os.unlink(db_path)

@pytest.fixture
def app(temp_db):
    """Создаёт экземпляр приложения с тестовой конфигурацией."""
    # Подменяем глобальную переменную DB_PATH (если она используется)
    frontend.DB_PATH = temp_db
    # Если используется config, также задаём:
    frontend.app.config['TESTING'] = True
    frontend.app.config['DATABASE'] = temp_db
    return frontend.app

@pytest.fixture
def client(app):
    """Возвращает тестовый клиент Flask."""
    return app.test_client()

# Фикстуры для моков API
@pytest.fixture
def mock_ai_response():
    from unittest.mock import patch, MagicMock
    with patch('ai_analyzer.requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Анализ выполнен: отклонений нет."}}]
        }
        mock_post.return_value = mock_response
        yield mock_post

@pytest.fixture
def mock_telegram():
    from unittest.mock import patch, MagicMock
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        yield mock_post