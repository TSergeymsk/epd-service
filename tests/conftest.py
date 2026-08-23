import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import tempfile
import sqlite3
from init_db import init_database
import frontend

@pytest.fixture(scope='function')
def temp_db():
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    init_database(db_path)
    yield db_path
    os.close(db_fd)
    os.unlink(db_path)

@pytest.fixture
def app(temp_db):
    frontend.DB_PATH = temp_db
    frontend.app.config['TESTING'] = True
    frontend.app.config['DATABASE'] = temp_db
    return frontend.app

@pytest.fixture
def client(app):
    return app.test_client()

# Моки для внешних API
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