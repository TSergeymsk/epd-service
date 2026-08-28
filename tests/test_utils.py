"""Тесты для модуля utils."""
import pytest
from pathlib import Path
from unittest.mock import patch
from utils import load_ini_config, load_ai_config, get_db_path, get_db_connection, setup_logging

def test_load_ini_config_success(tmp_path):
    config_content = """
[paths]
db_path = test.db
email_temp_dir = /tmp

[openrouter]
api_key = key123
    """
    config_file = tmp_path / "config.ini"
    config_file.write_text(config_content)
    with patch('utils.get_config_path', return_value=str(config_file)):
        config = load_ini_config()
        assert config.get('paths', 'db_path') == 'test.db'
        assert config.get('openrouter', 'api_key') == 'key123'

def test_load_ini_config_fallback():
    with patch('utils.get_config_path', return_value='/nonexistent.ini'):
        config = load_ini_config()
        assert config is not None
        assert config.has_section('paths') is False

def test_load_ai_config_missing(tmp_path):
    with patch('utils.get_project_root', return_value=tmp_path):
        ai_config = load_ai_config()
        assert ai_config == {}

def test_get_db_path():
    class MockConfig:
        def get(self, section, key):
            if section == 'paths' and key == 'db_path':
                return '/test/db.sqlite'
            return None
    with patch('utils.load_ini_config', return_value=MockConfig()):
        assert get_db_path() == '/test/db.sqlite'

def test_get_db_connection():
    with patch('utils.get_db_path', return_value=':memory:'):
        conn = get_db_connection()
        assert conn is not None
        conn.close()

def test_setup_logging(tmp_path):
    log_dir = tmp_path / 'logs'
    logger = setup_logging('test_logger', str(log_dir))
    assert logger.name == 'test_logger'
    assert log_dir.exists()