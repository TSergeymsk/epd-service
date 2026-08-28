#!/usr/bin/env python3
"""
Общие утилиты: загрузка конфигурации, логирование, работа с БД.
"""
import os
import sys
import logging
import configparser
import yaml
from pathlib import Path

def get_project_root():
    return Path(__file__).parent.absolute()

def get_config_path():
    env_path = os.environ.get('EPD_CONFIG')
    if env_path:
        return env_path
    return str(get_project_root() / 'config.ini')

def load_ini_config():
    config = configparser.ConfigParser()
    config.read(get_config_path())
    return config

def load_ai_config():
    yaml_path = get_project_root() / 'ai_config.yaml'
    if not yaml_path.exists():
        logging.warning("ai_config.yaml не найден, используются значения по умолчанию")
        return {}
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_db_path():
    config = load_ini_config()
    return config.get('paths', 'db_path')

def get_db_connection():
    import sqlite3
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def setup_logging(name, log_dir=None):
    if log_dir is None:
        config = load_ini_config()
        log_dir = config.get('logging', 'log_dir')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'{name}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)
