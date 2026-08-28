"""Тесты для orchestrator (AI-запросы и генерация сообщений)."""
import pytest
from unittest.mock import patch, MagicMock
import json
import time
from orchestrator import (
    clean_markdown,
    is_ai_configured,
    get_aggregated_data,
    format_prompt,
    call_ai_api,
    process_llm_requests,
    generate_telegram_message_for_period,
    process_telegram_messages,
    create_telegram_messages_for_successful_llm
)

def test_clean_markdown():
    text = "**жирный** *курсив* _подчеркнутый_ ~~зачеркнутый~~ ### Заголовок `код` [ссылка](url) ---"
    cleaned = clean_markdown(text)
    assert "жирный" in cleaned
    assert "курсив" in cleaned
    assert "подчеркнутый" in cleaned
    assert "зачеркнутый" in cleaned
    assert "Заголовок" in cleaned
    assert "код" in cleaned
    assert "ссылка" in cleaned
    assert "---" not in cleaned

def test_is_ai_configured_with_config(mock_config):
    with patch('orchestrator.INI_CONFIG', mock_config), \
         patch('orchestrator.AI_CONFIG', {'ai': {'url': 'http://test', 'model': 'test'}}):
        assert is_ai_configured() is True

def test_is_ai_configured_missing_key():
    with patch('orchestrator.INI_CONFIG', {'openrouter': {'api_key': ''}}):
        assert is_ai_configured() is False

def test_get_aggregated_data_no_accounts(temp_db):
    conn, path = temp_db
    address = "No such address"
    data, prev, prev_ym, last_12 = get_aggregated_data(conn, address, 1)
    assert data == []
    assert prev is None
    assert prev_ym is None
    assert last_12 == {}

def test_format_prompt_without_template():
    with patch('orchestrator.AI_CONFIG', {'prompts': {}}):
        result = format_prompt("addr", "2024-01", [], None, None, {})
        assert result is None  # должен вернуть None

def test_call_ai_api_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'choices': [{'message': {'content': 'Test response'}}],
        'usage': {'total_tokens': 100}
    }
    with patch('requests.post', return_value=mock_response), \
         patch('orchestrator.AI_CONFIG', {'ai': {'url': 'http://test', 'model': 'test', 'timeout': 10, 'temperature': 0.7, 'max_tokens': 100}}), \
         patch('orchestrator.INI_CONFIG', {'openrouter': {'api_key': 'key'}}):
        answer, tokens = call_ai_api("prompt")
        assert answer == 'Test response'
        assert tokens == 100

def test_call_ai_api_429_retry():
    # Первый запрос 429, второй успешный
    response_429 = MagicMock()
    response_429.status_code = 429
    response_429.headers = {'Retry-After': '1'}
    response_ok = MagicMock()
    response_ok.status_code = 200
    response_ok.json.return_value = {'choices': [{'message': {'content': 'OK'}}], 'usage': {'total_tokens': 50}}
    with patch('requests.post', side_effect=[response_429, response_ok]), \
         patch('time.sleep', return_value=None), \
         patch('orchestrator.AI_CONFIG', {'ai': {'url': 'http://test', 'model': 'test', 'timeout': 10, 'temperature': 0.7, 'max_tokens': 100}}), \
         patch('orchestrator.INI_CONFIG', {'openrouter': {'api_key': 'key'}}):
        answer, tokens = call_ai_api("prompt", retries=3)
        assert answer == 'OK'
        assert tokens == 50

def test_generate_telegram_message_for_period_no_ai(temp_db):
    conn, path = temp_db
    # Создаём тестовые данные: account, period, charges
    cur = conn.cursor()
    cur.execute("INSERT INTO accounts (id, account_number, address) VALUES (1, '123', 'Test addr')")
    cur.execute("INSERT INTO periods (id, year, month, start_date) VALUES (1, 2024, 1, '2024-01-01')")
    cur.execute("INSERT INTO services (id, name) VALUES (1, 'Test service')")
    cur.execute("INSERT INTO charges (account_id, period_id, service_id, quantity, amount_due) VALUES (1, 1, 1, 10, 500)")
    conn.commit()
    # Нет LLM-запроса
    with patch('orchestrator.is_ai_configured', return_value=True):
        msg = generate_telegram_message_for_period(conn, "Test addr", 1)
        assert "🏠 Анализ ЕПД" in msg
        assert "Итого: 500.00 руб." in msg
        assert "AI-анализ не выполнен" in msg  # потому что нет успешного LLM

def test_process_telegram_messages_no_messages():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    with patch('orchestrator.get_pending_telegram_messages', return_value=[]):
        process_telegram_messages(conn)
        # ничего не происходит
        conn.execute.assert_not_called()
