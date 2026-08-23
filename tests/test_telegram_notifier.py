import pytest
from unittest.mock import patch, MagicMock
from telegram_notifier import send_telegram_message

def test_send_telegram_success(mock_telegram):
    # Передаём все обязательные аргументы: bot_token также требуется
    result = send_telegram_message(chat_id="123", message_html="Тестовое сообщение", bot_token="test_token")
    assert result is True
    mock_telegram.assert_called_once()

def test_send_telegram_long_message(mock_telegram):
    long_text = "A" * 5000
    result = send_telegram_message(chat_id="123", message_html=long_text, bot_token="test_token")
    assert result is True
    # Проверяем, что было хотя бы одно обращение к API
    assert mock_telegram.call_count >= 1

def test_send_telegram_retry_on_failure():
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_post.side_effect = [Exception("Network error"), Exception("Network error"), MagicMock(status_code=200)]
        result = send_telegram_message(chat_id="123", message_html="test", bot_token="test_token")
        assert result is True
        assert mock_post.call_count == 3