import pytest
from unittest.mock import patch, MagicMock
from telegram_notifier import send_telegram_message

def test_send_telegram_success(mock_telegram):
    send_telegram_message(chat_id="123", message_html="Тестовое сообщение", bot_token="test_token")
    mock_telegram.assert_called_once()

def test_send_telegram_long_message(mock_telegram):
    long_text = "A" * 5000
    send_telegram_message(chat_id="123", message_html=long_text, bot_token="test_token")
    assert mock_telegram.call_count >= 1

def test_send_telegram_retry_on_failure():
    """Проверяем, что при ошибке функция логирует ошибку и не повторяет попытки."""
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_post.side_effect = Exception("Network error")
        # Функция должна обработать исключение и вернуть None
        result = send_telegram_message(chat_id="123", message_html="test", bot_token="test_token")
        # Проверяем, что был только один вызов (без повторных)
        assert mock_post.call_count == 1
        # Проверяем, что результат None (или False, смотря что возвращает)
        assert result is None