import pytest
from unittest.mock import patch, MagicMock
from telegram_notifier import send_telegram_message

def test_send_telegram_success(mock_telegram):
    # Вызываем функцию с параметрами (bot_token может игнорироваться)
    send_telegram_message(chat_id="123", message_html="Тестовое сообщение", bot_token="test_token")
    # Проверяем, что requests.post был вызван
    mock_telegram.assert_called_once()

def test_send_telegram_long_message(mock_telegram):
    long_text = "A" * 5000
    send_telegram_message(chat_id="123", message_html=long_text, bot_token="test_token")
    # Функция должна обрезать сообщение и отправить один раз (или несколько)
    # Проверяем, что был хотя бы один вызов
    assert mock_telegram.call_count >= 1

def test_send_telegram_retry_on_failure():
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_post.side_effect = [Exception("Network error"), Exception("Network error"), MagicMock(status_code=200)]
        send_telegram_message(chat_id="123", message_html="test", bot_token="test_token")
        # Проверяем, что было 3 попытки
        assert mock_post.call_count == 3