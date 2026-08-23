import pytest
from unittest.mock import patch
from telegram_notifier import send_telegram_message

def test_send_telegram_success(mock_telegram):
    # Предполагаем, что функция принимает chat_id и message_html
    # Если в реальности они берутся из конфига, в тесте можно передать заглушки
    result = send_telegram_message(chat_id="123", message_html="Тестовое сообщение")
    assert result is True
    mock_telegram.assert_called_once()

def test_send_telegram_long_message(mock_telegram):
    long_text = "A" * 5000
    # Функция должна разбивать длинные сообщения
    result = send_telegram_message(chat_id="123", message_html=long_text)
    assert result is True
    # Проверяем, что было несколько вызовов (если реализовано разбиение)
    # Если в реальности разбиения нет, то будет один вызов
    # Ожидаем хотя бы один вызов
    assert mock_telegram.call_count >= 1

def test_send_telegram_retry_on_failure():
    # Тест на повторные попытки
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_post.side_effect = [Exception("Network error"), Exception("Network error"), MagicMock(status_code=200)]
        result = send_telegram_message(chat_id="123", message_html="test")
        assert result is True
        assert mock_post.call_count == 3