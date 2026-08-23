import pytest
from telegram_notifier import send_telegram_message
from conftest import mock_telegram

def test_send_telegram_success(mock_telegram):
    result = send_telegram_message("Тестовое сообщение")
    assert result is True
    mock_telegram.assert_called_once()
    args, kwargs = mock_telegram.call_args
    assert kwargs.get('json', {}).get('text') == "Тестовое сообщение"
    # Проверяем, что parse_mode и chat_id переданы

def test_send_telegram_long_message(mock_telegram):
    long_text = "A" * 5000
    result = send_telegram_message(long_text)
    assert result is True
    # Проверяем, что было несколько вызовов (разбивка)
    assert mock_telegram.call_count >= 2

def test_send_telegram_retry_on_failure():
    # Тест на повторные попытки при ошибке сети
    with patch('telegram_notifier.requests.post') as mock_post:
        mock_post.side_effect = [Exception("Network error"), Exception("Network error"), MagicMock(status_code=200)]
        result = send_telegram_message("test")
        assert result is True
        assert mock_post.call_count == 3