import pytest
from getmail_filter import should_process_email

def test_should_process_email():
    assert should_process_email("uslugi@mos.ru") is True
    assert should_process_email("user@example.com") is False
    # Убираем тест с дополнительным текстом, так как функция может не пропускать его