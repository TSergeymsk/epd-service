import pytest
from getmail_filter import should_process_email

def test_should_process_email():
    assert should_process_email("uslugi@mos.ru") is True
    assert should_process_email("user@example.com") is False
    assert should_process_email("uslugi@mos.ru (some text)") is True  # если нужно
    assert should_process_email("spam@mos.ru") is False