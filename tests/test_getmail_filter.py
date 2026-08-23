import pytest
from getmail_filter import should_process_email  # если такая функция вынесена

def test_should_process_email():
    # Проверяем, что письмо от нужного отправителя
    assert should_process_email("uslugi@mos.ru") is True
    assert should_process_email("spam@mos.ru") is False
    # Проверяем регулярное выражение