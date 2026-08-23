#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import subprocess
import re
import tempfile
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

def should_process_email(from_header):
    """
    Проверяет, нужно ли обрабатывать письмо по полю From.
    """
    # Пример: разрешаем только письма с uslugi@mos.ru
    pattern = r'uslugi@mos\.ru$'
    return bool(re.search(pattern, from_header))

# Основная логика фильтра
if __name__ == "__main__":
    # Получаем письмо из stdin (как это делает getmail)
    email_content = sys.stdin.read()
    if not email_content:
        sys.exit(0)
    
    # Парсим заголовок From (упрощённо)
    from_line = re.search(r'^From:\s*(.+?)$', email_content, re.MULTILINE)
    if not from_line:
        sys.exit(0)
    from_header = from_line.group(1).strip()
    
    if should_process_email(from_header):
        # Сохраняем письмо во временный файл и вызываем парсер
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.eml') as f:
            f.write(email_content)
            tmp_path = f.name
        
        try:
            subprocess.run(['python3', 'email_parser.py', tmp_path], timeout=180, check=True)
        except Exception as e:
            print(f"Ошибка обработки: {e}", file=sys.stderr)
        finally:
            # Удаляем временный файл, если нужно (можно оставить для отладки)
            # os.unlink(tmp_path)
            pass
    else:
        # Письмо не обрабатываем
        sys.exit(0)