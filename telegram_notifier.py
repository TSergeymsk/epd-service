#!/usr/bin/env python3
import requests
import logging
import html

logger = logging.getLogger(__name__)

def send_telegram_message(bot_token, chat_id, message_html):
    """
    Отправляет сообщение в Telegram с HTML-разметкой.
    message_html должен содержать только разрешённые теги: <b>, <i>, <code>, <pre>.
    """
    # Telegram ограничивает длину сообщения примерно 4096 символами
    MAX_LEN = 4000
    if len(message_html) > MAX_LEN:
        logger.warning(f"Сообщение слишком длинное ({len(message_html)} символов), обрезаем до {MAX_LEN}")
        message_html = message_html[:MAX_LEN] + "... (обрезано)"
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message_html,
        'parse_mode': 'HTML'
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 400:
            logger.error(f"Telegram API вернул 400. Тело ответа: {response.text}")
            # Пробуем отправить как обычный текст (без parse_mode)
            logger.info("Повторная попытка отправки без parse_mode...")
            payload.pop('parse_mode')
            response2 = requests.post(url, json=payload, timeout=10)
            response2.raise_for_status()
            logger.info("Сообщение отправлено как обычный текст")
        else:
            response.raise_for_status()
            logger.info("Сообщение в Telegram отправлено успешно")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
