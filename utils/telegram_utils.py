import os
import requests
from loguru import logger

# Получаем токен из окружения
TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")

def send_message(chat_id: int, text: str):
    """Отправка сообщения пользователю через Telegram Bot API"""
    if not TOKEN:
        logger.error("❌ Не найден TELEGRAM_TOKEN. Сообщение не будет отправлено.")
        return

    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = {"chat_id": chat_id, "text": text}
        requests.post(url, data=data, timeout=10)
        logger.info(f"📨 Сообщение отправлено пользователю {chat_id}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения пользователю {chat_id}: {e}")
