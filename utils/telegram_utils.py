import os
import requests
import asyncio
from loguru import logger

# Получаем токен из окружения
TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else None


def send_message(chat_id: int, text: str):
    """Синхронная отправка сообщения пользователю через Telegram Bot API (устойчивая к вызовам из потоков)."""
    if not TOKEN:
        logger.error("❌ Не найден TELEGRAM_TOKEN. Сообщение не будет отправлено.")
        return

    url = f"{BASE_URL}/sendMessage"
    data = {"chat_id": chat_id, "text": text}

    # Несколько попыток отправки при сетевых сбоях
    for attempt in range(3):
        try:
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                logger.info(f"📨 Сообщение отправлено пользователю {chat_id}")
                return
            else:
                logger.warning(
                    f"⚠️ Ошибка Telegram API ({response.status_code}): {response.text}"
                )
        except requests.RequestException as e:
            logger.warning(f"🔁 Попытка {attempt+1}/3 не удалась: {e}")
            time_sleep = 2 ** attempt
            try:
                asyncio.sleep(time_sleep)
            except Exception:
                import time
                time.sleep(time_sleep)

    logger.error(f"❌ Не удалось отправить сообщение пользователю {chat_id} после 3 попыток.")


async def async_send_message(chat_id: int, text: str):
    """Асинхронная версия отправки сообщения — для использования внутри async контекста."""
    if not TOKEN:
        logger.error("❌ Не найден TELEGRAM_TOKEN. Сообщение не будет отправлено.")
        return

    import aiohttp
    url = f"{BASE_URL}/sendMessage"
    data = {"chat_id": chat_id, "text": text}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=10) as resp:
                if resp.status == 200:
                    logger.info(f"📨 [async] Сообщение отправлено пользователю {chat_id}")
                else:
                    text_resp = await resp.text()
                    logger.warning(f"⚠️ [async] Ошибка Telegram API ({resp.status}): {text_resp}")
    except Exception as e:
        logger.error(f"Ошибка при async-отправке сообщения пользователю {chat_id}: {e}")
