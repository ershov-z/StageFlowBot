import os
import requests
import asyncio
from loguru import logger

# ============================================================
# 🔑 Настройки Telegram API
# ============================================================
TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else None


# ============================================================
# 📨 Синхронная отправка текстовых сообщений
# ============================================================
def send_message(chat_id: int, text: str):
    """Синхронная отправка текстового сообщения пользователю (безопасно для потоков)."""
    if not TOKEN:
        logger.error("❌ Не найден TELEGRAM_TOKEN. Сообщение не будет отправлено.")
        return

    url = f"{BASE_URL}/sendMessage"
    data = {"chat_id": chat_id, "text": text}

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
            import time
            time.sleep(2 ** attempt)

    logger.error(f"❌ Не удалось отправить сообщение пользователю {chat_id} после 3 попыток.")


# ============================================================
# 🧩 Асинхронная версия (для использования внутри async контекста)
# ============================================================
async def async_send_message(chat_id: int, text: str):
    """Асинхронная версия отправки сообщения — используется только в PTB контексте."""
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


# ============================================================
# 📂 Синхронная отправка документов (файлов)
# ============================================================
def send_document(chat_id: int, file_path: str, caption: str = ""):
    """Синхронная отправка файла пользователю (безопасно для потоков)."""
    if not TOKEN:
        logger.error("❌ Не найден TELEGRAM_TOKEN. Документ не будет отправлен.")
        return

    url = f"{BASE_URL}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {"chat_id": chat_id, "caption": caption}
            response = requests.post(url, data=data, files=files, timeout=60)
            if response.status_code == 200:
                logger.info(f"📤 Документ отправлен пользователю {chat_id}: {file_path}")
            else:
                logger.warning(
                    f"⚠️ Ошибка Telegram API ({response.status_code}): {response.text}"
                )
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке документа {file_path} пользователю {chat_id}: {e}")
