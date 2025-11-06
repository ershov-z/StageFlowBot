# bot/main.py

import os
import sys
import json
import math
import time
import multiprocessing
import requests
from pathlib import Path
from datetime import datetime
from loguru import logger
from flask import Flask
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from utils.docx_reader import read_program
from utils.validator import (
    generate_program_variants,
    request_stop,   # 🛑 STOP FEATURE
)
from utils.docx_writer import save_program_to_docx
from utils.telegram_utils import send_message  # ✅ для отправки сообщений из процесса

# ============================================================
# ЛОГИРОВАНИЕ И HEALTH-CHECK
# ============================================================

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
logger.add("logs/bot_{time:YYYYMMDD}.log", rotation="10 MB", level="DEBUG")

app_health = Flask(__name__)

@app_health.route("/")
def health_root():
    return "OK"

@app_health.route("/health")
def health_check():
    return {"status": "healthy"}, 200

def start_health_server():
    def run():
        app_health.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
    proc = multiprocessing.Process(target=run, daemon=True)
    proc.start()
    logger.info("💓 Health-check сервер запущен на порту 8000")


# ============================================================
# KEEP-ALIVE
# ============================================================

def start_keep_alive():
    url = os.getenv("KOYEB_APP_URL")
    if not url:
        logger.warning("⚠️ KOYEB_APP_URL не задан, keep-alive отключён")
        return

    def ping_loop():
        while True:
            try:
                requests.get(url)
                logger.debug(f"[keep-alive] Пинг {url} успешен")
            except Exception as e:
                logger.warning(f"[keep-alive] Ошибка: {e}")
            time.sleep(240)

    proc = multiprocessing.Process(target=ping_loop, daemon=True)
    proc.start()
    logger.info(f"🩵 Keep-alive активирован (ping → {url})")


# ============================================================
# TOKEN
# ============================================================

TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    logger.error("❌ Не найден TELEGRAM_TOKEN (или BOT_TOKEN)")
    sys.exit(1)
else:
    logger.info(f"🔑 Токен найден ({len(TOKEN)} символов)")


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ
# ============================================================

def format_duration(seconds: float) -> str:
    minutes = int(seconds // 60)
    sec = int(seconds % 60)
    if minutes == 0:
        return f"{sec} сек"
    elif minutes < 60:
        return f"{minutes} мин {sec} сек"
    else:
        hours = minutes // 60
        minutes = minutes % 60
        return f"{hours} ч {minutes} мин {sec} сек"


# ============================================================
# 🛑 STOP FEATURE
# ============================================================

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Останавливает расчёт алгоритма"""
    user = update.effective_user
    logger.warning(f"🛑 Пользователь @{user.username} запросил остановку расчёта")
    request_stop()
    await update.message.reply_text("📨 Получен сигнал на остановку, завершение в ближайшие секунды...")


# ============================================================
# СТАРТ
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"/start от @{user.username} (id={user.id})")
    await update.message.reply_text(
        "👋 Привет! Отправь мне .docx с программой концерта — я её проанализирую, "
        "переставлю номера и при необходимости добавлю тянучки.\n\n"
        "🛑 Командой /stop можно прервать процесс и получить лучший найденный вариант."
    )


# ============================================================
# ОСНОВНАЯ ЛОГИКА (запускается в отдельном процессе)
# ============================================================

def run_generation(data, document_path, chat_id, username, timestamp):
    """Запускается в отдельном процессе, чтобы не блокировать Telegram"""
    try:
        start_time = time.time()
        variants, stats = generate_program_variants(data, chat_id=chat_id)
        elapsed = time.time() - start_time
        readable_time = format_duration(elapsed)

        if not variants:
            send_message(chat_id, "❌ Вариантов программы не нашлось. Попробуйте еще раз!")
            return

        result = variants[0]
        result_json_path = Path(f"data/result_{timestamp}_{chat_id}.json")
        with open(result_json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        out_path = Path(f"data/output_{timestamp}_{chat_id}.docx")
        save_program_to_docx(result, out_path, original_filename=document_path.name)

        tyan_titles = [x["title"] for x in result if x["type"] == "тянучка"]

        msg = (
            f"🎬 Программа собрана!\n"
            f"🕓 Время: {readable_time}\n"
            f"Проверено перестановок: {stats.get('checked_variants', 0)}\n"
            f"Исходных конфликтов: {stats.get('initial_conflicts', 0)}\n"
            f"Осталось конфликтов: {stats.get('final_conflicts', 0)}\n"
            f"Добавлено тянучек: {stats.get('tyanuchki_added', 0)}"
        )
        if tyan_titles:
            msg += "\n\n🧩 Тянучки:\n" + "\n".join(f"• {t}" for t in tyan_titles)
        else:
            msg += "\n\n✅ Без тянучек!"

        send_message(chat_id, f"✅ Готово! Время: {readable_time}")
        send_message(chat_id, msg)
        logger.info(f"✅ Завершено для @{username} за {readable_time}")

    except Exception as e:
        logger.exception(f"Ошибка генерации для @{username}: {e}")
        send_message(chat_id, f"❌ Ошибка при обработке файла: {e}")


# ============================================================
# ОБРАБОТКА ФАЙЛОВ
# ============================================================

async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    username = user.username or "unknown"
    document = update.message.document
    if not document.file_name.lower().endswith(".docx"):
        return await update.message.reply_text("⚠️ Отправь файл в формате .docx.")

    logger.info(f"📄 Получен файл {document.file_name} от @{username}")
    file = await document.get_file()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = Path(f"data/{timestamp}__{document.file_name}")
    await file.download_to_drive(local_path)

    data = read_program(local_path)
    parsed_json_path = Path(f"data/parsed_{timestamp}_{user.id}.json")
    with open(parsed_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    await update.message.reply_document(open(parsed_json_path, "rb"), caption="📘 Исходные данные после парсинга:")
    await update.message.reply_text("📊 Начинаю генерацию программы... (можно остановить командой /stop)")

    # 🧩 Запуск отдельного процесса для расчёта
    proc = multiprocessing.Process(
        target=run_generation,
        args=(data, local_path, user.id, username, timestamp),
        daemon=True,
    )
    proc.start()
    logger.info(f"🚀 Процесс генерации запущен (pid={proc.pid}) для @{username}")


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("🚀 Запуск Telegram-бота...")
    start_health_server()
    start_keep_alive()

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))

    logger.info("📡 Переходим в режим polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    main()
