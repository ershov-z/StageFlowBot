import os
import json
from datetime import datetime
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from loguru import logger

# === Импорт парсера ===
from utils.docx_reader import read_program
logger.info(f"✅ Импортирован read_program из: {getattr(read_program, '__code__', None) and read_program.__code__.co_filename}")

# === Конфигурация ===
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger.add(os.path.join(LOG_DIR, "bot_{time:YYYYMMDD}.log"), rotation="5 MB", retention="10 days")

# === Flask для health-check (Koyeb требует HTTP-сервер) ===
app_health = Flask(__name__)

@app_health.route("/")
def index():
    return "Bot is alive!"

def start_health_server():
    """Запуск health-check Flask-сервера"""
    from threading import Thread
    def run():
        logger.info("🌐 Запуск health-check сервера на порту 8000")
        app_health.run(host="0.0.0.0", port=8000, debug=False)
    Thread(target=run, daemon=True).start()


# === Хендлеры ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"/start от @{user.username} (id={user.id})")
    await update.message.reply_text(
        "Привет! Отправь мне .docx файл с программой, и я её разберу. 📄"
    )


async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает загруженный DOCX-файл"""
    user = update.effective_user
    document = update.message.document

    if not document or not document.file_name.endswith(".docx"):
        await update.message.reply_text("Отправь, пожалуйста, именно .docx файл.")
        return

    file = await document.get_file()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}__{document.file_name}"
    save_path = os.path.join(DATA_DIR, filename)

    logger.info(f"📄 Получен .docx от @{user.username}: {document.file_name}")
    await file.download_to_drive(save_path)
    logger.info(f"📥 Файл сохранён: {save_path}")

    # --- Парсинг файла ---
    try:
        data = read_program(save_path)
        if not data:
            await update.message.reply_text("❌ Не удалось прочитать таблицу из файла.")
            return

        logger.info("📊 Таблица успешно прочитана:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))

        # Отправим пользователю краткий результат
        msg_preview = "\n".join([f"{row['num']} {row['title']}" for row in data[:10]])
        await update.message.reply_text(
            f"✅ Прочитано {len(data)} строк из файла '{document.file_name}'.\n"
            f"Пример:\n{msg_preview}"
        )

    except Exception as e:
        logger.exception("Ошибка при парсинге docx")
        await update.message.reply_text(f"❌ Ошибка при обработке файла: {e}")


# === Основная функция ===
def main():
    logger.info("🚀 Запуск Telegram-бота...")

    if not TOKEN:
        logger.error("❌ Не найден BOT_TOKEN в переменных окружения.")
        return

    start_health_server()

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_docx))

    logger.info("📡 Переходим в режим polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
