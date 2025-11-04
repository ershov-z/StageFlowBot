import os
from datetime import datetime
from pathlib import Path
from threading import Thread

from dotenv import load_dotenv
from loguru import logger
from telegram import Update, Document
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ==== Health-check сервер для Koyeb ====
ENABLE_HEALTH_SERVER = True
HEALTH_PORT = int(os.getenv("PORT", "8000"))

def start_health_server():
    """Простой Flask-сервер для health-check Koyeb."""
    if not ENABLE_HEALTH_SERVER:
        return
    try:
        from flask import Flask
    except ImportError:
        logger.warning("Flask не установлен — health-check сервер отключён")
        return

    app = Flask(__name__)

    @app.get("/")
    def ok():
        return "OK", 200

    def run():
        app.run(host="0.0.0.0", port=HEALTH_PORT, use_reloader=False)

    Thread(target=run, daemon=True).start()
    logger.info(f"✅ Health-check сервер запущен на порту {HEALTH_PORT}")

# ==== Пути и логирование ====
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

logger.remove()
logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO")
logger.add(
    LOGS_DIR / "bot_{time:YYYYMMDD}.log",
    rotation="10 MB",
    retention="10 days",
    level="INFO",
    encoding="utf-8",
)

# ==== Токен ====
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN не найден!")
    raise SystemExit(1)

WELCOME_TEXT = (
    "👋 Привет! Отправьте мне вашу концертную программу (.docx).\n\n"
    "Я прочитаю таблицу, залогирую её структуру и верну файл обратно для проверки."
)

# ==== Хендлеры ====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"/start от @{getattr(user, 'username', None)} (id={user.id})")
    await update.message.reply_text(WELCOME_TEXT)

def _is_docx(document: Document) -> bool:
    """Проверяет, является ли файл DOCX."""
    return (
        document
        and (
            (document.file_name or "").lower().endswith(".docx")
            or (document.mime_type or "").endswith("wordprocessingml.document")
        )
    )

async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка загруженного .docx файла."""
    message = update.message
    user = update.effective_user
    if not message or not message.document:
        return

    doc: Document = message.document
    if not _is_docx(doc):
        await message.reply_text("⚠️ Отправь, пожалуйста, .docx файл.")
        return

    logger.info(f"📄 Получен .docx от @{getattr(user, 'username', None)}: {doc.file_name}")

    file = await context.bot.get_file(doc.file_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_name = f"{timestamp}__{doc.file_name or 'program.docx'}"
    local_path = DATA_DIR / local_name
    await file.download_to_drive(local_path.as_posix())
    logger.info(f"📥 Файл сохранён: {local_path}")

    # ==== УДАЛИТЬ (тест docx_reader) ====
    from utils.docx_reader import read_program
    import json

    def make_json_safe(obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    try:
        data = read_program(local_path)
        logger.info("📊 Таблица успешно прочитана:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False, default=make_json_safe))
    except Exception as e:
        logger.exception(f"Ошибка при парсинге docx: {e}")
    # ==== УДАЛИТЬ ====

    # Отправляем обратно исходный файл (временно)
    processed_path = DATA_DIR / f"processed_{local_name}"
    processed_path.write_bytes(local_path.read_bytes())
    await message.reply_document(
        document=processed_path.open("rb"),
        filename=processed_path.name,
        caption="✅ Таблица успешно распознана. Проверь логи Koyeb для детальной структуры.",
    )

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок."""
    logger.exception(f"Ошибка в обработчике: {context.error}")

# ==== Запуск ====
def main() -> None:
    logger.info("🚀 Запуск Telegram-бота...")
    start_health_server()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))
    app.add_error_handler(on_error)

    logger.info("📡 Переходим в режим polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
