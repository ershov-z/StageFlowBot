import os
from datetime import datetime
from pathlib import Path
from threading import Thread

from dotenv import load_dotenv
from loguru import logger
from telegram import Update, Document
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ==== Опционально: простой HTTP-сервер для health-check Koyeb ====
# Если в Koyeb настроен TCP health check на порт 8000, этот сервер позволит проверку пройти.
ENABLE_HEALTH_SERVER = True
HEALTH_PORT = int(os.getenv("PORT", "8000"))
def start_health_server():
    if not ENABLE_HEALTH_SERVER:
        return
    try:
        from flask import Flask
    except Exception:
        logger.warning("Flask не установлен — health-check сервер отключён")
        return
    app = Flask(__name__)

    @app.get("/")
    def ok():
        return "OK", 200

    def run():
        # use_reloader=False, чтобы не плодить процессы
        app.run(host="0.0.0.0", port=HEALTH_PORT, use_reloader=False)

    Thread(target=run, daemon=True).start()
    logger.info(f"Health-check сервер слушает порт {HEALTH_PORT}")

# ==== Пути и директории ====
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ==== Настройка логов ====
logger.remove()
logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO")
logger.add(
    LOGS_DIR / "bot_{time:YYYYMMDD}.log",
    rotation="10 MB",
    retention="10 days",
    level="INFO",
    encoding="utf-8",
    backtrace=True,
    diagnose=True,
)

# ==== Окружение ====
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN не найден. Добавь его в переменные окружения.")
    raise SystemExit(1)

WELCOME_TEXT = (
    "👋 Привет! Отправьте мне вашу концертную программу (.docx).\n\n"
    "Я сохраню файл, залогирую и верну обработанную версию для проверки."
)

# ==== Хендлеры ====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logger.info(f"/start от @{getattr(user, 'username', None)} (id={user.id})")
    await update.message.reply_text(WELCOME_TEXT)

def _is_docx(document: Document) -> bool:
    return (
        document
        and (
            (document.file_name or "").lower().endswith(".docx")
            or (document.mime_type or "").endswith("wordprocessingml.document")
        )
    )

async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    user = update.effective_user
    if not message or not message.document:
        return

    doc: Document = message.document
    if not _is_docx(doc):
        logger.info(f"Пользователь @{getattr(user, 'username', None)} прислал не .docx: {doc.file_name} ({doc.mime_type})")
        await message.reply_text("⚠️ Нужен .docx файл. Отправьте документ Word.")
        return

    logger.info(
        f"Получен .docx от @{getattr(user, 'username', None)} (id={user.id}): "
        f"name='{doc.file_name}', size={doc.file_size} bytes, mime='{doc.mime_type}'"
    )

    file = await context.bot.get_file(doc.file_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_name = f"{timestamp}__{doc.file_name or 'program.docx'}"
    local_path = DATA_DIR / local_name
    await file.download_to_drive(local_path.as_posix())
    logger.info(f"📥 Файл сохранён: {local_path}")

    processed_path = DATA_DIR / f"processed_{local_name}"
    try:
        processed_path.write_bytes(local_path.read_bytes())
        logger.info(f"🛠 Обработанный файл подготовлен: {processed_path}")
    except Exception as e:
        logger.exception(f"Ошибка при подготовке файла: {e}")
        await message.reply_text("❌ Ошибка при обработке файла.")
        return

    try:
        await message.reply_document(
            document=processed_path.open("rb"),
            filename=processed_path.name,
            caption="✅ Файл получен и возвращён обратно. (Smoke-test)",
        )
        logger.info(f"📤 Файл отправлен пользователю @{getattr(user, 'username', None)}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке файла: {e}")
        await message.reply_text("Файл обработан, но не удалось отправить обратно.")

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception(f"Исключение в обработчике: {context.error}")

# ==== Запуск приложения (СИНХРОННЫЙ) ====
def main() -> None:
    logger.info("🚀 Запуск Telegram-бота...")

    # опционально поднимем health-check сервер для Koyeb
    start_health_server()

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(lambda _: logger.info("✅ Application инициализирована"))
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))
    app.add_error_handler(on_error)

    logger.info("📡 Переходим в режим polling...")
    # ВАЖНО: это синхронный вызов, без await/asyncio.run
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )

# ==== Точкаs входа ====
if __name__ == "__main__":
    main()
