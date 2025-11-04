import os
from datetime import datetime
from pathlib import Path
from threading import Thread

from dotenv import load_dotenv
from loguru import logger
from telegram import Update, Document
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ==== Health-check сервер (опционально для Koyeb) ====
ENABLE_HEALTH_SERVER = True
HEALTH_PORT = int(os.getenv("PORT", "8000"))

def start_health_server():
    """Простой HTTP-сервер, чтобы Koyeb видел, что бот жив."""
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
    logger.info(f"Health-check сервер запущен на порту {HEALTH_PORT}")


# ==== Пути ====
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ==== Логирование ====
logger.remove()
logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO")
logger.add(
    LOGS_DIR / "bot_{time:YYYYMMDD}.log",
    rotation="10 MB",
    retention="10 days",
    level="INFO",
    encoding="utf-8",
)

# ==== Окружение ====
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN не найден! Укажи его в .env или переменных окружения.")
    raise SystemExit(1)

WELCOME_TEXT = (
    "👋 Привет! Отправьте мне вашу концертную программу (.docx).\n\n"
    "Я сохраню файл, залогирую и верну обработанную версию для проверки."
)

# ==== ХЕНДЛЕРЫ (они асинхронные, как требует PTB) ====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = update.effective_user

    if not message or not message.document:
        return

    doc: Document = message.document
    if not _is_docx(doc):
        logger.info(f"@{getattr(user, 'username', None)} прислал не .docx: {doc.file_name} ({doc.mime_type})")
        await message.reply_text("⚠️ Отправь, пожалуйста, .docx файл.")
        return

    logger.info(
        f"Получен .docx от @{getattr(user, 'username', None)} (id={user.id}): "
        f"name='{doc.file_name}', size={doc.file_size} bytes, mime='{doc.mime_type}'"
    )

    # Скачиваем
    file = await context.bot.get_file(doc.file_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_name = f"{timestamp}__{doc.file_name or 'program.docx'}"
    local_path = DATA_DIR / local_name
    await file.download_to_drive(local_path.as_posix())
    logger.info(f"📥 Сохранён файл: {local_path}")

    # Обрабатываем (пока просто копия)
    processed_path = DATA_DIR / f"processed_{local_name}"
    processed_path.write_bytes(local_path.read_bytes())
    logger.info(f"🛠 Подготовлен файл: {processed_path}")

    # Отправляем обратно
    await message.reply_document(
        document=processed_path.open("rb"),
        filename=processed_path.name,
        caption="✅ Файл принят и возвращён обратно (smoke-test).",
    )
    logger.info(f"📤 Отправлен обратно пользователю @{getattr(user, 'username', None)}")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception(f"Ошибка в обработчике: {context.error}")


# ==== СИНХРОННЫЙ MAIN ====
def main() -> None:
    logger.info("🚀 Запуск Telegram-бота...")

    start_health_server()  # health-check (можно убрать)

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
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


# ==== ТОЧКА ВХОДА ====
if __name__ == "__main__":
    main()
