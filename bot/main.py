import os
import asyncio
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from telegram import Update, Document
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters


# ==== Пути и директории ====
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


# ==== Настройка логов ====
logger.remove()  # убираем дефолтный sink
# лог в консоль
logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO")
# лог в файл с ротацией
logger.add(
    LOGS_DIR / "bot_{time:YYYYMMDD}.log",
    rotation="10 MB",
    retention="10 days",
    level="INFO",
    encoding="utf-8",
    backtrace=True,
    diagnose=True,
)


# ==== Настройки окружения ====
load_dotenv()  # локально читаем .env
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN не найден. Добавь его в .env или переменные окружения Render.")
    raise SystemExit(1)


WELCOME_TEXT = (
    "👋 Привет! Отправьте мне вашу концертную программу (.docx).\n\n"
    "Я сохраню файл, залогирую и верну обработанную версию для проверки."
)


# ==== Команды и обработчики ====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logger.info(f"/start от @{user.username} (id={user.id})")
    await update.message.reply_text(WELCOME_TEXT)


def _is_docx(document: Document) -> bool:
    """Проверяет, является ли файл .docx"""
    return (
        document
        and (
            (document.file_name or "").lower().endswith(".docx")
            or (document.mime_type or "").endswith("wordprocessingml.document")
        )
    )


async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Основная логика при получении файла от пользователя"""
    message = update.message
    user = update.effective_user

    if not message or not message.document:
        return

    doc: Document = message.document

    if not _is_docx(doc):
        logger.info(f"Пользователь @{user.username} прислал не .docx: {doc.file_name} ({doc.mime_type})")
        await message.reply_text("⚠️ Мне нужен именно .docx файл. Отправьте документ Word.")
        return

    # логируем метаданные файла
    logger.info(
        f"Получен .docx от @{user.username} (id={user.id}): "
        f"name='{doc.file_name}', size={doc.file_size} bytes, mime='{doc.mime_type}'"
    )

    # скачиваем файл во временную директорию
    file = await context.bot.get_file(doc.file_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_name = f"{timestamp}__{doc.file_name or 'program.docx'}"
    local_path = DATA_DIR / local_name
    await file.download_to_drive(local_path.as_posix())
    logger.info(f"📥 Файл сохранён: {local_path}")

    # === Здесь позже появится реальная обработка docx ===
    # пока делаем "эхо" — просто копируем как processed
    processed_path = DATA_DIR / f"processed_{local_name}"
    try:
        processed_path.write_bytes(local_path.read_bytes())
        logger.info(f"🛠 Обработанный файл подготовлен: {processed_path}")
    except Exception as e:
        logger.exception(f"Ошибка при подготовке файла: {e}")
        await message.reply_text("❌ Ошибка при обработке файла.")
        return

    # отправляем пользователю результат
    try:
        await message.reply_document(
            document=processed_path.open("rb"),
            filename=processed_path.name,
            caption="✅ Файл получен и возвращён обратно.\n(Базовая проверка работы бота.)",
        )
        logger.info(f"📤 Файл отправлен обратно пользователю @{user.username}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке файла: {e}")
        await message.reply_text("Файл обработан, но не удалось отправить обратно.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобальный перехватчик ошибок"""
    logger.exception(f"Исключение в обработчике: {context.error}")


# ==== Основной цикл приложения ====
async def main() -> None:
    logger.info("🚀 Запуск Telegram-бота...")

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
    await app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        close_loop=False,
        drop_pending_updates=True,
    )


# ==== Точка входа ====
if __name__ == "__main__":
    import asyncio
    import sys

    try:
        # Для Windows Telegram-бота запускаем только в отдельном процессе,
        # никакие патчи nest_asyncio не применяем.
        asyncio.run(main())
    except RuntimeError as e:
        print("\n❌ Ошибка цикла asyncio. "
              "Запусти эту команду в отдельном окне PowerShell, "
              "а не через VS Code / IPython.")
        print(e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("🛑 Остановка по Ctrl+C")
