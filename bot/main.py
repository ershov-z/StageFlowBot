import os
import sys
import json
import tempfile
from pathlib import Path
from loguru import logger
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from utils.docx_reader import read_program
from utils.validator import generate_program_variants
from utils.docx_writer import save_program_to_docx  # появится позже, пока можно закомментировать

# ============================================================
# 🔧 НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================

os.makedirs("logs", exist_ok=True)
logger.add("logs/bot_{time:YYYYMMDD}.log", rotation="10 MB", level="INFO")

# ============================================================
# 🔹 ИНИЦИАЛИЗАЦИЯ БОТА
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or ""
TOKEN = TELEGRAM_TOKEN.strip()
if not TOKEN:
    logger.error("❌ Не найден TELEGRAM_TOKEN (или BOT_TOKEN).")
    sys.exit(1)
else:
    logger.info(f"🔑 Токен найден, длина: {len(TOKEN)}")


# ============================================================
# 🔹 ОБРАБОТЧИКИ
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"/start от @{user.username} (id={user.id})")
    await update.message.reply_text(
        "👋 Привет! Отправь мне .docx с программой концерта — я проверю, соберу и при необходимости добавлю тянучки."
    )


async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает docx, парсит, валидирует и возвращает результат"""
    user = update.effective_user
    document = update.message.document

    if not document.file_name.lower().endswith(".docx"):
        await update.message.reply_text("⚠️ Отправь файл в формате .docx, пожалуйста.")
        return

    logger.info(f"📄 Получен .docx от @{user.username}: {document.file_name}")
    file = await document.get_file()

    os.makedirs("data", exist_ok=True)
    local_path = Path(f"data/{Path(document.file_name).stem}_{user.id}.docx")
    await file.download_to_drive(local_path)
    logger.info(f"📥 Файл сохранён: {local_path}")

    try:
        # 1️⃣ Парсинг
        data = read_program(local_path)
        logger.info(f"✅ Успешно извлечено {len(data)} строк.")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))

        # 2️⃣ Генерация вариантов
        variants, tcount = generate_program_variants(data)

        if not variants:
            await update.message.reply_text("❌ Не удалось собрать программу даже с тянучками.")
            return

        result = variants[0]  # берём первый корректный вариант
        msg = (
            f"✅ Программа успешно собрана!\n"
            f"Добавлено тянучек: {tcount}.\n"
            f"Всего номеров: {len(result)}."
        )

        # 3️⃣ Логирование результата
        logger.info(f"🎬 Итоговый вариант с {tcount} тянучками сформирован.")

        # 4️⃣ Сохранение в новый DOCX (если реализовано)
        # result_path = save_program_to_docx(result, f"data/output_{user.id}.docx")
        # await update.message.reply_document(open(result_path, "rb"), caption=msg)

        # Пока просто возвращаем JSON
        pretty = json.dumps(result, indent=2, ensure_ascii=False)
        await update.message.reply_text(msg)
        logger.debug(pretty)

    except Exception as e:
        logger.exception(f"Ошибка при обработке docx: {e}")
        await update.message.reply_text(f"❌ Ошибка при обработке файла: {e}")


# ============================================================
# 🔹 ОСНОВНОЙ ЗАПУСК
# ============================================================

def main():
    logger.info("🚀 Запуск Telegram-бота...")

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))

    logger.info("📡 Переходим в режим polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
