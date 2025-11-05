import os
import sys
import json
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

# from utils.docx_reader import read_program
from utils.validator import generate_program_variants
# from utils.docx_writer import save_program_to_docx  # включим, когда будет готов

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
    logger.info(f"🔑 Токен найден. Длина: {len(TOKEN)} символов.")


# ============================================================
# 🔹 ОБРАБОТЧИКИ
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"/start от @{user.username} (id={user.id})")
    await update.message.reply_text(
        "👋 Привет! Отправь мне .docx с программой концерта — я соберу её по правилам, "
        "проверю конфликты актёров и при необходимости вставлю тянучки."
    )


async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает docx, парсит, валидирует и возвращает результат"""
    user = update.effective_user
    doc = update.message.document

    if not doc or not doc.file_name.lower().endswith(".docx"):
        await update.message.reply_text("⚠️ Отправь файл в формате .docx, пожалуйста.")
        return

    logger.info(f"📄 Получен файл от @{user.username}: {doc.file_name}")
    file = await doc.get_file()

    os.makedirs("data", exist_ok=True)
    local_path = Path(f"data/{Path(doc.file_name).stem}_{user.id}.docx")
    await file.download_to_drive(local_path)
    logger.info(f"📥 Файл сохранён: {local_path}")

    try:
        # 1️⃣ Парсинг DOCX
        data = read_program(local_path)
        logger.info(f"✅ Успешно прочитано {len(data)} строк программы.")
        logger.debug(json.dumps(data, indent=2, ensure_ascii=False))

        # 2️⃣ Валидация и сборка программы
        logger.info("⚙️ Начинаем валидацию и поиск допустимых перестановок...")
        variants, tcount = generate_program_variants(data)

        if not variants:
            logger.error("🚫 Не удалось собрать ни одного корректного варианта.")
            await update.message.reply_text("❌ Не удалось собрать программу даже с тянучками.")
            return

        # 3️⃣ Выбор первого подходящего варианта
        result = variants[0]
        total_numbers = len(result)
        anchors = len([x for x in result if (x.get('type') or '').lower() in ['предкулисье', 'спонсоры']])

        if tcount == 0:
            msg = (
                f"🎉 Программа успешно собрана без тянучек!\n"
                f"Всего номеров: {total_numbers} (включая {anchors} якорных)."
            )
        else:
            msg = (
                f"✅ Программа готова!\n"
                f"Добавлено тянучек: {tcount}\n"
                f"Всего номеров: {total_numbers} (включая {anchors} якорных)."
            )

        logger.success(f"🎬 Итоговый вариант сформирован: {total_numbers} строк, {tcount} тянучек.")

        # 4️⃣ Сохранение в DOCX (когда будет готов docx_writer)
        # result_path = save_program_to_docx(result, f"data/output_{user.id}.docx")
        # await update.message.reply_document(open(result_path, "rb"), caption=msg)

        # Пока отправляем JSON и текст
        await update.message.reply_text(msg)
        short_preview = "\n".join(f"{i+1}. {r['title']}" for i, r in enumerate(result[:10]))
        await update.message.reply_text(
            f"🧾 Первые 10 строк программы:\n{short_preview}"
            + ("\n…" if len(result) > 10 else "")
        )

        logger.debug("📦 Итоговая программа:\n" + json.dumps(result, indent=2, ensure_ascii=False))

    except Exception as e:
        logger.exception(f"Ошибка при обработке docx: {e}")
        await update.message.reply_text(f"❌ Ошибка при обработке файла:\n{e}")


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
