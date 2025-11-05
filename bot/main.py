import os
import sys
import json
import threading
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
from utils.validator import generate_program_variants
from utils.docx_writer import save_program_to_docx

# ============================================================
# 🔧 НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
logger.add("logs/bot_{time:YYYYMMDD}.log", rotation="10 MB", level="DEBUG")

# ============================================================
# 🌐 HEALTH CHECK (для Koyeb)
# ============================================================

app_health = Flask(__name__)

@app_health.route("/")
def health_root():
    return "OK"

@app_health.route("/health")
def health_check():
    return {"status": "healthy"}, 200


def start_health_server():
    """Запускает Flask-сервер в отдельном потоке (порт 8000)"""
    def run():
        app_health.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    logger.info("💓 Health-check сервер запущен на порту 8000")


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
        "👋 Привет! Отправь мне .docx с программой концерта — я её проанализирую, "
        "переставлю номера и при необходимости добавлю тянучки."
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = Path(f"data/{timestamp}__{document.file_name}")
    await file.download_to_drive(local_path)
    logger.info(f"📥 Файл сохранён: {local_path}")

    try:
        # 1️⃣ ПАРСИНГ
        data = read_program(local_path)
        logger.info(f"✅ Прочитано {len(data)} строк.")
        parsed_json_path = Path(f"data/parsed_{timestamp}_{user.id}.json")
        with open(parsed_json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        await update.message.reply_document(
            open(parsed_json_path, "rb"),
            caption="📘 Исходные данные после парсинга:",
        )

        # 2️⃣ ВАЛИДАЦИЯ И ПЕРЕСТАНОВКИ
        variants, stats = generate_program_variants(data)

        initial_conflicts = stats.get("initial_conflicts", 0)
        final_conflicts = stats.get("final_conflicts", 0)
        tcount = stats.get("tyanuchki_added", 0)
        total_checked = stats.get("checked_variants", 0)

        if not variants:
            await update.message.reply_text("❌ Не удалось собрать программу даже с тянучками.")
            return

        result = variants[0]

        result_json_path = Path(f"data/result_{timestamp}_{user.id}.json")
        with open(result_json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        # Формирование отчёта
        tyan_titles = [x["title"] for x in result if x["type"] == "тянучка"]
        msg = (
            f"🎬 Программа успешно собрана!\n"
            f"Проверено перестановок: {total_checked}\n"
            f"Исходных конфликтов: {initial_conflicts}\n"
            f"Осталось конфликтов: {final_conflicts}\n"
            f"Добавлено тянучек: {tcount}\n"
            f"Всего номеров: {len(result)}.\n\n"
        )
        if tcount > 0:
            msg += "🧩 Добавлены тянучки:\n" + "\n".join(f"• {t}" for t in tyan_titles)
        else:
            msg += "✅ Программа собрана без тянучек!"

        # 3️⃣ СОХРАНЕНИЕ ИТОГОВОГО DOCX
        out_path = Path(f"data/output_{timestamp}_{user.id}.docx")
        save_program_to_docx(result, out_path)
        logger.success(f"🎯 Итоговый файл сохранён: {out_path}")

        # 4️⃣ ОТПРАВКА ПОЛЬЗОВАТЕЛЮ
        await update.message.reply_text("✅ Анализ завершён, готовлю отчёты...")
        await update.message.reply_document(open(result_json_path, "rb"), caption="📗 Итоговая программа (JSON):")
        await update.message.reply_document(open(out_path, "rb"), caption=msg)

    except Exception as e:
        logger.exception(f"Ошибка при обработке docx: {e}")
        await update.message.reply_text(f"❌ Ошибка при обработке файла: {e}")


# ============================================================
# 🔹 ОСНОВНОЙ ЗАПУСК
# ============================================================

def main():
    logger.info("🚀 Запуск Telegram-бота...")
    start_health_server()

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))

    logger.info("📡 Переходим в режим polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
