import os
import sys
import json
from datetime import datetime, timedelta
from threading import Thread
from pathlib import Path
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
# 🔧 ЛОГИРОВАНИЕ
# ============================================================

os.makedirs("logs", exist_ok=True)
logger.add("logs/bot_{time:YYYYMMDD}.log", rotation="10 MB", level="INFO")


# ============================================================
# 🔐 ТОКЕН
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or ""
TOKEN = TELEGRAM_TOKEN.strip()

if not TOKEN:
    logger.error("❌ Не найден TELEGRAM_TOKEN (или BOT_TOKEN).")
    sys.exit(1)
else:
    logger.info(f"🔑 Токен найден, длина: {len(TOKEN)}")


# ============================================================
# 🧹 ОЧИСТКА СТАРЫХ ФАЙЛОВ
# ============================================================

def cleanup_old_files(directory: str, days: int = 1):
    """Удаляет файлы старше указанного количества дней."""
    folder = Path(directory)
    if not folder.exists():
        return
    now = datetime.now()
    cutoff = now - timedelta(days=days)
    deleted = 0
    for file in folder.glob("*"):
        try:
            if file.is_file() and datetime.fromtimestamp(file.stat().st_mtime) < cutoff:
                file.unlink()
                deleted += 1
        except Exception as e:
            logger.warning(f"Не удалось удалить {file}: {e}")
    if deleted > 0:
        logger.info(f"🧹 Очищено {deleted} старых файлов в {directory}")


# ============================================================
# 💓 HEALTH CHECK SERVER (для Koyeb)
# ============================================================

def start_health_server():
    """Лёгкий Flask-сервер, чтобы Koyeb проходил health check"""
    app = Flask(__name__)

    @app.route("/")
    def health():
        return "OK", 200

    def run():
        port = int(os.getenv("PORT", 8000))
        logger.info(f"💓 Health-check сервер запущен на порту {port}")
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    Thread(target=run, daemon=True).start()


# ============================================================
# 🔹 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def _tags_to_symbols(tags: list[str]) -> str:
    if not tags:
        return ""
    result = []
    if "gk" in tags:
        result.append("(гк)")
    if "early" in tags:
        result.append("!")
    if "later" in tags:
        result.append("%")
    return "".join(result)


def _format_entry_line(idx: int, entry: dict) -> str:
    num = entry.get("num", "") or ""
    title = entry.get("title", "") or ""
    etype = (entry.get("type") or "").lower()
    kv = " 🏠КВ" if entry.get("kv") else ""
    tmark = "🧷" if etype == "тянучка" else "🎭"

    # актёры
    actors_chunks = []
    for a in entry.get("actors", []):
        name = a.get("name", "").strip()
        tag_sym = _tags_to_symbols(a.get("tags", []))
        actors_chunks.append(f"{name}{tag_sym}" if tag_sym else name)
    actors_str = ", ".join(actors_chunks) if actors_chunks else "—"

    # тип
    type_hint = ""
    if etype == "предкулисье":
        type_hint = " (предкулисье)"
    elif etype == "спонсоры":
        type_hint = " (спонсоры)"
    elif etype == "тянучка":
        type_hint = " (тянучка)"

    num_part = f"№{num}" if num else "—"
    return f"{idx:>2}. {tmark} {num_part} | {title}{type_hint}{kv}\n     👥 {actors_str}"


# ============================================================
# 🔹 ОБРАБОТЧИКИ
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"/start от @{user.username} (id={user.id})")
    await update.message.reply_text(
        "👋 Привет! Отправь .docx с программой концерта — я проверю её, "
        "переставлю при необходимости и добавлю тянучки.\n\n"
        "⚙️ Важно: не трогаю предкулисье, 1-й, 2-й, предпоследний, спонсоры и последний номера."
    )


async def handle_docx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    document = update.message.document

    if not document or not document.file_name.lower().endswith(".docx"):
        await update.message.reply_text("⚠️ Отправь файл в формате .docx, пожалуйста.")
        return

    logger.info(f"📄 Получен .docx от @{user.username}: {document.file_name}")
    file = await document.get_file()

    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = Path(f"data/{timestamp}__{document.file_name}")
    await file.download_to_drive(local_path)
    logger.info(f"📥 Файл сохранён: {local_path}")

    try:
        cleanup_old_files("data", days=1)
        cleanup_old_files("logs", days=3)

        data = read_program(local_path)
        logger.info(f"✅ Прочитано {len(data)} строк.")
        logger.debug(json.dumps(data, indent=2, ensure_ascii=False))

        variants, tcount = generate_program_variants(data)
        if not variants:
            await update.message.reply_text("❌ Не удалось собрать программу даже с тянучками.")
            return

        result = variants[0]
        logger.success(f"🎬 Итоговый вариант собран. Тянучек добавлено: {tcount}")

        lines = [_format_entry_line(i, e) for i, e in enumerate(result, start=1)]
        header = (
            "✅ Программа собрана!\n"
            f"Добавлено тянучек: {tcount}\n"
            f"Всего номеров: {len(result)}\n"
            "— — — — — — — — — — — — — —\n"
        )

        text = header + "\n".join(lines)
        MAX_LEN = 3900
        if len(text) <= MAX_LEN:
            await update.message.reply_text(text)
        else:
            await update.message.reply_text(header)
            chunk, size = [], 0
            for line in lines:
                if size + len(line) > MAX_LEN:
                    await update.message.reply_text("\n".join(chunk))
                    chunk, size = [], 0
                chunk.append(line)
                size += len(line)
            if chunk:
                await update.message.reply_text("\n".join(chunk))

        out_path = Path(f"data/output_{timestamp}_{user.id}.docx")
        save_program_to_docx(result, out_path)
        logger.info(f"📁 Итоговый DOCX сохранён: {out_path}")

        await update.message.reply_document(
            open(out_path, "rb"),
            caption=f"📄 Итоговый файл.\nТянучек добавлено: {tcount}."
        )

    except Exception as e:
        logger.exception(f"Ошибка при обработке docx: {e}")
        await update.message.reply_text(f"❌ Ошибка при обработке файла: {e}")


# ============================================================
# 🔹 ЗАПУСК
# ============================================================

def main():
    logger.info("🚀 Запуск Telegram-бота...")
    start_health_server()  # 💓 нужно для Koyeb

    cleanup_old_files("data", days=1)
    cleanup_old_files("logs", days=3)

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_docx))

    logger.info("📡 Переходим в режим polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
