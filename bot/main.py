# bot/main.py
from __future__ import annotations

import os
import json
import time
import asyncio
import threading
import logging
from pathlib import Path
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile
from flask import Flask, jsonify

# --- core pipeline ---
from core.parser import parse_docx
from core.optimizer import generate_arrangements
from core.validator import validate_arrangement
from core.exporter import export_all

# --- bot utils ---
from bot import responses
from bot.file_manager import (
    save_uploaded_file,
    cleanup_temp,
    get_user_dir,
    get_results_dir,
    save_json,
    export_variants,
)

# --- service utils ---
from service.logger import setup_logging, get_logger

# ============================================================
# ⚙️ Конфигурация
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    print("⚠️  BOT_TOKEN не задан — установите переменную окружения BOT_TOKEN")

PORT = int(os.getenv("PORT", "8080"))
HOST = os.getenv("HOST", "0.0.0.0")
SELF_PING_INTERVAL = int(os.getenv("SELF_PING_INTERVAL", "240"))

WORK_DIR = Path(os.getenv("WORK_DIR", "/tmp/stageflow"))
WORK_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 🪵 Логирование
# ============================================================
setup_logging()
logger = get_logger("stageflow.main")
logger.info("🪵 Логирование инициализировано (через service.logger)")

# ============================================================
# 🤖 Настройка бота
# ============================================================
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# ============================================================
# 🧭 Команды
# ============================================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(responses.START_MESSAGE)


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(responses.HELP_MESSAGE)


# ============================================================
# 📄 Основная логика обработки .docx
# ============================================================

@dp.message(lambda m: m.document and m.document.file_name.lower().endswith(".docx"))
async def handle_docx(message: types.Message):
    user_id = message.from_user.id
    document = message.document

    await message.answer(responses.FILE_RECEIVED.format(name=document.file_name))
    await message.answer(responses.PARSING_STARTED)

    user_dir = get_user_dir(WORK_DIR, user_id)
    results_dir = get_results_dir(user_dir)

    try:
        # === 1️⃣ Сохраняем исходный файл ===
        saved_path = await save_uploaded_file(bot, document, user_dir)
        logger.info(f"📥 Получен файл: {saved_path}")

        # === 2️⃣ Парсинг ===
        program = parse_docx(str(saved_path))
        parsed_json_path = user_dir / f"parsed_{time.strftime('%H%M%S')}.json"

        parsed_payload = [
            {
                "id": b.id,
                "name": b.name,
                "type": b.type,
                "kv": b.kv,
                "fixed": b.fixed,
                "num": b.num,
                "actors_raw": b.actors_raw,
                "pp_raw": b.pp_raw,
                "hire": b.hire,
                "responsible": b.responsible,
                "actors": [{"name": a.name, "tags": list(a.tags)} for a in b.actors],
            }
            for b in program.blocks
        ]
        await save_json(parsed_payload, parsed_json_path)
        await message.answer(responses.PARSING_DONE)
        await message.answer_document(
            FSInputFile(parsed_json_path),
            caption="🧾 Распарсенный JSON (исходная таблица).",
        )

        # === 3️⃣ Генерация ===
        await message.answer(responses.OPTIMIZATION_STARTED)
        arrangements = await generate_arrangements(program.blocks)
        arrangements_json = user_dir / f"arrangements_{time.strftime('%H%M%S')}.json"
        await save_json([a.seed for a in arrangements], arrangements_json)
        await message.answer(responses.OPTIMIZATION_DONE.format(count=len(arrangements)))

        # === 4️⃣ Валидация ===
        await message.answer(responses.VALIDATION_STARTED)
        valid_arrangements = [a for a in arrangements if validate_arrangement(a.blocks)]
        valid_json = user_dir / f"validated_{time.strftime('%H%M%S')}.json"
        await save_json([a.seed for a in valid_arrangements], valid_json)
        await message.answer(responses.VALIDATION_DONE.format(count=len(valid_arrangements)))

        if not valid_arrangements:
            await message.answer("⚠️ Не найдено валидных вариантов. Использую лучший найденный.")
            valid_arrangements = arrangements[:1]

        # === 5️⃣ Экспорт и упаковка ===
        await message.answer(responses.EXPORT_STARTED)
        template_path = saved_path
        zip_path = export_variants(valid_arrangements, export_all, template_path, results_dir)
        await message.answer(responses.EXPORT_DONE)
        await message.answer(responses.ARCHIVE_DONE)
        await message.answer_document(FSInputFile(zip_path), caption="📦 StageFlow — результаты работы")
        await message.answer(responses.DONE)

    except Exception as e:
        logger.exception(f"Ошибка при обработке файла: {e}")
        error_path = user_dir / f"error_{time.strftime('%H%M%S')}.json"
        await save_json({"error": str(e)}, error_path)
        await message.answer(responses.ERROR_MESSAGE.format(error=e))
        await message.answer_document(FSInputFile(error_path), caption="⚠️ Отладочная информация")

    finally:
        # === 6️⃣ Очистка (с сохранением результатов) ===
        try:
            await cleanup_temp(user_dir, keep_results=True)
        except Exception as e:
            logger.warning(f"Не удалось очистить временные файлы: {e}")


# ============================================================
# 🌡️ Flask healthcheck + self-ping
# ============================================================
flask_app = Flask(__name__)

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@flask_app.get("/")
def index():
    return jsonify({"app": "StageFlow v2", "status": "running"}), 200


def _self_ping_loop(port: int, interval: int):
    """Периодический пинг Flask, чтобы Koyeb не засыпал."""
    import requests
    url = f"http://127.0.0.1:{port}/health"
    while True:
        try:
            r = requests.get(url, timeout=5)
            logger.info(f"🫀 Self-ping {url} → {r.status_code}")
        except Exception as e:
            logger.warning(f"Self-ping error: {e}")
        time.sleep(interval)


def _run_flask(port: int, host: str):
    """Поднимаем Flask в отдельном потоке."""
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    flask_app.run(host=host, port=port, debug=False, use_reloader=False)


# ============================================================
# 🚀 Запуск StageFlow
# ============================================================
async def start_bot():
    logger.info("🤖 StageFlow Bot запущен (aiogram polling).")
    await dp.start_polling(bot)


def main():
    flask_thread = threading.Thread(target=_run_flask, args=(PORT, HOST), daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask healthcheck запущен на http://{HOST}:{PORT}/health")

    pinger_thread = threading.Thread(target=_self_ping_loop, args=(PORT, SELF_PING_INTERVAL), daemon=True)
    pinger_thread.start()
    logger.info(f"🔁 Self-ping каждые {SELF_PING_INTERVAL} сек.")

    try:
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Остановка по сигналу.")
    except Exception as e:
        logger.exception(f"Критическая ошибка запуска бота: {e}")


if __name__ == "__main__":
    main()
