# bot/main.py
from __future__ import annotations

import os
import json
import time
import asyncio
import logging
from pathlib import Path
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile
from aiohttp import web
import aiohttp

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

# Поддержка Render и Koyeb (универсально)
APP_URL = os.getenv("APP_URL")  # например: mybot.koyeb.app
RENDER_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME", APP_URL or "localhost")

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
from aiogram.client.default import DefaultBotProperties
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
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

        # ✅ Исправлено: используем напрямую export_all без двойного архивирования
        zip_path = export_all(valid_arrangements, template_path, results_dir)

        await message.answer(responses.EXPORT_DONE)
        await message.answer(responses.ARCHIVE_DONE)
        await message.answer_document(
            FSInputFile(zip_path),
            caption="📦 StageFlow — результаты работы"
        )
        await message.answer(responses.DONE)

    except Exception as e:
        logger.exception(f"Ошибка при обработке файла: {e}")
        error_path = user_dir / f"error_{time.strftime('%H%M%S')}.json"
        await save_json({"error": str(e)}, error_path)
        await message.answer(responses.ERROR_MESSAGE.format(error=e))
        await message.answer_document(FSInputFile(error_path), caption="⚠️ Отладочная информация")

    finally:
        try:
            await cleanup_temp(user_dir, keep_results=True)
        except Exception as e:
            logger.warning(f"Не удалось очистить временные файлы: {e}")

# ============================================================
# 🌐 Webhook + healthcheck (aiohttp)
# ============================================================

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"https://{RENDER_HOSTNAME}{WEBHOOK_PATH}"

async def healthcheck(request):
    return web.Response(text="OK")

async def index(request):
    return web.json_response({"app": "StageFlow v2", "status": "running"})

# ============================================================
# ♻️ Автопинг (анти-сон)
# ============================================================
async def keep_alive():
    """Пингует /health каждые 4 минуты, чтобы Koyeb/Render не засыпал."""
    base_url = (APP_URL or RENDER_HOSTNAME).replace("https://", "").strip().rstrip("/")
    url = f"https://{base_url}/health"
    while True:
        await asyncio.sleep(240)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    logger.debug(f"Ping → {url} ({resp.status})")
        except Exception as e:
            logger.warning(f"Auto-ping failed: {e}")

# ============================================================
# 🔧 Исправленный on_startup с очисткой URL
# ============================================================
async def on_startup(app):
    # Даём Koyeb/Render время активировать домен
    await asyncio.sleep(10)
    base_url = (APP_URL or RENDER_HOSTNAME).replace("https://", "").strip().rstrip("/")
    webhook_url = f"https://{base_url}{WEBHOOK_PATH}"
    logger.info(f"📡 Устанавливаю webhook → {webhook_url}")

    try:
        await bot.set_webhook(webhook_url, drop_pending_updates=True)
        logger.info(f"🌐 Webhook установлен: {webhook_url}")
    except Exception as e:
        logger.error(f"❌ Ошибка при установке webhook: {e}")

    asyncio.create_task(keep_alive())

async def on_shutdown(app):
    try:
        await bot.session.close()
    finally:
        logger.info("🛑 Завершение без удаления webhook (сессия закрыта)")

def create_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/health", healthcheck)
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    setup_application(app, dp, bot=bot)
    return app

# ============================================================
# 🚀 Точка входа
# ============================================================
def main():
    app = create_app()
    logger.info(f"🚀 StageFlow webhook server запущен на {HOST}:{PORT}")
    web.run_app(app, host=HOST, port=PORT)

if __name__ == "__main__":
    main()
