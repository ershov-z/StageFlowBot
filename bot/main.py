import asyncio
import logging
import os
import json
import uuid
import tempfile
from io import BytesIO
from pathlib import Path
import threading
import requests
from flask import Flask

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile
from aiogram.filters import CommandStart, Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

from bot import file_manager, responses
from core.parser import parse_docx
from core.optimizer import stochastic_branch_and_bound
from core.validator import validate_arrangement
from service.seeds import generate_seeds

# === Инициализация бота ===
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("❌ BOT_TOKEN not found in environment variables")

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stageflow.main")

# === Flask healthcheck ===
app = Flask(__name__)

@app.route("/health")
def health():
    return {"status": "ok"}, 200


def start_flask():
    """Запускает Flask сервер в отдельном потоке."""
    port = int(os.getenv("PORT", 8080))
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True
    ).start()
    logger.info(f"🌐 Flask healthcheck сервер запущен на порту {port}")


# === Авто-пинг приложения каждые 2 минуты ===
async def self_ping_loop():
    app_url = os.getenv("APP_URL")
    if not app_url:
        logger.warning("⚠️ APP_URL не задан, пинг отключён.")
        return
    while True:
        try:
            requests.get(app_url + "/health", timeout=10)
            logger.info("🔁 Self-ping → /health OK")
        except Exception as e:
            logger.warning(f"⚠️ Self-ping error: {e}")
        await asyncio.sleep(120)


# === Обработчики ===
@dp.message(CommandStart())
async def start_command(message: types.Message):
    await message.answer(responses.start_message())


@dp.message(Command(commands=["help"]))
async def help_command(message: types.Message):
    await message.answer(responses.help_message())


@dp.message(lambda msg: msg.document and msg.document.file_name.endswith(".docx"))
async def handle_docx(message: types.Message):
    document = message.document
    file_name = document.file_name
    logger.info(f"📄 Получен файл: {file_name}")
    await message.answer(responses.processing_message())

    try:
        # === 1. Скачиваем файл ===
        file_path = await file_manager.download_docx(bot, document)
        logger.info(f"✅ Файл сохранён: {file_path}")

        # === 2. Парсим документ ===
        program = parse_docx(file_path)
        blocks = program.blocks
        logger.info(f"📊 Извлечено блоков: {len(blocks)}")

        # 💾 Сохраняем parsed.json
        parsed_path = Path(tempfile.gettempdir()) / f"parsed_{uuid.uuid4().hex[:6]}.json"
        with open(parsed_path, "w", encoding="utf-8") as f:
            json.dump(
                [
                    {
                        "id": b.id,
                        "name": b.name,
                        "type": b.type,
                        "kv": b.kv,
                        "fixed": b.fixed,
                        "actors": [{"name": a.name, "tags": a.tags} for a in b.actors],
                    }
                    for b in blocks
                ],
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info(f"💾 Сохранён parsed.json: {parsed_path}")

        # 📤 Отправляем parsed.json пользователю
        try:
            with open(parsed_path, "rb") as f:
                json_bytes = f.read()
            json_file = BufferedInputFile(json_bytes, filename="parsed.json")
            await message.answer_document(
                document=json_file,
                caption="📄 Вот как я распознал программу из твоего файла."
            )
            await asyncio.sleep(1)  # ждём отправку
            logger.info("📤 parsed.json отправлен пользователю.")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить parsed.json: {e}")

        # === 3. Генерируем варианты ===
        seeds = generate_seeds(5)
        arrangements = []
        for seed in seeds:
            arranged = await stochastic_branch_and_bound(blocks, seed)
            if validate_arrangement(arranged):
                arrangements.append(
                    type("Arrangement", (), {"blocks": arranged, "seed": seed})
                )

        if not arrangements:
            await message.answer(responses.validation_failed_message())
            return

        # === 4. Экспортируем ===
        template_path = Path(file_path)
        zip_buffer = await file_manager.export_variants(arrangements, template_path)

        # === 5. Добавляем parsed.json в архив ===
        with open(parsed_path, "rb") as f:
            parsed_bytes = f.read()

        final_zip = BytesIO()
        import zipfile
        zip_buffer.seek(0)
        with zipfile.ZipFile(zip_buffer, "r") as src_zip, zipfile.ZipFile(final_zip, "w", zipfile.ZIP_DEFLATED) as dst_zip:
            for item in src_zip.infolist():
                dst_zip.writestr(item, src_zip.read(item.filename))
            dst_zip.writestr("parsed.json", parsed_bytes)
        final_zip.seek(0)

        # === 6. Отправляем архив ===
        result_file = BufferedInputFile(final_zip.getvalue(), filename="StageFlow_Results.zip")
        await message.answer_document(document=result_file, caption=responses.success_message())

    except Exception as e:
        logger.exception("❌ Ошибка при обработке файла")
        await message.answer(responses.internal_error_message())
        await message.answer(f"<code>{e}</code>")


@dp.message()
async def fallback(message: types.Message):
    await message.answer(responses.unknown_message())


# === Главная точка входа ===
async def main():
    logger.info("🤖 StageFlow Bot запущен.")
    start_flask()
    asyncio.create_task(self_ping_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
