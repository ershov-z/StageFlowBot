import asyncio
import logging
import os
from io import BytesIO
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile
from aiogram.filters import CommandStart, Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

from bot import file_manager

# === Инициализация бота ===
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("❌ BOT_TOKEN not found in environment variables")

# Новая форма инициализации (Aiogram ≥ 3.7.0)
bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# === Обработчики ===
@dp.message(CommandStart())
async def start_command(message: types.Message):
    await message.answer(
        "👋 Привет! Отправь мне .docx файл с программой концерта, "
        "и я подготовлю пять вариантов перестройки."
    )


@dp.message(Command(commands=["help"]))
async def help_command(message: types.Message):
    await message.answer(
        "📘 Отправь .docx файл, содержащий таблицу концертной программы.\n"
        "Бот создаст 5 идеальных вариантов перестройки и вернёт архив ZIP."
    )


@dp.message(lambda msg: msg.document and msg.document.file_name.endswith(".docx"))
async def handle_docx(message: types.Message):
    document = message.document
    file_name = document.file_name
    logger.info(f"Получен файл: {file_name}")

    try:
        # === 1. Скачиваем файл ===
        file_path = await file_manager.download_docx(bot, document)
        logger.info(f"Файл сохранён: {file_path}")

        # === 2. Генерируем варианты ===
        # Пока можно оставить заглушку, чтобы проверить отправку
        zip_buffer = BytesIO(b"Test ZIP")
        logger.info("ZIP с вариантами создан (тестовая заглушка)")

        # === 3. Отправляем пользователю ===
        zip_bytes = zip_buffer.getvalue()
        result_file = BufferedInputFile(zip_bytes, filename="variants.zip")
        await message.answer_document(result_file, caption="🎯 Вот 5 идеальных вариантов программы")

    except Exception as e:
        logger.exception("Ошибка при обработке файла")
        await message.answer(f"❌ Произошла ошибка: {e}")


@dp.message()
async def fallback(message: types.Message):
    await message.answer("Отправь мне .docx файл, чтобы я создал варианты программы.")


# === Точка входа ===
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
