# stageflow_v2/bot/file_manager.py
import os
import io
import uuid
import tempfile
import logging
import zipfile
import asyncio
import time
from pathlib import Path
from aiogram import Bot
from aiogram.types import Document
from aiofiles import open as aio_open

from core.exporter import export_all
from service.logger import get_logger

log = get_logger("stageflow.file_manager")

# ----------------------------------------------------
# 🔧 Временные директории
# ----------------------------------------------------
BASE_TMP = tempfile.gettempdir()
DOWNLOAD_DIR = os.path.join(BASE_TMP, "stageflow_downloads")
RESULTS_DIR = os.path.join(BASE_TMP, "stageflow_results")


# ----------------------------------------------------
# 📁 Создание директорий
# ----------------------------------------------------
async def ensure_dirs() -> None:
    """Создаёт временные директории, если их нет."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)


# ----------------------------------------------------
# 📥 Загрузка .docx
# ----------------------------------------------------
async def download_docx(bot: Bot, document: Document) -> str:
    """Скачивает docx-файл, присланный пользователем, во временную директорию."""
    await ensure_dirs()
    file_info = await bot.get_file(document.file_id)

    unique_name = f"{uuid.uuid4()}_{document.file_name}"
    local_path = os.path.join(DOWNLOAD_DIR, unique_name)

    try:
        file_data = await bot.download_file(file_info.file_path)
        data = file_data.read()
        async with aio_open(local_path, "wb") as f:
            await f.write(data)
        log.info(f"📂 Файл сохранён: {local_path}")
        return local_path
    except Exception as e:
        log.exception(f"Ошибка при загрузке файла {document.file_name}: {e}")
        raise


# ----------------------------------------------------
# 🧹 Очистка временных файлов
# ----------------------------------------------------
async def cleanup_old_files(hours: int = 2) -> None:
    """Удаляет временные файлы старше указанного количества часов."""
    await ensure_dirs()
    cutoff = time.time() - hours * 3600

    for folder in (DOWNLOAD_DIR, RESULTS_DIR):
        for filename in os.listdir(folder):
            path = os.path.join(folder, filename)
            try:
                stat = os.stat(path)
                if stat.st_mtime < cutoff:
                    os.remove(path)
                    log.debug(f"🧹 Удалён старый файл: {path}")
            except Exception as e:
                log.warning(f"Не удалось удалить {path}: {e}")


# ----------------------------------------------------
# 🧩 Экспорт и упаковка вариантов
# ----------------------------------------------------
async def export_variants(arrangements, template_path: Path) -> io.BytesIO:
    """Экспортирует 5 вариантов программы и возвращает ZIP-буфер."""
    await ensure_dirs()

    try:
        export_dir = Path(RESULTS_DIR) / f"export_{uuid.uuid4().hex[:8]}"
        export_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"🧾 Начало экспорта пяти вариантов → {export_dir}")

        zip_path = export_all(arrangements, template_path, export_dir)

        # Читаем ZIP в память для отправки
        with open(zip_path, "rb") as f:
            buffer = io.BytesIO(f.read())
        buffer.seek(0)
        os.remove(zip_path)  # очистка после упаковки

        log.info(f"📦 Готов ZIP для отправки: {zip_path}")
        return buffer

    except Exception as e:
        log.exception(f"Ошибка при экспорте вариантов: {e}")
        raise


# ----------------------------------------------------
# 📦 Ручная архивация (для тестов)
# ----------------------------------------------------
async def zip_results(file_paths: list[str], zip_name: str = "StageFlow_Results.zip") -> io.BytesIO:
    """Упаковывает список файлов в ZIP и возвращает буфер BytesIO."""
    buffer = io.BytesIO()
    try:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for path in file_paths:
                arcname = os.path.basename(path)
                zipf.write(path, arcname=arcname)
        buffer.seek(0)
        log.info(f"📦 Упаковано {len(file_paths)} файлов в архив {zip_name}")
        return buffer
    except Exception as e:
        log.exception(f"Ошибка при упаковке ZIP: {e}")
        raise


# ----------------------------------------------------
# 🧰 Утилита для CLI и тестов
# ----------------------------------------------------
def get_temp_paths() -> dict:
    """Возвращает текущие пути временных директорий."""
    return {
        "downloads": DOWNLOAD_DIR,
        "results": RESULTS_DIR,
        "base_tmp": BASE_TMP,
    }
