# bot/file_manager.py
from __future__ import annotations
import os
import json
import shutil
import aiofiles
import zipfile
from datetime import datetime
from pathlib import Path
from aiogram import Bot, types

from service.logger import get_logger

logger = get_logger(__name__)

# ============================================================
# 🧭 Вспомогательные функции путей
# ============================================================

def get_user_dir(base: Path, user_id: int) -> Path:
    """Возвращает путь каталога пользователя."""
    d = base / f"user_{user_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_results_dir(user_dir: Path) -> Path:
    """Возвращает каталог результатов и создаёт его при необходимости."""
    d = user_dir / "results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def timestamp() -> str:
    """Строка-время для имён файлов."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ============================================================
# 📥 Приём и сохранение исходного файла
# ============================================================

async def save_uploaded_file(bot: Bot, document: types.Document, user_dir: Path) -> Path:
    """
    Сохраняет .docx из Telegram в каталог пользователя.
    Возвращает путь к сохранённому файлу.
    """
    user_dir.mkdir(parents=True, exist_ok=True)
    filename = document.file_name or f"input_{timestamp()}.docx"
    dest_path = user_dir / filename

    logger.info(f"📥 Скачиваю файл {filename} …")
    file_info = await bot.get_file(document.file_id)
    stream = await bot.download_file(file_info.file_path)

    async with aiofiles.open(dest_path, "wb") as f:
        await f.write(stream.read())

    logger.info(f"📂 Файл сохранён: {dest_path}")
    return dest_path


# ============================================================
# 💾 Сохранение промежуточных данных
# ============================================================

async def save_json(data, path: Path):
    """Асинхронно сохраняет объект как JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))
    logger.debug(f"💾 JSON сохранён: {path.name}")


def save_sync_json(data, path: Path):
    """Синхронная версия сохранения JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.debug(f"💾 JSON сохранён (sync): {path.name}")


# ============================================================
# 📦 Экспорт и упаковка результатов
# ============================================================

def copy_export_files(src_dir: Path, dst_dir: Path):
    """
    Копирует все файлы результатов (docx/json) в папку результатов пользователя.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    for f in src_dir.glob("*"):
        if f.is_file():
            shutil.copy2(f, dst_dir / f.name)
            logger.debug(f"📎 Скопирован: {f.name}")


def make_zip(export_dir: Path, archive_path: Path) -> Path:
    """Создаёт архив ZIP из всех файлов export_dir."""
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in export_dir.glob("*"):
            if file.is_file():
                zipf.write(file, arcname=file.name)
                logger.debug(f"📦 Добавлен: {file.name}")
    logger.info(f"🎁 Архив создан: {archive_path}")
    return archive_path


def export_variants(arrangements, exporter_func, template_path: Path, results_dir: Path) -> Path:
    """
    Экспортирует все варианты с помощью exporter_func (export_all) и возвращает путь к ZIP.
    """
    zip_path = results_dir / f"StageFlow_Results_{timestamp()}.zip"
    exporter_func(arrangements, template_path, results_dir)
    make_zip(results_dir, zip_path)
    logger.info(f"📦 Экспорт завершён. Архив: {zip_path}")
    return zip_path


# ============================================================
# 🧹 Очистка временных директорий
# ============================================================

async def cleanup_temp(user_dir: Path, keep_results: bool = True):
    """
    Удаляет все временные файлы пользователя.
    Если keep_results=True, папка results/ сохраняется.
    """
    if not user_dir.exists():
        return

    for item in user_dir.iterdir():
        try:
            if keep_results and item.is_dir() and item.name == "results":
                continue
            if item.is_file():
                item.unlink(missing_ok=True)
            else:
                shutil.rmtree(item, ignore_errors=True)
        except Exception as e:
            logger.warning(f"⚠️ Ошибка очистки {item}: {e}")

    logger.info(f"🧹 Очистка завершена: {user_dir}")


# ============================================================
# 🧪 Локальный тест (CLI)
# ============================================================

if __name__ == "__main__":
    base = Path("/tmp/stageflow_test")
    user = get_user_dir(base, 123)
    res = get_results_dir(user)
    print("Создан:", res)
    make_zip(res, res / "dummy.zip")
    print("ZIP готов.")
