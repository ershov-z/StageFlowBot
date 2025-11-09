from __future__ import annotations
import logging
import shutil
from datetime import datetime
from pathlib import Path

from core.exporter import export_all_variants


logger = logging.getLogger("stageflow.file_manager")


# ===========================
# УТИЛИТЫ ПО ФАЙЛАМ/ДИРЕКТОРИЯМ
# ===========================

TMP_ROOT = Path("/tmp/stageflow")


def _ts() -> str:
    """Строка-временная метка для имён файлов/папок."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(path: Path) -> Path:
    """Гарантирует, что директория существует."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def user_root_dir(user_id: int | str) -> Path:
    """Корневая временная папка пользователя."""
    return TMP_ROOT / f"user_{user_id}"


def results_dir_for(user_id: int | str) -> Path:
    """Папка результатов для пользователя."""
    return ensure_dir(user_root_dir(user_id) / "results")


def uploads_dir_for(user_id: int | str) -> Path:
    """Папка входящих файлов (оригиналы) для пользователя."""
    return ensure_dir(user_root_dir(user_id) / "uploads")


def save_local_file(src: Path, user_id: int | str, dest_name: str | None = None) -> Path:
    """
    Сохранить загруженный пользователем файл в его uploads/.
    Возвращает путь к сохранённой копии.
    """
    up_dir = uploads_dir_for(user_id)
    dest = up_dir / (dest_name or src.name)
    ensure_dir(dest.parent)
    shutil.copy2(src, dest)
    logger.info(f"[FILE_MANAGER] Файл сохранён: {dest}")
    return dest


# Совместимость со старым названием (синхронный вариант)
save_uploaded_file_sync = save_local_file


def write_bytes(user_id: int | str, rel_path: str, data: bytes) -> Path:
    """Записать байты в /tmp/stageflow/user_{id}/{rel_path}."""
    target = ensure_dir((user_root_dir(user_id) / rel_path).parent) / Path(rel_path).name
    with open(target, "wb") as f:
        f.write(data)
    logger.info(f"[FILE_MANAGER] Записан файл: {target}")
    return target


def write_text(user_id: int | str, rel_path: str, text: str, encoding: str = "utf-8") -> Path:
    """Записать текст в /tmp/stageflow/user_{id}/{rel_path}."""
    target = ensure_dir((user_root_dir(user_id) / rel_path).parent) / Path(rel_path).name
    with open(target, "w", encoding=encoding) as f:
        f.write(text)
    logger.info(f"[FILE_MANAGER] Записан текстовый файл: {target}")
    return target


# ===========================
# ЭКСПОРТ ВАРИАНТОВ
# ===========================

def export_variants(arrangements, results_dir: Path) -> Path:
    """
    Экспортирует все варианты через НОВЫЙ интерфейс export_all_variants(arrangements, results_dir)
    и возвращает путь к ZIP-архиву с результатами.

    ВНИМАНИЕ: здесь больше НЕТ template_path и дополнительной ручной упаковки ZIP.
    export_all_variants сам формирует DOCX/JSON и собирает архив.
    """
    ensure_dir(results_dir)
    logger.info("[FILE_MANAGER] Экспорт вариантов через export_all_variants()")
    zip_path = export_all_variants(arrangements, results_dir)
    logger.info(f"[FILE_MANAGER] 📦 Экспорт завершён. Архив готов: {zip_path}")
    return zip_path


# ===========================
# ОЧИСТКА
# ===========================

def cleanup_user_workspace(user_id: int | str) -> None:
    """Полная очистка рабочей папки пользователя (/tmp/stageflow/user_{id})."""
    root = user_root_dir(user_id)
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
        logger.info(f"🧹 Очистка завершена: {root}")


def cleanup_path(path: Path) -> None:
    """Удалить произвольный путь (директорию/файл)."""
    try:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink(missing_ok=True)
        logger.info(f"🧹 Очистка завершена: {path}")
    except Exception as e:
        logger.warning(f"[FILE_MANAGER] Ошибка очистки {path}: {e}")

# ===========================
# АСИНХРОННАЯ ЗАГРУЗКА ФАЙЛОВ ИЗ TELEGRAM
# ===========================

async def save_uploaded_file(bot, document, user_id: int | str) -> Path:
    """
    Сохраняет файл, присланный пользователем, прямо из Telegram API.
    Возвращает путь к сохранённому файлу.
    """
    up_dir = uploads_dir_for(user_id)
    dest = up_dir / document.file_name
    ensure_dir(dest.parent)

    await bot.download(document, destination=dest)
    logger.info(f"[FILE_MANAGER] Файл сохранён из Telegram: {dest}")
    return dest

# ===========================
# ВСПОМОГАТЕЛЬНОЕ (НЕОБЯЗАТЕЛЬНО)
# ===========================

def prepare_results_dir(user_id: int | str) -> Path:
    """
    Создаёт свежую results/ с временной меткой внутри user_{id}.
    Можно использовать, если нужно разносить выгрузки по подпапкам.
    """
    base = results_dir_for(user_id)
    target = ensure_dir(base)  # оставляем плоскую структуру, как в логах проекта
    logger.info(f"[FILE_MANAGER] Директория результатов: {target}")
    return target
