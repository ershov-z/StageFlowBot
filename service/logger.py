# service/logger.py
import logging
import os
from datetime import datetime

try:
    import coloredlogs
except ImportError:
    coloredlogs = None

# ============================================================
# 🪵 Унифицированное логирование StageFlow v2
# ============================================================

LOG_DIR = os.path.join("/tmp", "logs")  # безопасно для Koyeb
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "stageflow.log")

FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DATEFMT = "%H:%M:%S"


def setup_logging(level=logging.INFO):
    """Настраивает систему логирования (файл + консоль)."""
    logging.basicConfig(
        level=level,
        format=FMT,
        datefmt=DATEFMT,
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a"),
            logging.StreamHandler(),
        ],
    )

    if coloredlogs:
        coloredlogs.install(level=level, fmt=FMT, datefmt=DATEFMT)

    # Уменьшаем шум от aiogram и urllib
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.info(f"🪵 Логирование инициализировано. Файл: {LOG_FILE}")


def get_logger(name: str) -> logging.Logger:
    """Возвращает именованный логгер с уже настроенным форматированием."""
    return logging.getLogger(name)


# 🔧 Инициализация при импорте (чтобы логи сразу работали)
setup_logging()
