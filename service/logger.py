# service/logger.py
import logging
import os
from datetime import datetime

try:
    import coloredlogs
except ImportError:
    coloredlogs = None

LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "stageflow.log")


def setup_logging(level=logging.INFO):
    """Настраивает систему логирования (файл + консоль)."""
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%H:%M:%S"

    # Основной логгер
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a"),
            logging.StreamHandler(),
        ],
    )

    if coloredlogs:
        coloredlogs.install(level=level, fmt=fmt, datefmt=datefmt)
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.info("🪵 Логирование инициализировано.")


def get_logger(name: str) -> logging.Logger:
    """Возвращает именованный логгер с уже настроенным форматированием."""
    return logging.getLogger(name)
