# service/hash_utils.py
import hashlib
import json
from typing import Set, Any
from service.logger import get_logger

log = get_logger("stageflow.hash_utils")


def arrangement_hash(arrangement: Any) -> str:
    """
    Возвращает стабильный хэш варианта программы.
    Основан на JSON-представлении без учёта seed-полей.
    """
    try:
        if isinstance(arrangement, list):
            serializable = [
                {
                    "id": b.id,
                    "name": b.name,
                    "type": b.type,
                    "actors": sorted([a.name for a in b.actors]),
                    "kv": b.kv,
                    "fixed": b.fixed,
                }
                for b in arrangement
            ]
        else:
            serializable = arrangement

        data = json.dumps(serializable, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha1(data.encode("utf-8")).hexdigest()
        return digest
    except Exception as e:
        log.warning(f"⚠️ Ошибка при расчёте хэша: {e}")
        return "INVALID_HASH"


def is_duplicate(arrangement: Any, existing_hashes: Set[str]) -> bool:
    """Проверяет, существует ли уже такой вариант."""
    h = arrangement_hash(arrangement)
    return h in existing_hashes


def register_hash(arrangement: Any, existing_hashes: Set[str]) -> None:
    """Добавляет хэш варианта в множество известных."""
    h = arrangement_hash(arrangement)
    if h not in existing_hashes:
        existing_hashes.add(h)
        log.debug(f"💾 Зарегистрирован новый вариант: {h}")
    else:
        log.info(f"🔁 Повтор варианта: {h}")
