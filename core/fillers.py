# core/fillers.py
from __future__ import annotations
import random
import logging
from typing import Optional
from core.types import Block, Actor

log = logging.getLogger("stageflow.fillers")

# ============================================================
# 🎭 Приоритет актёров для тянучек
# ============================================================
FILLER_PRIORITY = ["Пушкин", "Исаев", "Рожков"]


# ============================================================
# 🧩 Проверки на допустимость актёра для тянучки
# ============================================================
def _has_actor_with_tag(block: Block, actor_name: str, tag: str) -> bool:
    """Проверяет, есть ли у актёра указанный тег в блоке"""
    for a in block.actors:
        if a.name.lower() == actor_name.lower() and tag in a.tags:
            return True
    return False


def _actor_in_block(block: Block, actor_name: str) -> bool:
    """Проверяет, участвует ли актёр в блоке"""
    return any(a.name.lower() == actor_name.lower() for a in block.actors)


def _is_actor_allowed(prev: Block, next: Block, actor_name: str) -> bool:
    """
    Проверяет, можно ли использовать актёра в тянучке.
    Нельзя, если:
    - актёр есть в следующем номере с тегом gk
    - актёр есть в предыдущем номере с тегом gk
    - актёр есть в следующем номере без тега later
    """
    if _has_actor_with_tag(next, actor_name, "gk"):
        log.debug(f"🚫 {actor_name}: gk в следующем блоке ({next.name})")
        return False

    if _has_actor_with_tag(prev, actor_name, "gk"):
        log.debug(f"🚫 {actor_name}: gk в предыдущем блоке ({prev.name})")
        return False

    if _actor_in_block(next, actor_name) and not _has_actor_with_tag(next, actor_name, "later"):
        log.debug(f"🚫 {actor_name}: в следующем блоке без 'later' ({next.name})")
        return False

    return True


# ============================================================
# 🎯 Основная функция выбора актёра
# ============================================================
def pick_filler_actor(prev: Block, next: Block, seed: int) -> Optional[str]:
    """
    Выбирает актёра для тянучки между блоками prev и next.
    Приоритет: Пушкин → Исаев → Рожков, но порядок внутри приоритета
    перемешивается по seed для разнообразия.
    Возвращает имя актёра или None, если никто не подходит.
    """
    rng = random.Random(seed)
    candidates = FILLER_PRIORITY.copy()
    rng.shuffle(candidates)

    for name in candidates:
        if _is_actor_allowed(prev, next, name):
            log.info(f"✅ Выбран актёр для тянучки: {name}")
            return name

    log.warning("⚠ Не найден допустимый актёр для тянучки — конфликт повышается до сильного.")
    return None


# ============================================================
# 🧪 Тест (локальный)
# ============================================================
if __name__ == "__main__":
    prev = Block(
        id=1,
        name="Номер 1",
        type="performance",
        actors=[Actor("Пушкин"), Actor("Рожков", ["gk"])],
    )
    next = Block(
        id=2,
        name="Номер 2",
        type="performance",
        actors=[Actor("Исаев", ["later"]), Actor("Пушкин")],
    )
    print(pick_filler_actor(prev, next, seed=42))
