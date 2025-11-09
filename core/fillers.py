# core/fillers.py
from __future__ import annotations

import logging
from typing import Optional

from core.types import Block, Actor

log = logging.getLogger("stageflow.fillers")

# ============================================================
# 🎭 Приоритет актёров для тянучек
# ============================================================
FILLER_PRIORITY = ["Пушкин", "Исаев"]  # Рожков исключён


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
    - актёр есть в следующем номере и не имеет тегов later или vo.
    """
    # Проверяем, если актёр есть в следующем блоке
    if _actor_in_block(next, actor_name):
        # Если у него нет тегов later и vo — запрещено
        if not (
            _has_actor_with_tag(next, actor_name, "later")
            or _has_actor_with_tag(next, actor_name, "vo")
        ):
            log.debug(f"🚫 {actor_name}: в следующем блоке без 'later'/'vo' ({next.name})")
            return False

    return True


# ============================================================
# 🎯 Основная функция выбора актёра
# ============================================================
def pick_filler_actor(prev: Block, next: Block, seed: int) -> Optional[str]:
    """
    Выбирает актёра для тянучки между блоками prev и next.
    Приоритет: Пушкин → Исаев.
    Возвращает имя актёра или None, если никто не подходит.
    """
    for name in FILLER_PRIORITY:
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
        actors=[Actor("Пушкин"), Actor("Исаев", ["vo"])],
    )
    next = Block(
        id=2,
        name="Номер 2",
        type="performance",
        actors=[Actor("Исаев", ["later"]), Actor("Пушкин")],
    )
    print(pick_filler_actor(prev, next, seed=42))
