# core/conflicts.py
from __future__ import annotations
from core.types import Block


def _is_perf_pair(a: Block, b: Block) -> bool:
    return a.type == "performance" and b.type == "performance"


def _shared_actors(a: Block, b: Block) -> set[str]:
    names_a = {actor.name for actor in a.actors}
    names_b = {actor.name for actor in b.actors}
    return names_a & names_b


def strong_conflict(a: Block, b: Block) -> bool:
    """
    Сильный конфликт:
    - kv:true рядом с kv:true
    Без исключений.
    """
    if not _is_perf_pair(a, b):
        return False

    return a.kv and b.kv


def weak_conflict(a: Block, b: Block) -> bool:
    """
    Слабый конфликт:
    - общий актёр в соседних performance-блоках
    Исключения:
        • у актёра из a есть тег 'early' → может быть в b
        • у актёра из b есть тег 'later' → может быть в a
        • любой из актёров имеет тег 'vo' → конфликта нет
    """
    if not _is_perf_pair(a, b):
        return False

    # сильный конфликт приоритетен
    if strong_conflict(a, b):
        return False

    shared = _shared_actors(a, b)
    if not shared:
        return False

    for name in shared:
        actor_a = next(x for x in a.actors if x.name == name)
        actor_b = next(x for x in b.actors if x.name == name)

        # если у любого есть vo → конфликта нет
        if "vo" in actor_a.tags or "vo" in actor_b.tags:
            continue
        # если a имеет early → может быть в b
        if "early" in actor_a.tags:
            continue
        # если b имеет later → может быть в a
        if "later" in actor_b.tags:
            continue

        # если ни одно исключение не сработало — конфликт
        return True

    return False


def kv_conflict(a: Block, b: Block) -> bool:
    """kv:true не может соседствовать с kv:true (для полноты API)."""
    if not _is_perf_pair(a, b):
        return False
    return a.kv and b.kv
