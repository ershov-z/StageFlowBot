# core/conflicts.py
from __future__ import annotations
from core.types import Block


def _is_perf_pair(a: Block, b: Block) -> bool:
    """Возвращает True, если оба блока — номера (performance)."""
    return a.type == "performance" and b.type == "performance"


def _shared_actors(a: Block, b: Block) -> set[str]:
    """Возвращает множество общих актёров между двумя блоками."""
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
    - общий актёр в соседних performance-блоках.
    
    Исключения:
        • у актёра из a есть тег 'early' → может быть в b
        • у актёра из b есть тег 'later' → может быть в a
        • любой из актёров имеет тег 'vo' → конфликта нет
    Конфликт считается слабым, если хотя бы один общий актёр
    не подпадает под исключения.
    """
    if not _is_perf_pair(a, b):
        return False

    # Сильный конфликт приоритетен
    if strong_conflict(a, b):
        return False

    shared = _shared_actors(a, b)
    if not shared:
        return False

    # Проверяем каждый общий актёр отдельно
    conflict_found = False
    for name in shared:
        actor_a = next(x for x in a.actors if x.name == name)
        actor_b = next(x for x in b.actors if x.name == name)

        # Игнорируем vo (озвучку)
        if "vo" in actor_a.tags or "vo" in actor_b.tags:
            continue

        # Если у a есть early → допустимо
        if "early" in actor_a.tags:
            continue

        # Если у b есть later → допустимо
        if "later" in actor_b.tags:
            continue

        # Ни одно исключение не сработало → конфликт
        conflict_found = True
        break

    return conflict_found


def kv_conflict(a: Block, b: Block) -> bool:
    """
    kv:true не может соседствовать с kv:true.
    Оставлено для совместимости API.
    """
    if not _is_perf_pair(a, b):
        return False
    return a.kv and b.kv
