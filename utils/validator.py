from __future__ import annotations
from typing import List, Dict, Any, Tuple, Optional
from copy import deepcopy
from loguru import logger

Actor = Dict[str, Any]
Item = Dict[str, Any]


# -----------------------------
# Вспомогательные: актёры/теги
# -----------------------------
def _actor_names(item: Item) -> List[str]:
    """Список имён актёров из item['actors'] (уже нормализованных docx_reader'ом)."""
    names = []
    for a in item.get("actors", []):
        name = str(a.get("name", "")).strip()
        if name:
            names.append(name)
    return names


def _has_gk(item: Item, person: str) -> bool:
    """Есть ли у данного person тег (гк) в этом номере."""
    for a in item.get("actors", []):
        if str(a.get("name", "")).strip() == person:
            tags = {t.strip().lower() for t in a.get("tags", [])}
            if "gk" in tags:
                return True
    return False


def _overlap(prev: Item, nxt: Item) -> List[str]:
    """Пересечение актёров в соседних номерах (по именам)."""
    a = set(_actor_names(prev))
    b = set(_actor_names(nxt))
    both = sorted(list(a & b))
    return both


# -----------------------------
# Конфликты и тянучки
# -----------------------------
def _is_conflict(prev: Item, nxt: Item) -> bool:
    """Конфликт: есть общий актёр в соседних номерах."""
    return len(_overlap(prev, nxt)) > 0


def _choose_tyan_actor(prev: Item, nxt: Item) -> Optional[str]:
    """
    Выбор актёра для тянучки: Пушкин > Исаев > Рожков,
    запрещаем, если этот актёр с (гк) стоит в prev или nxt.
    """
    candidates = ["Пушкин", "Исаев", "Рожков"]
    for c in candidates:
        if not _has_gk(prev, c) and not _has_gk(nxt, c):
            return c
    return None  # теоретически маловероятно, но оставим защиту


def _make_tyanuchka(lead: str) -> Item:
    """Создаёт элемент тянучки с ведущим lead."""
    return {
        "order": None,            # пересчитаем позже
        "num": "",
        "title": "Тянучка",
        "actors_raw": "",
        "pp": "",
        "hire": "",
        "responsible": "",
        "kv": False,
        "type": "тянучка",
        "actors": [{"name": lead, "tags": []}],
    }


# -----------------------------
# Фиксация неизменяемых позиций
# -----------------------------
def _immutable_positions(data: List[Item]) -> Dict[str, int]:
    """
    Возвращает индексы элементов, которые нельзя двигать,
    строго по их текущему расположению.
    """
    n = len(data)
    idx = { "prelude": None, "first": None, "second": None,
            "penultimate": None, "sponsors": None, "last": None }

    # предкулисье
    for i, it in enumerate(data):
        if str(it.get("type", "")).lower() == "предкулисье":
            idx["prelude"] = i
            break

    # спонсоры
    sponsors_idx = None
    for i, it in enumerate(data):
        if str(it.get("type", "")).lower() == "спонсоры":
            sponsors_idx = i
            break
    idx["sponsors"] = sponsors_idx

    # первый/второй/последний/предпоследний — по текущему расположению
    if n >= 1:
        idx["first"] = 0
        idx["last"] = n - 1
    if n >= 2:
        idx["second"] = 1
    if n >= 2:
        idx["penultimate"] = n - 2

    # Если предкулисье есть и стоит не в нуле — всё равно оно фиксировано там, где стоит
    # (по условию: «основываться на расположении»).
    # Если предкулисье в нуле, то «first/second» сдвинутся фактически на 1/2 индекс,
    # но мы фиксируем именно те позиции, что есть сейчас, не переопределяем.

    # Фиксируем, что сами индексы — финальные "недвигаемые" места.
    return idx


def _fixed_index_set(idx: Dict[str, int], total: int) -> set:
    """Множество индексов, которые нельзя трогать."""
    fixed = set()
    for k in ("prelude", "first", "second", "penultimate", "sponsors", "last"):
        v = idx.get(k, None)
        if v is not None and 0 <= v < total:
            fixed.add(v)
    return fixed


# -----------------------------
# Перестановка для снижения конфликтов
# -----------------------------
def _try_local_swaps(seq: List[Item], fixed_idx: set, max_passes: int = 4) -> None:
    """
    Пытается локальными перестановками (swap соседних) уменьшить соседние конфликты.
    Не трогаем элементы на фиксированных индексах.
    Логируем каждое изменение.
    """
    n = len(seq)
    if n < 3:
        return

    def conflicts_count() -> int:
        c = 0
        for i in range(n - 1):
            if _is_conflict(seq[i], seq[i + 1]):
                c += 1
        return c

    before = conflicts_count()
    logger.debug(f"🔧 Начинаем локальные перестановки. Конфликтов до: {before}")

    improved = True
    passes = 0
    while improved and passes < max_passes:
        improved = False
        passes += 1
        for i in range(1, n - 1):  # пытаемся свапать (i, i+1)
            j = i + 1
            if i in fixed_idx or j in fixed_idx:
                continue

            # текущее количество конфликтов вокруг i и j
            local_before = 0
            for a, b in ((i - 1, i), (i, j), (j, j + 1)):
                if 0 <= a < n and 0 <= b < n and _is_conflict(seq[a], seq[b]):
                    local_before += 1

            seq[i], seq[j] = seq[j], seq[i]  # попробуем свап
            local_after = 0
            for a, b in ((i - 1, i), (i, j), (j, j + 1)):
                if 0 <= a < n and 0 <= b < n and _is_conflict(seq[a], seq[b]):
                    local_after += 1

            if local_after < local_before:
                improved = True
                logger.info(f"🔁 Перестановка: «{seq[i]['title']}» ⟷ «{seq[j]['title']}» (позиции {i}↔{j}) улучшила локальные конфликты {local_before}→{local_after}")
            else:
                # откатываем, если не помогло
                seq[i], seq[j] = seq[j], seq[i]

    after = 0
    for i in range(n - 1):
        if _is_conflict(seq[i], seq[i + 1]):
            after += 1
    logger.debug(f"🔧 Перестановки завершены. Конфликтов после: {after}")


# -----------------------------
# Главная функция
# -----------------------------
def generate_program_variants(data: List[Item]) -> Tuple[List[Item], int]:
    """
    Возвращает:
      - итоговый один вариант программы (список элементов)
      - количество вставленных тянучек

    Порядок действий:
      1) фиксируем недвигаемые индексы (по текущему расположению),
      2) пытаемся локальными перестановками убрать конфликты,
      3) если конфликт остался — вставляем тянучку по приоритету (Пушкин>Исаев>Рожков) с учётом (гк),
      4) пересчитываем order.
    """
    logger.info("🧩 Запуск валидации программы...")
    seq = deepcopy(data)

    # 1) фиксация индексов
    anchors = _immutable_positions(seq)
    fixed_idx = _fixed_index_set(anchors, len(seq))
    logger.debug(f"📌 Фиксированные индексы: {sorted(list(fixed_idx))} (по местам входа)")

    # 2) пробуем свопами убрать конфликты
    _try_local_swaps(seq, fixed_idx)

    # 3) вставляем тянучки, если остались конфликты
    tcount = 0
    i = 0
    while i < len(seq) - 1:
        cur, nxt = seq[i], seq[i + 1]
        if _is_conflict(cur, nxt):
            logger.info(f"⚠️ Конфликт между «{cur['title']}» и «{nxt['title']}».")
            # если хотя бы один из соседей стоит на фиксированном индексе, двигать нельзя — сразу тянучка
            if i in fixed_idx or (i + 1) in fixed_idx:
                lead = _choose_tyan_actor(cur, nxt) or "Пушкин"
                seq.insert(i + 1, _make_tyanuchka(lead))
                tcount += 1
                logger.info(f"➕ Вставлена тянучка ({lead}) между {cur['title']} и {nxt['title']}.")
                # сдвигаем дальше после тянучки
                i += 2
                continue

            # попробуем локальный своп (ещё раз, точечно), если не фикс
            seq[i], seq[i + 1] = seq[i + 1], seq[i]
            if _is_conflict(seq[i], seq[i + 1]):
                # своп не помог — откат и тянучка
                seq[i], seq[i + 1] = seq[i + 1], seq[i]
                lead = _choose_tyan_actor(cur, nxt) or "Пушкин"
                seq.insert(i + 1, _make_tyanuchka(lead))
                tcount += 1
                logger.info(f"➕ Вставлена тянучка ({lead}) между {cur['title']} и {nxt['title']} (свап не дал эффекта).")
                i += 2
            else:
                logger.info(f"✅ Локальная перестановка «{seq[i]['title']}» ⟷ «{seq[i+1]['title']}» устранила конфликт без тянучки.")
                i += 2  # перепрыгиваем свопнутую пару
        else:
            i += 1

    logger.success(f"🎯 Валидация завершена. Добавлено {tcount} тянучек.")

    # 4) пересчёт order (визуальная нумерация позиций в итоговой выдаче; поле num не трогаем)
    for pos, it in enumerate(seq, start=1):
        it["order"] = pos

    return seq, tcount
