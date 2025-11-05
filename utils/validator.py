# utils/validator.py

from __future__ import annotations

from itertools import product
from typing import Dict, List, Tuple, Set, Any, Optional
from loguru import logger


# ===========================
# Константы / настройки
# ===========================

TYAN_LEADERS = ["Пушкин", "Исаев", "Рожков"]  # приоритет для тянучек
MAX_PERMUTATIONS_PER_SEGMENT = 2000           # защитный лимит генерации на сегмент
MAX_GLOBAL_VARIANTS = 2000                    # общий защитный лимит (после склейки сегментов)


# ===========================
# Вспомогательные
# ===========================

def _actor_tags_map(entry: Dict[str, Any]) -> Dict[str, Set[str]]:
    """Возвращает имя актёра -> набор тегов {'early','later','gk'} для номера."""
    m: Dict[str, Set[str]] = {}
    for a in entry.get("actors", []):
        name = a.get("name", "").strip()
        if not name:
            continue
        tags = set(a.get("tags", []))
        m.setdefault(name, set()).update(tags)
    return m


def _actor_names(entry: Dict[str, Any]) -> Set[str]:
    """Возвращает множество имён актёров (без тегов)."""
    return {a.get("name", "").strip() for a in entry.get("actors", []) if a.get("name", "").strip()}


def _has_tag(entry: Dict[str, Any], actor_name: str, tag: str) -> bool:
    """Проверяет, есть ли у конкретного актёра указанный тег в номере."""
    for a in entry.get("actors", []):
        if a.get("name", "").strip().lower() == actor_name.strip().lower():
            return tag in set(a.get("tags", []))
    return False


def _is_tyanuchka(entry: Dict[str, Any]) -> bool:
    return (entry.get("type") or "").lower() == "тянучка"


def _make_tyanuchka(actor_name: str) -> Dict[str, Any]:
    """Создаёт объект строки тянучки с заданным ведущим."""
    return {
        "order": 999999,  # неважно: в итоговый docx пишем в порядке списка
        "num": "",
        "title": "Тянучка",
        "actors_raw": actor_name,
        "pp": "",
        "hire": "",
        "responsible": "",
        "kv": False,
        "type": "тянучка",
        "actors": [{"name": actor_name, "tags": []}],
    }


def _choose_tyan_leader() -> str:
    # По ТЗ — всегда берём из приоритетного списка, независимо от их наличия в программе
    return TYAN_LEADERS[0]  # Пушкин (если захочешь — можно рандомизировать по приоритету)


def _can_place_tyan_between(a: Dict[str, Any], b: Dict[str, Any], actor_name: str) -> bool:
    """Можно ли вставить тянучку между A и B с данным ведущим.
    Ограничение: если у ведущего в A или B стоит (гк) — нельзя.
    """
    if _has_tag(a, actor_name, "gk"):
        return False
    if _has_tag(b, actor_name, "gk"):
        return False
    return True


# ===========================
# Правила валидности между соседями
# ===========================

def _valid_adjacent(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Проверка соседства A -> B по всем правилам, если оба — обычные номера/спец-строки.
       Учитывает (гк), !, %, kv, и особые правила для тянучки.
    """
    # KV: нельзя два подряд
    if a.get("kv") and b.get("kv"):
        return False

    # Если хотя бы один — тянучка
    if _is_tyanuchka(a) or _is_tyanuchka(b):
        # Тянучка снимает все ограничения для последующих номеров, КРОМЕ (гк).
        # То есть проверяем только, что у ведущего тянучки нет конфликта (гк) со смежным номером.
        tyan = a if _is_tyanuchka(a) else b
        other = b if _is_tyanuchka(a) else a
        leader = tyan["actors"][0]["name"] if tyan.get("actors") else ""
        if not leader:
            return False
        # Нельзя, если у лидера (гк) в соседнем номере
        if _has_tag(other, leader, "gk"):
            return False
        # KV уже проверили, у тянучки kv=False, так что всё ок
        return True

    # Обычный случай: проверяем пересечения актёров
    names_a = _actor_names(a)
    names_b = _actor_names(b)

    intersect = names_a & names_b
    if not intersect:
        return True

    # Для каждого пересекающегося актёра — проверяем разрешающие теги
    for name in intersect:
        # (гк) имеет высший приоритет: соседство запрещено
        if _has_tag(a, name, "gk") or _has_tag(b, name, "gk"):
            return False
        # Базово: нельзя подряд, КРОМЕ:
        # - если в A у актёра 'early' (!) => может быть и в B
        # - если в B у актёра 'later' (%) => может быть и в B
        allow = _has_tag(a, name, "early") or _has_tag(b, name, "later")
        if not allow:
            return False

    return True


# ===========================
# Поиск якорей и сегментов
# ===========================

def _find_anchor_indices(data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Находит индексы якорей по расположению:
       - 'pred' (предкулисье) — по type == 'предкулисье'
       - 'first' — первый обычный номер после предкулисья
       - 'second' — второй обычный после предкулисья
       - 'sponsors' — по type == 'спонсоры'
       - 'penultimate' — предпоследний обычный номер (по расположению)
       - 'last' — последний обычный номер (последняя строка программы)
    """
    n = len(data)
    pred = next((i for i, e in enumerate(data) if (e.get("type") or "").lower() == "предкулисье"), None)

    def is_ordinary(e: Dict[str, Any]) -> bool:
        t = (e.get("type") or "").lower()
        return t not in {"предкулисье", "спонсоры", "тянучка"}  # 'обычный' и прочие номера

    # первый и второй после предкулисья
    first = None
    second = None
    if pred is not None:
        after = [i for i in range(pred + 1, n) if is_ordinary(data[i])]
        if after:
            first = after[0]
        if len(after) >= 2:
            second = after[1]

    sponsors = next((i for i, e in enumerate(data) if (e.get("type") or "").lower() == "спонсоры"), None)

    # последний и предпоследний обычные номера (по расположению)
    ordinary_indices = [i for i, e in enumerate(data) if is_ordinary(e)]
    last = ordinary_indices[-1] if ordinary_indices else None
    penultimate = ordinary_indices[-2] if len(ordinary_indices) >= 2 else None

    anchors = {
        "pred": pred,
        "first": first,
        "second": second,
        "sponsors": sponsors,
        "penultimate": penultimate,
        "last": last,
    }

    logger.info(f"📌 Якоря: {anchors}")
    return anchors


def _segments_between_anchors(data: List[Dict[str, Any]], anchors: Dict[str, int]) -> List[Tuple[int, int, List[int]]]:
    """Возвращает список сегментов в виде (left_anchor_index, right_anchor_index, индексы_внутри).
       Сегменты формируем строго по расположению:
       - [pred .. first] — элементы между ними
       - [first .. second]
       - [second .. sponsors]
       - [sponsors .. penultimate]
       - [penultimate .. last]
       Только те, где реально есть внутренние элементы.
    """
    idxs = []
    order = ["pred", "first", "second", "sponsors", "penultimate", "last"]
    # Оставляем только существующие якоря и соблюдаем их порядок
    chain = [anchors[k] for k in order if anchors.get(k) is not None]
    chain = sorted(set(chain))  # на всякий: возрастающий по позиции

    segments: List[Tuple[int, int, List[int]]] = []
    for i in range(len(chain) - 1):
        L = chain[i]
        R = chain[i + 1]
        inside = [j for j in range(L + 1, R) if j != L and j != R]
        if inside:
            segments.append((L, R, inside))
    logger.info(f"🧩 Найдены сегменты: {[(l, r, len(inside)) for (l, r, inside) in segments]}")
    return segments


# ===========================
# Бэктрекинг сборки сегмента
# ===========================

def _build_segment_permutations(
    data: List[Dict[str, Any]],
    candidate_indices: List[int],
    left_anchor_idx: int,
    right_anchor_idx: int,
    allow_tyan: bool,
    best_tyan_so_far: Optional[int] = None,
) -> List[Tuple[List[Dict[str, Any]], int]]:
    """
    Строит все валидные перестановки для сегмента (между двумя якорями).
    Возвращает список (список_элементов_с_тянучками, число_вставленных_тянучек).
    Если allow_tyan=False — ищем только без тянучек.
    Если allow_tyan=True — допускаем вставки тянучек и минимизируем их число (программно).
    """
    L = data[left_anchor_idx]
    R = data[right_anchor_idx]
    items = [data[i] for i in candidate_indices]

    results: List[Tuple[List[Dict[str, Any]], int]] = []
    used = [False] * len(items)

    # быстрая проверка/прогноз: если два KV внутри и их много, сразу не отсекаем — правила может спасти порядок

    def dfs(path: List[Dict[str, Any]], tcount: int) -> None:
        nonlocal results, best_tyan_so_far

        # лимиты
        if len(results) >= MAX_PERMUTATIONS_PER_SEGMENT:
            return
        if best_tyan_so_far is not None and tcount > best_tyan_so_far:
            return

        # если все элементы расставлены — проверяем стык с правым якорем (R)
        if len(path) == len(items):
            last_node = path[-1] if path else L
            if _valid_adjacent(last_node, R):
                # ок
                seq = list(path)
                results.append((seq, tcount))
                if allow_tyan:
                    # обновим лучший
                    if best_tyan_so_far is None or tcount < best_tyan_so_far:
                        best_tyan_so_far = tcount
                return
            else:
                # попробуем вставить тянучку перед правым якорем (если разрешено)
                if allow_tyan:
                    leader = _choose_tyan_leader()
                    if _can_place_tyan_between(last_node, R, leader):
                        tyan = _make_tyanuchka(leader)
                        if _valid_adjacent(last_node, tyan) and _valid_adjacent(tyan, R):
                            seq = list(path) + [tyan]
                            results.append((seq, tcount + 1))
                            if best_tyan_so_far is None or (tcount + 1) < best_tyan_so_far:
                                best_tyan_so_far = tcount + 1
                return

        # выбираем следующий элемент
        prev = path[-1] if path else L
        for i in range(len(items)):
            if used[i]:
                continue
            candidate = items[i]

            # проверяем стык prev -> candidate
            if _valid_adjacent(prev, candidate):
                used[i] = True
                dfs(path + [candidate], tcount)
                used[i] = False
            elif allow_tyan:
                # попробуем вставить тянучку между prev и candidate
                leader = _choose_tyan_leader()
                if _can_place_tyan_between(prev, candidate, leader):
                    tyan = _make_tyanuchka(leader)
                    if _valid_adjacent(prev, tyan) and _valid_adjacent(tyan, candidate):
                        used[i] = True
                        dfs(path + [tyan, candidate], tcount + 1)
                        used[i] = False

    dfs([], 0)

    # Если разрешены тянучки — отфильтруем на минимальный tcount
    if allow_tyan and results:
        best = min(tc for _, tc in results)
        results = [r for r in results if r[1] == best]

    return results


# ===========================
# Склейка сегментов + итог
# ===========================

def generate_program_variants(data: List[Dict[str, Any]]) -> Tuple[List[List[Dict[str, Any]]], int]:
    """
    Главная функция:
    1) Находит якоря.
    2) Делит программу на сегменты между якорями.
    3) Для каждого сегмента ищет все валидные перестановки БЕЗ тянучек.
       Если в каком-то сегменте ноль вариантов без тянучек — для него ищем с тянучками (минимальным числом).
    4) Комбинирует сегменты между якорями в глобальные варианты.
    5) Возвращает (варианты_полной_программы, минимальное_число_тянучек_в_лучших_вариантах).
    """
    logger.info("🔧 Валидация и генерация вариантов по сегментам...")

    if not data or len(data) < 3:
        # слишком мало строк — возвращаем как есть
        return [data], 0

    anchors = _find_anchor_indices(data)
    segments = _segments_between_anchors(data, anchors)

    # Для каждого сегмента получим набор вариантов и их tcount
    segment_variants: List[List[Tuple[List[Dict[str, Any]], int]]] = []
    segment_bounds: List[Tuple[int, int]] = []

    for (L, R, inside) in segments:
        segment_bounds.append((L, R))

        # 1) пробуем без тянучек
        no_tyan = _build_segment_permutations(data, inside, L, R, allow_tyan=False)
        if no_tyan:
            logger.info(f"✅ Сегмент ({L}->{R}): найдено {len(no_tyan)} вариантов без тянучек.")
            segment_variants.append(no_tyan)
            continue

        # 2) с тянучками (минимальное число)
        with_tyan = _build_segment_permutations(data, inside, L, R, allow_tyan=True)
        if with_tyan:
            best_t = min(tc for _, tc in with_tyan)
            logger.info(f"🧩 Сегмент ({L}->{R}): без тянучек нельзя. Минимум тянучек: {best_t}, вариантов: {len([x for x in with_tyan if x[1]==best_t])}.")
            segment_variants.append(with_tyan)
            continue

        # 3) крайний случай: вообще не собрать — вернём исходный порядок сегмента (как есть)
        logger.warning(f"🚫 Сегмент ({L}->{R}) не удалось собрать даже с тянучками. Оставляем исходный порядок.")
        orig = [data[i] for i in inside]
        segment_variants.append([(orig, 0)])

    # Если сегментов нет — вся программа якорная или нечего переставлять
    if not segments:
        logger.info("ℹ️ Переставлять нечего — вся программа фиксирована якорями или не содержит внутренних блоков.")
        return [data], 0

    # Склеиваем: перебираем декартово произведение по сегментам
    combined: List[List[Dict[str, Any]]] = []
    combined_tyan_counts: List[int] = []

    # Собираем список «фиксированных» индексов (якоря)
    fixed_indices = set()
    for k, v in anchors.items():
        if v is not None:
            fixed_indices.add(v)

    # Вспомогательная функция для склейки
    def build_full_variant(segment_choice: List[Tuple[List[Dict[str, Any]], int]]) -> Tuple[List[Dict[str, Any]], int]:
        """Принимает список выбранных вариантов по каждому сегменту (в том же порядке),
           возвращает полную программу и суммарный tcount.
        """
        # ядро: идём по программе и когда встречаем границы сегмента — вставляем выбранную перестановку
        result: List[Dict[str, Any]] = []
        total_tyan = 0

        # конструируем карту границ -> содержимое сегмента
        bounds_to_seq: Dict[Tuple[int, int], Tuple[List[Dict[str, Any]], int]] = {}
        for b, choice in zip(segment_bounds, segment_choice):
            bounds_to_seq[b] = choice

        i = 0
        while i < len(data):
            if i in fixed_indices:
                result.append(data[i])
                # если это левая граница какого-то сегмента — после неё вставим сегмент
                for (L, R), (seg_seq, seg_t) in bounds_to_seq.items():
                    if L == i:
                        # добавляем содержимое сегмента
                        result.extend(seg_seq)
                        total_tyan += seg_t
                i += 1
                continue
            else:
                # это «внутренний» элемент какого-то сегмента — он будет уже вставлен с сегментом, пропускаем
                # найдём правую границу ближайшего сегмента, чтобы перескочить туда
                jumped = False
                for (L, R) in segment_bounds:
                    if L < i < R:
                        i = R  # прыгнем к правой границе; сама граница обработается как якорь
                        jumped = True
                        break
                if not jumped:
                    # внесегментный/неожиданный (на практике не должно случаться)
                    result.append(data[i])
                    i += 1

        # финальная проверка соседей по всему результату (на всякий пожарный)
        ok = True
        for j in range(len(result) - 1):
            if not _valid_adjacent(result[j], result[j + 1]):
                ok = False
                break
        if not ok:
            logger.debug("⚠️ Склейка дала невалидный глобальный вариант (не должен случаться).")
        return result, total_tyan

    # Перебираем все комбинации вариантов по сегментам
    # segment_variants: List[List[(seq, tcount)]]
    for choice in product(*segment_variants):
        if len(combined) >= MAX_GLOBAL_VARIANTS:
            break
        full, tc = build_full_variant(list(choice))
        combined.append(full)
        combined_tyan_counts.append(tc)

    if not combined:
        # Теоретически не должно — вернём исходное
        logger.warning("🚨 Не удалось собрать ни одного глобального варианта. Возвращаем исходный порядок.")
        return [data], 0

    # Фильтруем по минимальному числу тянучек (если были)
    min_tyan = min(combined_tyan_counts) if combined_tyan_counts else 0
    best_variants = [v for v, t in zip(combined, combined_tyan_counts) if t == min_tyan]

    logger.info(f"🏁 Итог: вариантов={len(best_variants)} (из {len(combined)}), минимально тянучек={min_tyan}")
    return best_variants, min_tyan
