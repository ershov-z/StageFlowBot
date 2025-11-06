# utils/validator.py
# ============================================================
# 🎯 Валидатор и подбор программы
# — расширенное логирование, многослойная вставка тянучек, возврат лучшего при STOP
# — совместим с multiprocessing.Event (из main)
# ============================================================

from __future__ import annotations
import copy
import random
import time
import threading
from typing import List, Tuple, Dict, Any, Optional
from loguru import logger
from utils.telegram_utils import send_message

# ============================================================
# 🛑 STOP (поддержка внешнего multiprocessing.Event)
# ============================================================

STOP_EVENT = threading.Event()

def set_external_stop_event(event):
    """Позволяет подменить стандартный STOP_EVENT внешним multiprocessing.Event."""
    global STOP_EVENT
    STOP_EVENT = event
    logger.debug("🔗 STOP_EVENT: подключён внешний контроллер (multiprocessing.Event)")

class StopComputation(Exception):
    """Сигнал для мгновенной остановки расчёта."""
    pass

def request_stop():
    """Локально поднять STOP (если используется потоковая модель)."""
    STOP_EVENT.set()
    logger.warning("🛑 Получен запрос на остановку расчёта пользователем.")

def reset_stop():
    """Сброс STOP для нового запуска (если нет внешнего контроллера)."""
    try:
        STOP_EVENT.clear()
    except Exception:
        # если STOP_EVENT — multiprocessing.Event, у него тоже есть clear()
        pass


# ============================================================
# 🧩 Нормализация и типы элементов
# ============================================================

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _is_tyan(item: Dict[str, Any]) -> bool:
    return _norm(item.get("type")) == "тянучка"

def _is_sponsor(item: Dict[str, Any]) -> bool:
    t = _norm(item.get("type"))
    title = _norm(item.get("title"))
    return t == "спонсоры" or "спонсор" in title

def _is_prekulisse(item: Dict[str, Any]) -> bool:
    t = _norm(item.get("type"))
    title = _norm(item.get("title"))
    return "предкулис" in (t or title)

def _is_full_number(item: Dict[str, Any]) -> bool:
    """Полноценный номер (участвует в перестановке)."""
    return _norm(item.get("type")) == "обычный"

def _is_non_number(item: Dict[str, Any]) -> bool:
    return _is_tyan(item) or _is_sponsor(item) or _is_prekulisse(item)

def _is_kv(item: Dict[str, Any]) -> bool:
    return bool(item and item.get("kv"))

# ============================================================
# 👥 Работа с актёрами и тегами
# ============================================================

def _actor_tags(item: Dict[str, Any], name: str) -> set:
    for a in (item.get("actors") or []):
        if a.get("name") == name:
            return {_norm(t) for t in (a.get("tags") or [])}
    return set()

def _has_actor(item: Dict[str, Any], name: str) -> bool:
    return any(a.get("name") == name for a in (item.get("actors") or []))

def _has_tag(item: Dict[str, Any], name: str, tag: str) -> bool:
    tags = _actor_tags(item, name)
    if tag == "late":
        return "late" in tags or "later" in tags
    return tag in tags

def _has_gk(item, name): return _has_tag(item, name, "gk")
def _has_late(item, name): return _has_tag(item, name, "late")
def _has_early(item, name): return _has_tag(item, name, "early")

# ============================================================
# ⚔️ Конфликты: сильные и слабые
# ============================================================

def _shared_actors(left: Dict[str, Any], right: Dict[str, Any]) -> set:
    return {a["name"] for a in (left.get("actors") or [])} & {a["name"] for a in (right.get("actors") or [])}

def _weak_conflict(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    """Слабый конфликт: общий актёр, без gk, без early/late смягчений."""
    if not (_is_full_number(left) and _is_full_number(right)):
        return False
    for n in _shared_actors(left, right):
        if _has_gk(left, n) or _has_gk(right, n):
            continue
        if _has_early(left, n) or _has_late(right, n):
            continue
        return True
    return False

def _adjacency_forbidden(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    """Строго запрещённое соседство: две КВ подряд, общий gk и т.п."""
    if not (_is_full_number(left) and _is_full_number(right)):
        return False
    if _is_kv(left) and _is_kv(right):
        return True
    for n in _shared_actors(left, right):
        if _has_gk(left, n) or _has_gk(right, n):
            return True
    return False

def _has_kv_violation(program: List[Dict[str, Any]]) -> bool:
    """Две КВ с разделителем только тянучками/спонсорами — запрещено."""
    last_kv = None
    for i, p in enumerate(program):
        if _is_full_number(p) and _is_kv(p):
            if last_kv is not None:
                between = program[last_kv + 1:i]
                if not any(_is_full_number(x) for x in between):
                    return True
            last_kv = i
    return False

def _has_gk_violation(program: List[Dict[str, Any]]) -> bool:
    """Один и тот же актёр с gk в двух «номерах» без буфера из обычного номера — запрещено."""
    last_seen = {}
    for i, p in enumerate(program):
        if not _is_full_number(p):
            continue
        for a in (p.get("actors") or []):
            name = a.get("name")
            if not name:
                continue
            tags = {_norm(t) for t in (a.get("tags") or [])}
            if "gk" in tags:
                if name in last_seen:
                    prev_i = last_seen[name]
                    between = program[prev_i + 1:i]
                    if not any(_is_full_number(x) for x in between):
                        return True
                last_seen[name] = i
    return False

def _count_weak_conflicts(program: List[Dict, Any]) -> int:
    return sum(_weak_conflict(program[i], program[i + 1]) for i in range(len(program) - 1))

def _strong_constraints_ok(program: List[Dict[str, Any]]) -> bool:
    """Проверка жёстких ограничений."""
    if _has_kv_violation(program) or _has_gk_violation(program):
        return False
    for i in range(len(program) - 1):
        if _adjacency_forbidden(program[i], program[i + 1]):
            return False
    return True


# ============================================================
# 🧱 Фиксация зон (логическая, не по индексам)
# — фиксируем: начало→2-й полноценный номер включительно;
#              предпоследний полноценный→последний включительно;
#              все «спонсоры» всегда фикс.
# ============================================================

def _fixed_zones(program: List[Dict[str, Any]]) -> Tuple[List[int], List[int]]:
    n = len(program)
    fixed = set()
    full_idxs = [i for i, p in enumerate(program) if _is_full_number(p)]

    if not full_idxs:
        fixed.update(range(n))
        return sorted(fixed), []

    # Зона 1: от начала до второго полноценного номера (включительно)
    if len(full_idxs) >= 2:
        second = full_idxs[1]
    else:
        second = full_idxs[-1]
    for i in range(0, second + 1):
        fixed.add(i)

    # Зона 2: от предпоследнего до последнего полноценного номера (включительно)
    if len(full_idxs) >= 2:
        prelast, last = full_idxs[-2], full_idxs[-1]
        for i in range(prelast, last + 1):
            fixed.add(i)
    else:
        fixed.add(full_idxs[0])

    # Всех спонсоров фиксируем всегда
    for i, p in enumerate(program):
        if _is_sponsor(p):
            fixed.add(i)

    fixed_list = sorted(fixed)
    movable = [i for i in range(n) if i not in fixed_list]
    logger.info(
        f"📍 Фикс: 0→{second}, "
        f"{(full_idxs[-2] if len(full_idxs)>=2 else full_idxs[0])}→{full_idxs[-1]}, "
        f"спонсоры зафиксированы. "
        f"Итог: fixed={len(fixed_list)}, movable={len(movable)}"
    )
    return fixed_list, movable


# ============================================================
# 🔁 Перебор/бэктрекинг (с возвратом лучшего при STOP)
# ============================================================

SLEEP_INTERVAL = 200
SLEEP_TIME = 0.02

def _search_variants(program: List[Dict[str, Any]],
                     max_results: int = 100,
                     chat_id: Optional[int] = None,
                     stop_event=None) -> Tuple[List[Tuple[int, List[Dict[str, Any]]]], int]:
    stop_event = stop_event or STOP_EVENT
    n = len(program)
    fixed, movable = _fixed_zones(program)
    movables = [program[i] for i in movable]
    random.shuffle(movables)

    current = [None] * n
    for i in fixed:
        current[i] = copy.deepcopy(program[i])

    used = [False] * len(movables)
    best: List[Tuple[int, List[Dict[str, Any]]]] = []
    best_weak = float("inf")
    valid_count = 0
    iteration = 0
    checked_total = 0

    def backtrack(pos: int):
        nonlocal iteration, best_weak, valid_count, checked_total
        if stop_event.is_set():
            raise StopComputation
        # throttling
        if iteration and iteration % SLEEP_INTERVAL == 0:
            time.sleep(SLEEP_TIME)

        # пропускаем фиксированные позиции
        while pos < n and current[pos] is not None:
            if stop_event.is_set():
                raise StopComputation
            pos += 1

        # достигли конца — валидируем
        if pos >= n:
            checked_total += 1
            if checked_total % 25 == 0:
                wk = _count_weak_conflicts(current)
                logger.debug(f"🧮 Проверен вариант №{checked_total} (слабых={wk})")
            if _strong_constraints_ok(current):
                valid_count += 1
                wk = _count_weak_conflicts(current)
                if wk <= best_weak:
                    best.append((wk, copy.deepcopy(current)))
                    best.sort(key=lambda x: x[0])
                    if len(best) > max_results:
                        best[:] = best[:max_results]
                    best_weak = best[0][0]
                    logger.debug(f"✅ Новый лучший вариант (слабых={wk}, всего лучших={len(best)})")
            iteration += 1
            return

        left = current[pos - 1] if pos > 0 else None

        for i in range(len(movables)):
            if stop_event.is_set():
                raise StopComputation
            if used[i]:
                continue
            el = movables[i]

            # быстрый отсев
            if left and _adjacency_forbidden(left, el):
                continue

            add = 1 if (left and _weak_conflict(left, el)) else 0
            # если уже хуже текущего лучшего — дальше нет смысла
            if add > best_weak:
                continue

            current[pos] = el
            used[i] = True
            backtrack(pos + 1)
            used[i] = False
            current[pos] = None

        iteration += 1

    # приветствие
    if chat_id:
        try:
            send_message(chat_id, "🚀 Начинаю реальный перебор вариантов. Это может занять пару минут ⏳")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось уведомить пользователя перед стартом перебора: {e}")

    # запуск
    try:
        backtrack(0)
    except StopComputation:
        logger.warning("🚫 Перебор прерван по STOP (команда /stop). Возвращаю лучшее найденное.")
        # важно: даже при стопе — вернуть то, что уже было найдено
        return best, valid_count

    logger.info(
        f"🔎 Завершён перебор: проверено={checked_total}, валидных={valid_count}, "
        f"лучший_слабых={(best[0][0] if best else '—')}, всего_лучших={len(best)}"
    )
    return best, valid_count


# ============================================================
# 🪶 Тянучки — вставка по правилам приоритета
# Приоритет: Пушкин → Исаев → Рожков
# Условия (как ты просил):
#  1) если у актёра есть gk в левом или правом номере — запрещено;
#  2) если он есть в правом номере БЕЗ тегов — запрещено;
#  3) если он есть в правом номере с тегом late — можно;
#  4) если его нет в правом номере — можно.
# ============================================================

def _can_pick_host_for_gap(left: Dict[str, Any], right: Dict[str, Any], actor: str) -> bool:
    # 1) GK в левом или правом — запрещено
    if _has_gk(left, actor) or _has_gk(right, actor):
        return False
    # 2) есть в правом без тегов — нельзя
    if _has_actor(right, actor) and not _has_late(right, actor):
        return False
    # 3) есть с late — можно; 4) нет в правом — можно
    return True

def _insert_tyanuchki_exact(program: List[Dict[str, Any]], max_tyan: int) -> Tuple[List[Dict[str, Any]], int, bool]:
    prog = copy.deepcopy(program)
    count_added = 0
    leaders = ["Пушкин", "Исаев", "Рожков"]
    i = 0
    while i < len(prog) - 1:
        if STOP_EVENT.is_set():
            raise StopComputation
        if count_added >= max_tyan:
            break

        left, right = prog[i], prog[i + 1]
        if not (_is_full_number(left) and _is_full_number(right)):
            i += 1
            continue

        if _weak_conflict(left, right):
            chosen = None
            for a in leaders:
                if _can_pick_host_for_gap(left, right, a):
                    chosen = a
                    reason = "нет gk и допустим по next/late" if not _has_actor(right, a) else "в next с late"
                    logger.info(f"🎯 Выбран ведущий для тянучки: {a} ({reason}) между «{left.get('title','')}» и «{right.get('title','')}»")
                    break
                else:
                    logger.debug(f"⛔ {a}: не подходит для тянучки между "
                                 f"«{left.get('title','')}» и «{right.get('title','')}» (gk или присутствует в next без late)")

            if not chosen:
                logger.warning(f"⚠️ Не удалось подобрать ведущего для тянучки между "
                               f"«{left.get('title','')}» и «{right.get('title','')}» — конфликт временно остаётся")
                i += 1
                continue

            t = {
                "order": None, "num": "", "title": f"Тянучка ({chosen})",
                "actors_raw": chosen, "pp": "", "hire": "",
                "responsible": chosen, "kv": False, "type": "тянучка",
                "actors": [{"name": chosen, "tags": []}],
            }
            prog.insert(i + 1, t)
            count_added += 1
            logger.info(f"➕ Добавлена тянучка ({chosen}) между «{left.get('title','')}» и «{right.get('title','')}» (всего={count_added})")
            # через вставку шагаем на +2: left, tyan, right
            i += 2
            continue

        i += 1

    ok = _count_weak_conflicts(prog) == 0
    return prog, count_added, ok


# ============================================================
# 🧾 Форматирование / метрики (для расширенных логов)
# ============================================================

def _summary_titles(program: List[Dict[str, Any]]) -> str:
    """Короткая строка с названиями — удобно в логах при отладке."""
    titles = []
    for p in program:
        if _is_full_number(p):
            titles.append(p.get("title") or "№")
        elif _is_tyan(p):
            titles.append("ТЯН")
        elif _is_sponsor(p):
            titles.append("СПОН")
        elif _is_prekulisse(p):
            titles.append("ПРЕД")
        else:
            titles.append("·")
    return " | ".join(titles)


# ============================================================
# 🎯 Главная функция: генерация
# — поддержка внешнего stop_event
# — layered-проход по максимальному количеству тянучек (0→3)
# — возврат лучшего при STOP
# ============================================================

def generate_program_variants(program: List[Dict[str, Any]],
                              chat_id: Optional[int] = None,
                              top_n: int = 5,
                              stop_event=None):
    """
    Возвращает: ([лучшие_решения], статистика).
    При STOP возвращает лучший найденный на момент остановки (включая попытку вставки тянучек).
    """
    # подключаем внешний STOP, если передан (multiprocessing.Event)
    if stop_event is not None:
        set_external_stop_event(stop_event)
    else:
        reset_stop()

    logger.info("🧩 Подготовка к генерации вариантов программы…")

    if chat_id:
        try:
            send_message(chat_id, "📦 Подготовка данных… скоро начнётся реальный перебор ⏳")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить сообщение о подготовке: {e}")

    if not program or len(program) < 2:
        base = _count_weak_conflicts(program or [])
        stats = {
            "checked_variants": 0,
            "valid_variants": 1 if _strong_constraints_ok(program or []) else 0,
            "initial_conflicts": base,
            "final_conflicts": base,
            "tyanuchki_added": 0,
        }
        return [program], stats

    # 1) Перебираем перестановки (возвращает несколько лучших по слабым конфликтам)
    best, valid_count = _search_variants(program, chat_id=chat_id, stop_event=STOP_EVENT)

    if not best:
        base = _count_weak_conflicts(program)
        stats = {
            "checked_variants": 0,
            "valid_variants": 0,
            "initial_conflicts": base,
            "final_conflicts": base,
            "tyanuchki_added": 0,
        }
        logger.warning("⚠️ Валидных перестановок не найдено — возвращаю исходный порядок.")
        return [program], stats

    # 2) Многослойная попытка «доведения до 0» тянучками
    best_solution = None
    best_layer = None
    best_added = 0
    initial_best_conf = best[0][0]  # слабые конфликты у лучшего бэйс-варианта
    best_base_candidate = copy.deepcopy(best[0][1])

    try:
        for layer in (0, 1, 2, 3):
            if STOP_EVENT.is_set():
                raise StopComputation
            # пробуем применить слой ко всем отобранным base-кандидатам,
            # но ускоряемся — не трогаем те, у кого слабых конфликтов > layer
            for wk, cand in best:
                if STOP_EVENT.is_set():
                    raise StopComputation
                if wk > layer:
                    continue
                prog2, added, ok = _insert_tyanuchki_exact(cand, max_tyan=layer)
                if ok:
                    best_solution, best_layer, best_added = prog2, layer, added
                    logger.success(f"🎯 Уровень {layer}: слабых=0, добавлено тянучек={added}")
                    raise StopComputation
    except StopComputation:
        pass

    if best_solution is None:
        # не смогли обнулить слабые конфликты тянучками в рамках лимитов
        # отдаём лучший base-кандидат, попробовав максимум тянучек для него
        try:
            prog2, added, ok = _insert_tyanuchki_exact(best_base_candidate, max_tyan=3)
            if ok:
                best_solution, best_layer, best_added = prog2, 3, added
            else:
                best_solution, best_layer, best_added = best_base_candidate, None, 0
        except StopComputation:
            # если стоп прямо во время вставки — отдаём то, что уже есть
            best_solution, best_layer, best_added = best_base_candidate, None, 0

    final_conf = _count_weak_conflicts(best_solution)
    logger.info("🧾 Итоговый порядок:\n" + _summary_titles(best_solution))
    logger.success(f"✅ Итог: слабых {initial_best_conf} → {final_conf}, тянучек добавлено={best_added}, слой={best_layer}")

    stats = {
        "checked_variants": valid_count,
        "initial_conflicts": initial_best_conf,
        "final_conflicts": final_conf,
        "tyanuchki_added": best_added,
        "best_layer": best_layer,
    }
    return [best_solution], stats
