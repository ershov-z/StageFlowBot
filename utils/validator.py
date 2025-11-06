# utils/validator.py
# ============================================================
# 🎯 Валидатор и подбор программы с расширенным логированием и корректной вставкой тянучек
# ============================================================

import copy
import random
import time
import threading
from typing import List, Tuple, Dict, Any, Optional
from loguru import logger
from telegram_utils import send_message

# ============================================================
# 🛑 STOP
# ============================================================

STOP_EVENT = threading.Event()

class StopComputation(Exception):
    """Сигнал для мгновенной остановки расчёта"""
    pass

def request_stop():
    STOP_EVENT.set()
    logger.warning("🛑 Получен запрос на остановку расчёта пользователем.")

def reset_stop():
    STOP_EVENT.clear()

# ============================================================
# 🧩 Типы и вспомогательные функции
# ============================================================

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _is_tyan(item): return _norm(item.get("type")) == "тянучка"
def _is_sponsor(item): return "спонсор" in (_norm(item.get("title")) or _norm(item.get("type")))
def _is_prekulisse(item): return "предкулис" in (_norm(item.get("title")) or _norm(item.get("type")))
def _is_full_number(item): return _norm(item.get("type")) == "обычный"
def _is_non_number(item): return _is_tyan(item) or _is_sponsor(item) or _is_prekulisse(item)
def _is_kv(item): return bool(item and item.get("kv"))

def _actor_tags(item, name):
    for a in (item.get("actors") or []):
        if a.get("name") == name:
            return {_norm(t) for t in (a.get("tags") or [])}
    return set()

def _has_tag(item, name, tag):
    tags = _actor_tags(item, name)
    if tag == "late":
        return "late" in tags or "later" in tags
    return tag in tags

def _has_actor(item, name): return any(a.get("name") == name for a in (item.get("actors") or []))
def _has_gk(item, name): return _has_tag(item, name, "gk")
def _has_late(item, name): return _has_tag(item, name, "late")
def _has_early(item, name): return _has_tag(item, name, "early")

# ============================================================
# ⚔️ Конфликты и ограничения
# ============================================================

def _shared_actors(left, right):
    return {a["name"] for a in (left.get("actors") or [])} & {a["name"] for a in (right.get("actors") or [])}

def _weak_conflict(left, right):
    if not (_is_full_number(left) and _is_full_number(right)):
        return False
    for n in _shared_actors(left, right):
        if _has_gk(left, n) or _has_gk(right, n):
            continue
        if _has_early(left, n) or _has_late(right, n):
            continue
        return True
    return False

def _adjacency_forbidden(left, right):
    if not (_is_full_number(left) and _is_full_number(right)):
        return False
    if _is_kv(left) and _is_kv(right):
        return True
    for n in _shared_actors(left, right):
        if _has_gk(left, n) or _has_gk(right, n):
            return True
    return False

def _count_weak_conflicts(prog):
    return sum(_weak_conflict(prog[i], prog[i+1]) for i in range(len(prog)-1))

def _strong_constraints_ok(program):
    seen_gk = {}
    last_kv = None
    for i, p in enumerate(program):
        if _is_full_number(p) and _is_kv(p):
            if last_kv is not None:
                between = program[last_kv + 1:i]
                if not any(_is_full_number(x) for x in between):
                    return False
            last_kv = i
        if not _is_full_number(p):
            continue
        for a in (p.get("actors") or []):
            name = a.get("name")
            tags = {_norm(t) for t in (a.get("tags") or [])}
            if "gk" in tags:
                if name in seen_gk:
                    prev_i = seen_gk[name]
                    between = program[prev_i + 1:i]
                    if not any(_is_full_number(x) for x in between):
                        return False
                seen_gk[name] = i
    for i in range(len(program) - 1):
        if _adjacency_forbidden(program[i], program[i+1]):
            return False
    return True

# ============================================================
# 🧱 Фиксированные зоны
# ============================================================

def _fixed_zones(program: List[Dict[str, Any]]) -> Tuple[List[int], List[int]]:
    """
    Фиксируем зоны:
      • от начала до второго полноценного номера (предкулисье/1/2 и всё между ними);
      • от предпоследнего до последнего (и всё между ними);
      • все спонсоры — всегда фикс.
    """
    n = len(program)
    fixed = set()
    full = [i for i, p in enumerate(program) if _is_full_number(p)]

    if not full:
        fixed.update(range(n))
        return sorted(fixed), []

    if len(full) >= 2:
        second = full[1]
    else:
        second = full[-1]
    for i in range(0, second + 1):
        fixed.add(i)

    if len(full) >= 2:
        prelast, last = full[-2], full[-1]
        for i in range(prelast, last + 1):
            fixed.add(i)
    else:
        fixed.add(full[0])

    for i, p in enumerate(program):
        if _is_sponsor(p):
            fixed.add(i)

    logger.info(f"📍 Фикс: от начала→{second}, от {full[-2] if len(full)>=2 else full[0]}→{full[-1]}, спонсоры включены.")
    fixed_list = sorted(fixed)
    movable = [i for i in range(n) if i not in fixed_list]
    return fixed_list, movable

# ============================================================
# 🔁 Перебор базовых перестановок
# ============================================================

SLEEP_INTERVAL = 200
SLEEP_TIME = 0.02

def _search_variants(program, chat_id=None, stop_event=None, max_results=100):
    stop_event = stop_event or STOP_EVENT
    n = len(program)
    fixed, movable = _fixed_zones(program)
    movables = [program[i] for i in movable]
    random.shuffle(movables)

    current = [None]*n
    for i in fixed:
        current[i] = copy.deepcopy(program[i])
    used = [False]*len(movables)
    best, best_weak, valid = [], float("inf"), 0
    iter_count = 0

    def backtrack(pos):
        nonlocal iter_count, best_weak, valid
        if stop_event.is_set():
            raise StopComputation
        if iter_count and iter_count % SLEEP_INTERVAL == 0:
            time.sleep(SLEEP_TIME)
        while pos < n and current[pos] is not None:
            pos += 1
        if pos >= n:
            if _strong_constraints_ok(current):
                valid += 1
                wk = _count_weak_conflicts(current)
                if wk <= best_weak:
                    best.append((wk, copy.deepcopy(current)))
                    best.sort(key=lambda x: x[0])
                    best[:] = best[:max_results]
                    best_weak = best[0][0]
                    logger.debug(f"✅ Новый лучший вариант (слабых={wk})")
            iter_count += 1
            return
        left = current[pos-1] if pos > 0 else None
        for i in range(len(movables)):
            if stop_event.is_set():
                raise StopComputation
            if used[i]:
                continue
            el = movables[i]
            if left and _adjacency_forbidden(left, el):
                continue
            add = 1 if (left and _weak_conflict(left, el)) else 0
            if add > best_weak:
                continue
            current[pos] = el
            used[i] = True
            backtrack(pos+1)
            used[i] = False
            current[pos] = None
        iter_count += 1

    if chat_id:
        try:
            send_message(chat_id, "🚀 Запуск реального перебора вариантов…")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось уведомить: {e}")

    try:
        backtrack(0)
    except StopComputation:
        logger.warning("🚫 Перебор остановлен пользователем.")
        return best, valid

    logger.info(f"🔎 Перебор завершён: валидных={valid}, лучший слабых={best[0][0] if best else '—'}")
    return best, valid

# ============================================================
# 🪶 Вставка тянучек
# ============================================================

def _insert_tyanuchki_exact(program, max_tyan):
    """
    Вставка тянучек между конфликтными номерами с правильной логикой выбора ведущего.
    Приоритет актёров: Пушкин → Исаев → Рожков.
    Условия вставки:
      1. У актёра есть GK в левом или правом номере — запрещено.
      2. Актёр есть в следующем номере без тегов — запрещено.
      3. Актёр есть в следующем номере с тегом 'late' — разрешено.
      4. Актёра нет в следующем номере — разрешено.
    """
    prog = copy.deepcopy(program)
    count = 0
    leaders = ["Пушкин", "Исаев", "Рожков"]
    i = 0
    while i < len(prog) - 1:
        if STOP_EVENT.is_set():
            raise StopComputation
        left, right = prog[i], prog[i+1]
        if not (_is_full_number(left) and _is_full_number(right)):
            i += 1
            continue

        if _weak_conflict(left, right) and count < max_tyan:
            placed = False
            for actor in leaders:
                # 1. Проверка GK
                if _has_gk(left, actor) or _has_gk(right, actor):
                    logger.debug(f"⛔ {actor}: имеет GK — пропуск")
                    continue
                # 2. Есть в правом номере без late — нельзя
                if _has_actor(right, actor) and not _has_late(right, actor):
                    logger.debug(f"⛔ {actor}: есть в правом номере без late — пропуск")
                    continue
                # 3. Можно, если нет в правом номере или есть с late
                if not _has_actor(right, actor) or _has_late(right, actor):
                    logger.info(f"🎯 Выбран {actor} для тянучки между «{left.get('title')}» и «{right.get('title')}»")
                    t = {
                        "title": f"Тянучка ({actor})", "type": "тянучка",
                        "actors_raw": actor, "actors": [{"name": actor, "tags": []}],
                        "pp": "", "hire": "", "responsible": actor, "kv": False,
                    }
                    prog.insert(i+1, t)
                    count += 1
                    placed = True
                    break
            if not placed:
                logger.warning(f"⚠️ Никто не подошёл для тянучки между «{left.get('title')}» и «{right.get('title')}»")
            i += 2
            continue
        i += 1
    ok = _count_weak_conflicts(prog) == 0
    return prog, count, ok

# ============================================================
# 🎯 Главная функция
# ============================================================

def generate_program_variants(program, chat_id=None, top_n=5):
    reset_stop()
    logger.info("🧩 Подготовка к генерации вариантов…")

    if chat_id:
        try:
            send_message(chat_id, "📦 Начинаю подготовку данных ⏳")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить сообщение: {e}")

    base_best, valid = _search_variants(program, chat_id=chat_id, stop_event=STOP_EVENT)
    if not base_best:
        base = _count_weak_conflicts(program)
        return [program], {"checked_variants": 0, "initial_conflicts": base, "final_conflicts": base, "tyanuchki_added": 0}

    best_conf, best_prog = base_best[0]
    best_solution = None
    best_layer, best_added = None, 0

    try:
        for layer in [0, 1, 2, 3]:
            if STOP_EVENT.is_set():
                raise StopComputation
            for wk, cand in base_best:
                if wk > layer:
                    continue
                prog, added, ok = _insert_tyanuchki_exact(cand, layer)
                if ok:
                    best_solution, best_layer, best_added = prog, layer, added
                    logger.success(f"🎯 Уровень {layer}: слабых=0, добавлено тянучек={added}")
                    raise StopComputation
    except StopComputation:
        pass

    if not best_solution:
        best_solution, best_layer, best_added = best_prog, None, 0

    final_conf = _count_weak_conflicts(best_solution)
    logger.success(f"✅ Итог: конфликтов {best_conf} → {final_conf}, добавлено {best_added}, слой={best_layer}")

    return [best_solution], {
        "checked_variants": valid,
        "initial_conflicts": best_conf,
        "final_conflicts": final_conf,
        "tyanuchki_added": best_added,
        "best_layer": best_layer,
    }
