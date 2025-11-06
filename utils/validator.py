# validator.py
# ============================================================
# 🎯 Валидатор и подбор программы с оптимизацией + STOP
# ============================================================

import copy
import random
import time
import threading
from typing import List, Tuple, Dict, Any, Optional
from loguru import logger

# Если в проекте telegram_utils лежит рядом с этим файлом (как у тебя),
# используем прямой импорт без пакета utils.*
from telegram_utils import send_message

# ============================================================
# 🛑 STOP-событие — читается из любой глубины рекурсии
# ============================================================

STOP_EVENT = threading.Event()

class StopComputation(Exception):
    """Сигнал для мгновенной остановки расчёта"""
    pass

def request_stop():
    """Запросить остановку текущего расчёта"""
    STOP_EVENT.set()
    logger.warning("🛑 Получен запрос на остановку расчёта пользователем.")

def reset_stop():
    """Сбросить флаг остановки перед новым запуском"""
    STOP_EVENT.clear()


# ============================================================
# 🔧 Утилиты по типам элементов
# ============================================================

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _is_tyan(item: Dict[str, Any]) -> bool:
    return _norm(item.get("type")) == "тянучка"

def _is_sponsor(item: Dict[str, Any]) -> bool:
    t = _norm(item.get("type"))
    ttl = _norm(item.get("title"))
    return t == "спонсоры" or "спонсор" in ttl

def _is_prekulisse(item: Dict[str, Any]) -> bool:
    t = _norm(item.get("type"))
    ttl = _norm(item.get("title"))
    return "предкулис" in (t or ttl)

def _is_full_number(item: Dict[str, Any]) -> bool:
    """Полноценный номер — учитывается как «номер» для gk/kv буфера"""
    return _norm(item.get("type")) == "обычный"

def _is_non_number(item: Dict[str, Any]) -> bool:
    """Тянучки и спонсоры не считаются «номерами»"""
    return _is_tyan(item) or _is_sponsor(item) or _is_prekulisse(item)

def _is_kv(item: Dict[str, Any]) -> bool:
    return bool(item and item.get("kv"))

def _actor_tags(item: Dict[str, Any], name: str) -> set:
    for a in (item.get("actors") or []):
        if a.get("name") == name:
            # нормализуем теги в нижний регистр
            return { _norm(t) for t in (a.get("tags") or []) }
    return set()

def _has_actor(item: Dict[str, Any], name: str) -> bool:
    return any(a.get("name") == name for a in (item.get("actors") or []))

def _has_tag(item: Dict[str, Any], name: str, tag: str) -> bool:
    tags = _actor_tags(item, name)
    # поддержим 'late' и 'later' как один смысл
    if tag == "late":
        return "late" in tags or "later" in tags
    return tag in tags

def _has_gk(item: Dict[str, Any], name: str) -> bool:
    return _has_tag(item, name, "gk")

def _has_late(item: Dict[str, Any], name: str) -> bool:
    return _has_tag(item, name, "late")

def _has_early(item: Dict[str, Any], name: str) -> bool:
    return _has_tag(item, name, "early")


# ============================================================
# ⚔️ Конфликты и ограничения
# ============================================================

def _shared_actors(left: Dict[str, Any], right: Dict[str, Any]) -> set:
    return {a["name"] for a in (left.get("actors") or [])} & {a["name"] for a in (right.get("actors") or [])}

def _weak_conflict(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    """
    «Слабый» конфликт по актёрам между соседними ПОЛНОЦЕННЫМИ номерами,
    с учётом исключений: early (у левого) и late (у правого).
    """
    if not (_is_full_number(left) and _is_full_number(right)):
        return False
    for name in _shared_actors(left, right):
        # 'gk' — не слабый, рассмотрим в «сильном»
        if _has_gk(left, name) or _has_gk(right, name):
            continue
        # снимающие исключения
        if _has_early(left, name) or _has_late(right, name):
            continue
        return True
    return False

def _adjacency_forbidden(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    """
    Соседство недопустимо (сильный конфликт), если:
      - два kv подряд;
      - общий актёр и у одного из них gk (требуется «номер-буфер»).
    """
    if not (_is_full_number(left) and _is_full_number(right)):
        return False
    if _is_kv(left) and _is_kv(right):
        return True
    for name in _shared_actors(left, right):
        if _has_gk(left, name) or _has_gk(right, name):
            return True
    return False

def _has_kv_violation(program: List[Dict[str, Any]]) -> bool:
    """
    Для kv:true — между повторами подряд должен быть хотя бы один полноценный номер.
    """
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
    """
    Для актёра с gk — между соседними появлениями должен быть хотя бы один полноценный номер.
    """
    last_seen: Dict[str, int] = {}
    for i, p in enumerate(program):
        if not _is_full_number(p):
            continue
        for a in (p.get("actors") or []):
            name = a.get("name")
            tags = { _norm(t) for t in (a.get("tags") or []) }
            if "gk" in tags:
                if name in last_seen:
                    prev_i = last_seen[name]
                    between = program[prev_i + 1:i]
                    if not any(_is_full_number(x) for x in between):
                        return True
                last_seen[name] = i
    return False

def _count_weak_conflicts(program: List[Dict[str, Any]]) -> int:
    c = 0
    for i in range(len(program) - 1):
        if _weak_conflict(program[i], program[i + 1]):
            c += 1
    return c

def _strong_constraints_ok(program: List[Dict[str, Any]]) -> bool:
    """Проверяем только «сильные» ограничения: kv/gk и прямое недопустимое соседство."""
    if _has_kv_violation(program):
        return False
    if _has_gk_violation(program):
        return False
    for i in range(len(program) - 1):
        if _adjacency_forbidden(program[i], program[i + 1]):
            return False
    return True


# ============================================================
# 🧱 Фиксированные позиции (иммьютаблы)
# ============================================================

def _fixed_zones(program: List[Dict[str, Any]]) -> Tuple[List[int], List[int]]:
    """
    Фиксируем:
      - зону от начала до ВТОРОГО полноценного номера включительно (предкулисье/1/2 и тянучки/спонсоры между ними);
      - зону от ПРЕДПОСЛЕДНЕГО полноценного номера до последнего включительно (и любые вставки между ними);
      - все спонсоры — всегда фикс.

    Возвращает (fixed_indexes, movable_indexes).
    """
    n = len(program)
    full_idxs = [i for i, p in enumerate(program) if _is_full_number(p)]

    fixed = set()

    # зона начала → второму номеру включительно
    if len(full_idxs) >= 2:
        second = full_idxs[1]
        for i in range(0, second + 1):
            fixed.add(i)
    else:
        # если вдруг меньше двух полноценных — фиксируем всё до конца
        fixed.update(range(n))

    # зона предпоследний → последний номер включительно
    if len(full_idxs) >= 2:
        prev_last, last = full_idxs[-2], full_idxs[-1]
        for i in range(prev_last, last + 1):
            fixed.add(i)

    # все спонсоры — фикс
    for i, p in enumerate(program):
        if _is_sponsor(p):
            fixed.add(i)

    fixed_list = sorted(fixed)
    movable = [i for i in range(n) if i not in fixed_list]
    logger.debug(f"📍 Фиксированные позиции: {fixed_list}")
    return fixed_list, movable


# ============================================================
# 🔁 Перебор базовых перестановок (без тянучек)
# ============================================================

SLEEP_INTERVAL = 200
SLEEP_TIME = 0.02

def _search_variants(program: List[Dict[str, Any]],
                     max_results: int = 100,
                     chat_id: Optional[int] = None,
                     stop_event: Optional[threading.Event] = None
                    ) -> Tuple[List[Tuple[int, List[Dict[str, Any]]]], int]:
    """
    Перебор перестановок (branch-and-bound):
      - Уважает «сильные» ограничения (kv/gk/запрет соседства);
      - Считает число «слабых» конфликтов (с учётом early/late);
      - Возвращает до max_results лучших по возрастанию слабых конфликтов
        + счётчик всех валидных по сильным ограничений вариантов.
    """
    stop_event = stop_event or STOP_EVENT
    n = len(program)
    fixed, movable = _fixed_zones(program)
    movables = [program[i] for i in movable]
    random.shuffle(movables)

    current: List[Optional[Dict[str, Any]]] = [None] * n
    for i in fixed:
        current[i] = copy.deepcopy(program[i])

    used = [False] * len(movables)

    best: List[Tuple[int, List[Dict[str, Any]]]] = []
    best_weak = float("inf")
    valid_count = 0
    iteration = 0

    def backtrack(pos: int):
        nonlocal iteration, best_weak, valid_count
        if stop_event.is_set():
            raise StopComputation

        # throttle
        if iteration and iteration % SLEEP_INTERVAL == 0:
            time.sleep(SLEEP_TIME)

        # пропускаем фиксированные
        while pos < n and current[pos] is not None:
            if stop_event.is_set():
                raise StopComputation
            pos += 1

        if pos >= n:
            # полная перестановка — проверим сильные
            if _strong_constraints_ok(current):
                valid_count += 1
                wk = _count_weak_conflicts(current)
                if wk <= best_weak:
                    best.append((wk, copy.deepcopy(current)))
                    best.sort(key=lambda x: x[0])
                    if len(best) > max_results:
                        best[:] = best[:max_results]
                    best_weak = best[0][0]
                    logger.debug(f"✅ Новый лучший базовый вариант (слабых={wk})")
            iteration += 1
            return

        left = current[pos - 1] if pos > 0 else None

        for i in range(len(movables)):
            if stop_event.is_set():
                raise StopComputation
            if used[i]:
                continue
            el = movables[i]

            # отсечка: нельзя сразу ставить то, что образует «сильный» конфликт слева
            if left and _adjacency_forbidden(left, el):
                continue

            # грубая нижняя оценка слабых (только слева) — для отсечки
            add = 1 if (left and _weak_conflict(left, el)) else 0
            tentative = add
            if tentative > best_weak:
                continue

            current[pos] = el
            used[i] = True
            backtrack(pos + 1)
            used[i] = False
            current[pos] = None

        iteration += 1

    if chat_id:
        try:
            send_message(chat_id, "🚀 Начинаю реальный перебор вариантов. Это может занять пару минут ⏳")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить уведомление: {e}")

    try:
        backtrack(0)
    except StopComputation:
        logger.warning("🚫 Перебор прерван по STOP.")
        return best, valid_count

    logger.info(f"🔎 Перебор завершён: валидных (по «сильным»)={valid_count}, лучший слабых={best[0][0] if best else '—'}")
    return best, valid_count


# ============================================================
# 🪶 Тянучки: допустимость и вставка
# ============================================================

def _can_actor_host_tyan(program: List[Dict[str, Any]], insert_left_idx: int, actor: str) -> bool:
    """
    Строгий чек допустимости ведущего тянучки перед позицией insert_left_idx+1:

    НЕЛЬЗЯ, если ЛЮБОЕ из:
      - в следующем полноценном номере тот же актёр и у него есть gk;
      - в предыдущем полноценном номере тот же актёр и у него есть gk;
      - в следующем полноценном номере тот же актёр присутствует и у него НЕТ тега 'late' (или 'later').

    Разрешается во всех остальных случаях (в т.ч. если актёр встречается через один номер — R+2, или больше не встречается).
    """
    n = len(program)
    prev_i = insert_left_idx
    next_i = insert_left_idx + 1

    if 0 <= prev_i < n and _is_full_number(program[prev_i]):
        prev = program[prev_i]
        if _has_gk(prev, actor):
            return False

    if 0 <= next_i < n and _is_full_number(program[next_i]):
        nxt = program[next_i]
        if _has_gk(nxt, actor):
            return False
        if _has_actor(nxt, actor) and not _has_late(nxt, actor):
            return False

    return True

def _insert_tyanuchki_exact(program: List[Dict[str, Any]], max_tyan: int) -> Tuple[List[Dict[str, Any]], int, bool]:
    """
    Пытаемся погасить все слабые конфликты, вставляя тянучки (не более max_tyan).
    Ведущие — строго по приоритету: Пушкин → Исаев → Рожков.
    Актёр НЕ обязан быть участником конфликтующих номеров — проверяется только допустимость.
    Если для конкретной пары не найден допустимый ведущий — эскалируем в «сильный» (возврат success=False).
    """
    prog = copy.deepcopy(program)
    tcount = 0
    priority = ["Пушкин", "Исаев", "Рожков"]

    i = 0
    while i < len(prog) - 1:
        if STOP_EVENT.is_set():
            raise StopComputation
        if tcount > max_tyan:
            break

        left, right = prog[i], prog[i + 1]
        if not (_is_full_number(left) and _is_full_number(right)):
            i += 1
            continue

        if _weak_conflict(left, right):
            if tcount == max_tyan:
                return prog, tcount, False

            placed = False
            for actor in priority:
                if STOP_EVENT.is_set():
                    raise StopComputation
                if not _can_actor_host_tyan(prog, i, actor):
                    continue

                # Добавляем тянучку
                t = {
                    "order": None,
                    "num": "",
                    "title": f"Тянучка ({actor})",
                    "actors_raw": actor,
                    "pp": "",
                    "hire": "",
                    "responsible": actor,
                    "kv": False,
                    "type": "тянучка",
                    "actors": [{"name": actor, "tags": []}],
                }
                prog.insert(i + 1, t)
                tcount += 1
                placed = True
                logger.info(f"➕ Добавлена тянучка ({actor}) между «{left.get('title','')}» и «{right.get('title','')}» (#{tcount})")
                break

            if not placed:
                logger.debug("⛔ Ни один из ведущих (Пушкин/Исаев/Рожков) не прошёл критерии — эскалация в сильный.")
                return prog, tcount, False

            # после успешной вставки перескочим через вставленную тянучку
            i += 2
            continue

        i += 1

    # финальная проверка — слабых конфликтов не осталось?
    if _count_weak_conflicts(prog) == 0:
        return prog, tcount, True
    return prog, tcount, False


# ============================================================
# 🧾 Форматирование топ-вариантов
# ============================================================

def _format_variant_line(program: List[Dict[str, Any]]) -> str:
    """Короткая строка: названия ПОЛНОЦЕННЫХ номеров через ' — ' (без тянучек/спонсоров/предкулисья)"""
    titles = [p.get("title", "") for p in program if _is_full_number(p)]
    return " — ".join(titles)


# ============================================================
# 🎯 Главная функция
# ============================================================

def generate_program_variants(program: List[Dict[str, Any]],
                              chat_id: Optional[int] = None,
                              top_n: int = 5) -> Tuple[List[List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Возвращает ([лучшие_варианты], статистика).

    Алгоритм:
      1) Перебор базовых перестановок, соблюдающих «сильные» правила (kv/gk).
         Сортировка по числу «слабых» конфликтов.
      2) Ранняя остановка по слоям допустимых тянучек: 0 → 1 → 2 → 3.
         На каждом слое пытаемся погасить все «слабые» тянучками (по приоритету ведущих).
      3) Если ни на одном слое не получилось, возвращаем лучший базовый вариант без вставок.

    Уважает STOP и возвращает лучшее из найденного на момент остановки.
    """
    reset_stop()
    logger.info("🧩 Подготовка к генерации вариантов программы...")

    if chat_id:
        try:
            send_message(chat_id, "📦 Подготовка данных... скоро начнётся реальный перебор ⏳")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить сообщение в Telegram: {e}")

    if not program or len(program) < 2:
        base_weak = _count_weak_conflicts(program)
        stats = {
            "checked_variants": 0,
            "valid_variants_count": 1 if _strong_constraints_ok(program) else 0,
            "initial_conflicts": base_weak,
            "final_conflicts": base_weak,
            "tyanuchki_added": 0,
            "best_tyanuchki": 0,
            "top_variants_lines": [_format_variant_line(program)],
        }
        return [program], stats

    # 1) базовые перестановки
    base_best, valid_count = _search_variants(program, chat_id=chat_id, stop_event=STOP_EVENT, max_results=100)

    if not base_best:
        # вообще нет валидных по «сильным»
        base_weak = _count_weak_conflicts(program)
        stats = {
            "checked_variants": 0,
            "valid_variants_count": 0,
            "initial_conflicts": base_weak,
            "final_conflicts": base_weak,
            "tyanuchki_added": 0,
            "best_tyanuchki": None,
            "top_variants_lines": [],
        }
        return [program], stats

    # 2) ранняя остановка по слоям 0/1/2/3
    best_solution: Optional[List[Dict[str, Any]]] = None
    best_layer: Optional[int] = None
    best_inserted = 0

    try:
        for layer_limit in [0, 1, 2, 3]:
            if STOP_EVENT.is_set():
                raise StopComputation
            for wk, cand in base_best:
                if STOP_EVENT.is_set():
                    raise StopComputation
                # если слабых больше, чем разрешённый лимит тянучек — смысла пробовать нет
                if wk > layer_limit:
                    continue
                prog2, ins, ok = _insert_tyanuchki_exact(cand, max_tyan=layer_limit)
                if ok:
                    best_solution = prog2
                    best_layer = layer_limit
                    best_inserted = ins
                    logger.success(f"🎯 Достигнута цель слоя {layer_limit}: слабые=0, тянучек добавлено={ins}")
                    raise StopComputation  # используем исключение для быстрого выхода из двух циклов
    except StopComputation:
        # если мы попали сюда через достижение цели — best_solution уже установлен
        if best_solution is None:
            logger.warning("🚫 Остановка до достижения решения — вернём лучшее из найденного ниже.")

    # Если не найдено на слоях (или остановились слишком рано) — попробуем лучший базовый с лимитом 3.
    if best_solution is None:
        wk, cand = base_best[0]
        try:
            prog2, ins, ok = _insert_tyanuchki_exact(cand, max_tyan=3)
        except StopComputation:
            # на STOP берём то, что уже было лучшим базовым
            prog2, ins, ok = cand, 0, False

        if ok:
            best_solution = prog2
            best_layer = 3
            best_inserted = ins
        else:
            # вернём лучший базовый без вставок
            best_solution = cand
            best_layer = None
            best_inserted = 0

    # Сформируем топ-строки (до 5)
    top_lines: List[str] = []
    for i, (wk, cand) in enumerate(sorted(base_best, key=lambda x: x[0])[:min(top_n, 5)], start=1):
        top_lines.append(f"{i}) слабых={wk} | " + _format_variant_line(cand))

    final_weak = _count_weak_conflicts(best_solution)

    stats = {
        "checked_variants": 0,              # считаем валидные, а не все посещения
        "valid_variants_count": valid_count,
        "initial_conflicts": None,          # не всегда осмысленно для перестановок
        "final_conflicts": final_weak,
        "tyanuchki_added": best_inserted,
        "best_tyanuchki": best_layer,       # 0/1/2/3 либо None, если не удалось
        "top_variants_lines": top_lines,
    }

    return [best_solution], stats
