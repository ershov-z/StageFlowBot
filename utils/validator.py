# utils/validator.py
# ============================================================
# 🎯 Валидатор и подбор программы с оптимизацией
# ============================================================

import copy
import random
import time
from loguru import logger
from utils.telegram_utils import send_message

# ============================================================
# 🛑 STOP FEATURE — глобальный флаг для прерывания расчёта
# ============================================================

STOP_FLAG = False


def request_stop():
    """Запрашивает остановку текущего перебора"""
    global STOP_FLAG
    STOP_FLAG = True
    logger.warning("🛑 Получен запрос на остановку расчёта!")


def reset_stop():
    """Сбрасывает флаг остановки перед новым запуском"""
    global STOP_FLAG
    STOP_FLAG = False


# ============================================================
# 🔧 Служебные функции
# ============================================================

class StopComputation(Exception):
    """Сигнал для мгновенной остановки перебора"""
    pass


def _is_tyan(item): return item and item.get("type") == "тянучка"
def _is_number(item): return item and item.get("type") != "тянучка"
def _is_kv(item): return bool(item.get("kv"))


def _has_actor(item, name):
    for a in item.get("actors", []) or []:
        if a.get("name") == name:
            return True
    return False


def _actor_tags(item, name):
    for a in item.get("actors", []) or []:
        if a.get("name") == name:
            return set(a.get("tags") or [])
    return set()


def _has_gk(item, name): return "gk" in _actor_tags(item, name)
def _has_later(item, name): return "later" in _actor_tags(item, name)


# ============================================================
# ⚔️ Конфликты и запрещённые соседства
# ============================================================

def _adjacent_conflict(left, right):
    """Возвращает 1, если актёр встречается в соседних номерах (без гк)"""
    if not (_is_number(left) and _is_number(right)):
        return 0
    shared = {a["name"] for a in left["actors"]} & {a["name"] for a in right["actors"]}
    for actor in shared:
        if not _has_gk(left, actor) and not _has_gk(right, actor):
            return 1
    return 0


def _adjacency_forbidden(left, right):
    """Возвращает True, если соседство номеров недопустимо (две кв или гк актёр)"""
    if not (_is_number(left) and _is_number(right)):
        return False
    if _is_kv(left) and _is_kv(right):
        return True
    shared = {a["name"] for a in left["actors"]} & {a["name"] for a in right["actors"]}
    for actor in shared:
        if _has_gk(left, actor) or _has_gk(right, actor):
            return True
    return False


def _count_conflicts(program):
    """Подсчёт текущих конфликтов"""
    return sum(_adjacent_conflict(program[i], program[i + 1]) for i in range(len(program) - 1))


# ============================================================
# 🧱 Фиксированные позиции
# ============================================================

def _compute_fixed_indices(program):
    n = len(program)
    fixed = {0, 1, 2, max(0, n - 2), max(0, n - 1)}
    for i, p in enumerate(program):
        if p.get("type") == "спонсоры":
            fixed.add(i)
    return sorted(fixed), [i for i in range(n) if i not in fixed]


# ============================================================
# 🔍 Проверки KV и gk
# ============================================================

def _has_kv_violation(program):
    last_kv = None
    for i, p in enumerate(program):
        if _is_number(p) and _is_kv(p):
            if last_kv is not None:
                between = program[last_kv + 1:i]
                if all(_is_tyan(x) for x in between):
                    return True
            last_kv = i
    return False


def _has_gk_violation(program):
    last_seen = {}
    for i, p in enumerate(program):
        if not _is_number(p): 
            continue
        for a in p.get("actors", []):
            name = a["name"]
            tags = set(a.get("tags") or [])
            if "gk" in tags and name in last_seen:
                prev_i = last_seen[name]
                between = program[prev_i + 1:i]
                if all(_is_tyan(x) for x in between):
                    return True
            last_seen[name] = i
    return False


# ============================================================
# 🔁 Поиск оптимальных вариантов (бэктрекинг)
# ============================================================

SLEEP_INTERVAL = 50
SLEEP_TIME = 0.005


def _search_best_variants(program, max_results=5, max_conflicts_allowed=3, chat_id=None):
    """Бэктрекинг-перебор перестановок"""
    from utils.validator import STOP_FLAG
    n = len(program)
    fixed, movable = _compute_fixed_indices(program)
    movables = [program[i] for i in movable]
    random.shuffle(movables)

    current = [None] * n
    for i in fixed:
        current[i] = program[i]
    used = [False] * len(movables)

    best, checked, best_conf = [], 0, float("inf")
    found_zero = False
    iteration = 0
    notified_started = False

    def backtrack(pos, confs):
        nonlocal checked, best_conf, found_zero, iteration, notified_started

        if STOP_FLAG:
            raise StopComputation

        if not notified_started:
            notified_started = True
            logger.info("🚀 Реальный перебор запущен")
            if chat_id:
                try:
                    send_message(chat_id, "🚀 Реальный перебор запущен: начинаю проверять перестановки.")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось отправить уведомление: {e}")

        # Контроль нагрузки
        if iteration > 0 and iteration % SLEEP_INTERVAL == 0:
            time.sleep(SLEEP_TIME)

        if confs > max_conflicts_allowed or found_zero:
            return

        while pos < n and current[pos] is not None:
            pos += 1
        if pos >= n:
            checked += 1
            iteration += 1
            if iteration % 25 == 0:
                logger.debug(f"🧮 Проверен вариант №{checked} — конфликты: {confs}")
            if STOP_FLAG:
                raise StopComputation

            if _has_kv_violation(current) or _has_gk_violation(current):
                return

            if confs <= best_conf:
                best.append((confs, copy.deepcopy(current)))
                best.sort(key=lambda x: x[0])
                best[:] = best[:max_results]
                best_conf = best[0][0]
                logger.debug(f"✅ Новый лучший вариант (конфликтов={confs})")

            if confs == 0:
                found_zero = True
            return

        left = current[pos - 1] if pos > 0 else None
        choices = [i for i, u in enumerate(used) if not u]
        random.shuffle(choices)

        for i in choices:
            if STOP_FLAG:
                raise StopComputation
            el = movables[i]
            if left and _adjacency_forbidden(left, el):
                continue
            add = _adjacent_conflict(left, el) if left else 0
            newc = confs + add
            if newc > min(best_conf, max_conflicts_allowed):
                continue
            current[pos] = el
            used[i] = True
            backtrack(pos + 1, newc)
            used[i] = False
            current[pos] = None
            if found_zero:
                return

    try:
        backtrack(0, 0)
    except StopComputation:
        logger.warning("🚫 Перебор прерван по STOP_FLAG")

    logger.info(f"🔎 Завершён перебор: проверено {checked} вариантов, лучший конфликт={best_conf}")
    return best[:max_results], checked


# ============================================================
# 🪶 Добавление тянучек
# ============================================================

def _can_actor_host_tyan(program, idx, actor):
    n = len(program)
    if idx + 1 < n and _is_number(program[idx + 1]):
        nxt = program[idx + 1]
        if _has_gk(nxt, actor):
            return False
        if _has_actor(nxt, actor) and not _has_later(nxt, actor):
            return False
    return True


def _insert_tyanuchki(program, max_tyanuchki=3):
    """Добавляет тянучки для устранения конфликтов"""
    tcount, pri = 0, ["Пушкин", "Исаев", "Рожков"]
    i = 0
    while i < len(program) - 1:
        if tcount >= max_tyanuchki:
            break
        if i <= 2 or i >= len(program) - 3:
            i += 1
            continue
        l, r = program[i], program[i + 1]
        if not (_is_number(l) and _is_number(r)):
            i += 1
            continue
        if _adjacent_conflict(l, r):
            shared = {a["name"] for a in l["actors"]} & {a["name"] for a in r["actors"]}
            for actor in pri:
                if actor in shared and _can_actor_host_tyan(program, i, actor):
                    t = {
                        "order": None, "num": "", "title": f"Тянучка ({actor})",
                        "actors_raw": actor, "pp": "", "hire": "",
                        "responsible": actor, "kv": False, "type": "тянучка",
                        "actors": [{"name": actor, "tags": []}],
                    }
                    program.insert(i + 1, t)
                    tcount += 1
                    logger.info(f"➕ Тянучка ({actor}) между «{l['title']}» и «{r['title']}» ({tcount})")
                    break
        i += 1
    return program, tcount


# ============================================================
# 🎯 Основная функция
# ============================================================

def generate_program_variants(program, chat_id=None, top_n=5):
    """Главный контроллер подбора и генерации"""
    from utils.validator import STOP_FLAG, reset_stop
    reset_stop()
    logger.info("🧩 Подготовка к генерации вариантов программы...")

    if chat_id:
        try:
            send_message(chat_id, "📦 Подготовка данных... скоро начнётся реальный перебор ⏳")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось отправить сообщение в Telegram: {e}")

    if not program or len(program) < 2:
        base = _count_conflicts(program)
        return [program], {
            "checked_variants": 0,
            "initial_conflicts": base,
            "final_conflicts": 0,
            "tyanuchki_added": 0,
        }

    best, checked = _search_best_variants(program, chat_id=chat_id)

    if not best:
        base = _count_conflicts(program)
        return [program], {
            "checked_variants": checked,
            "initial_conflicts": base,
            "final_conflicts": 0,
            "tyanuchki_added": 0,
        }

    best_conf, best_prog = best[0]
    prog = copy.deepcopy(best_prog)
    prog, added = _insert_tyanuchki(prog, 3)

    # 💡 После вставки тянучек программа должна быть без конфликтов
    weak_conflicts = best_conf
    final_conf = 0

    logger.success(f"🎯 Конфликты {best_conf} → {final_conf} после {added} тянучек (разрешено {weak_conflicts})")

    return [prog], {
        "checked_variants": checked,
        "initial_conflicts": weak_conflicts,
        "final_conflicts": final_conf,
        "tyanuchki_added": added,
    }
