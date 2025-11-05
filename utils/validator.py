# Импортируем библиотеки и начнем обработку нового валидатора с учетом всех требований

import copy
import random
from loguru import logger

# ============================================================
# 🔧 Вспомогательные проверки
# ============================================================

def _is_tyan(item):
    return item is not None and item.get("type") == "тянучка"


def _is_number(item):
    return item is not None and item.get("type") != "тянучка"


def _is_kv(item):
    return bool(item.get("kv"))


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


def _has_gk(item, name):
    return "gk" in _actor_tags(item, name)


def _has_later(item, name):
    return "later" in _actor_tags(item, name)


# ============================================================
# ⚔️ Конфликты и запрещённые соседства
# ============================================================

def _adjacent_conflict(left, right):
    if not (_is_number(left) and _is_number(right)):
        return 0
    shared = {a["name"] for a in left["actors"]} & {a["name"] for a in right["actors"]}
    for actor in shared:
        if not _has_gk(left, actor) and not _has_gk(right, actor):
            return 1
    return 0


def _adjacency_forbidden(left, right):
    if not (_is_number(left) and _is_number(right)):
        return False
    # KV подряд запрещено
    if _is_kv(left) and _is_kv(right):
        return True
    # gk-разрыв запрещён
    shared = {a["name"] for a in left["actors"]} & {a["name"] for a in right["actors"]}
    for actor in shared:
        if _has_gk(left, actor) or _has_gk(right, actor):
            return True
    return False


def _count_conflicts(program):
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
# 🔍 Проверка KV-цепочек (через тянучки)
# ============================================================

def _has_kv_violation(program):
    """True, если два KV-номера идут подряд даже через тянучки."""
    last_kv = None
    for i, p in enumerate(program):
        if _is_number(p) and _is_kv(p):
            if last_kv is not None:
                between = program[last_kv + 1:i]
                if all(_is_tyan(x) for x in between):
                    return True
            last_kv = i
    return False


# ============================================================
# 🔍 Проверка gk-разрывов сквозь тянучки
# ============================================================

def _has_gk_violation(program):
    """True, если актёр с gk появляется снова, и между появлениями только тянучки."""
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
# 🔁 Поиск с фильтрацией KV и gk
# ============================================================

def _search_best_variants(program, max_results=5, max_conflicts_allowed=3):
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

    def backtrack(pos, confs):
        nonlocal checked, best_conf, found_zero
        if confs > max_conflicts_allowed or found_zero:
            return
        while pos < n and current[pos] is not None:
            pos += 1
        if pos >= n:
            checked += 1
            if _has_kv_violation(current) or _has_gk_violation(current):
                logger.debug("⛔ Отброшен вариант из-за KV или gk-разрыва")
                return
            if confs <= best_conf:
                best.append((confs, copy.deepcopy(current)))
                best.sort(key=lambda x: x[0])
                best[:] = best[:max_results]
                best_conf = best[0][0]
            if confs == 0:
                found_zero = True
            return
        left = current[pos - 1] if pos > 0 else None
        choices = [i for i, u in enumerate(used) if not u]
        random.shuffle(choices)
        for i in choices:
            el = movables[i]
            if left and _adjacency_forbidden(left, el):
                continue
            if left and _is_number(left) and _is_kv(left) and _is_kv(el):
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

    backtrack(0, 0)
    return best[:max_results], checked


# ============================================================
# 🪶 Тянучки
# ============================================================

def _can_actor_host_tyan(program, idx, actor):
    """
    Новая логика ведущего:
      - нельзя, если актёр есть в следующем номере (R), кроме случая если там тег 'later';
      - нельзя, если в следующем номере у него 'gk';
      - можно, если через один номер (R+1) он появляется — это разрешено;
      - можно, если в R+1 у него 'gk' — это не влияет;
      - можно, если он больше не встречается.
    """
    n = len(program)
    # Следующий номер
    if idx + 1 < n and _is_number(program[idx + 1]):
        nxt = program[idx + 1]
        if _has_gk(nxt, actor):
            return False
        if _has_actor(nxt, actor) and not _has_later(nxt, actor):
            return False

    # Через один номер — разрешено в любом случае
    return True


def _insert_tyanuchki(program, max_tyanuchki=3):
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
                        "responsible": actor, "kv": False,
                        "type": "тянучка", "actors": [{"name": actor, "tags": []}]
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

def generate_program_variants(program, top_n=5):
    logger.info("🧩 Генерация вариантов программы...")
    if not program or len(program) < 2:
        base = _count_conflicts(program)
        return [program], {"checked_variants": 0, "initial_conflicts": base,
                           "final_conflicts": base, "tyanuchki_added": 0}

    best, checked = _search_best_variants(program)
    if not best:
        base = _count_conflicts(program)
        return [program], {"checked_variants": checked, "initial_conflicts": base,
                           "final_conflicts": base, "tyanuchki_added": 0}

    logger.info(f"✅ Проверено {checked} вариантов")
    best_conf, best_prog = best[0]
    prog = copy.deepcopy(best_prog)
    prog, added = _insert_tyanuchki(prog, 3)

    # Финальная KV/gk проверка
    if _has_kv_violation(prog):
        logger.warning("⚠️ Финальный вариант содержит KV подряд — отброшен")
        return [program], {"checked_variants": checked, "initial_conflicts": best_conf,
                           "final_conflicts": None, "tyanuchki_added": added}
    if _has_gk_violation(prog):
        logger.warning("⚠️ Финальный вариант содержит gk-разрыв только с тянучками — отброшен")
        return [program], {"checked_variants": checked, "initial_conflicts": best_conf,
                           "final_conflicts": None, "tyanuchki_added": added}

    final_conf = _count_conflicts(prog)
    logger.success(f"🎯 Конфликтов {best_conf} → {final_conf} после {added} тянучек")
    return [prog], {"checked_variants": checked, "initial_conflicts": best_conf,
                    "final_conflicts": final_conf, "tyanuchki_added": added}
