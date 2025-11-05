# utils/validator.py
import copy
from loguru import logger


# ============================================================
# 🔧 Подсчёт конфликтов между соседними номерами
# ============================================================

def _has_gk(item, actor_name):
    for a in item.get("actors", []):
        if a.get("name") == actor_name and "gk" in (a.get("tags") or []):
            return True
    return False


def _adjacent_conflict(left, right):
    """Возвращает 1 если между left и right есть конфликт, иначе 0."""
    if left is None or right is None:
        return 0

    if left.get("type") == "тянучка" or right.get("type") == "тянучка":
        return 0

    left_names = {a["name"] for a in left.get("actors", [])}
    right_names = {a["name"] for a in right.get("actors", [])}
    shared = left_names & right_names

    for actor in shared:
        if not _has_gk(left, actor) and not _has_gk(right, actor):
            return 1
    return 0


def _count_conflicts(program):
    """Подсчёт конфликтов по всей программе."""
    conflicts = 0
    for i in range(len(program) - 1):
        conflicts += _adjacent_conflict(program[i], program[i + 1])
    return conflicts


# ============================================================
# 🧱 Фиксированные элементы
# ============================================================

def _compute_fixed_indices(program):
    n = len(program)
    fixed = {0, 1, 2, max(0, n - 2), max(0, n - 1)}
    for idx, item in enumerate(program):
        if item.get("type") == "спонсоры":
            fixed.add(idx)
    fixed_indices = sorted(i for i in fixed if 0 <= i < n)
    movable_indices = [i for i in range(n) if i not in fixed_indices]
    return fixed_indices, movable_indices


# ============================================================
# 🔁 Backtracking + ранний стоп
# ============================================================

def _search_best_variants(program, max_results=5, max_checked=None, max_conflicts_allowed=3):
    """
    Оптимизированный backtracking с ранним стопом:
      - ищем сначала вариант без конфликтов (0)
      - если нет — ждём с 1 конфликтом
      - если нет — с 2
      - если нет — с 3
      - варианты с >3 конфликтами не рассматриваются
    """
    n = len(program)
    fixed_indices, movable_indices = _compute_fixed_indices(program)
    movable_elems = [program[i] for i in movable_indices]

    best_heap = []
    checked_variants = 0
    best_conflicts_seen = float("inf")
    found_target_conflicts = None  # 0, 1, 2 или 3

    current = [None] * n
    for i in fixed_indices:
        current[i] = program[i]
    used = [False] * len(movable_elems)

    def backtrack(pos, current_conflicts):
        nonlocal checked_variants, best_conflicts_seen, found_target_conflicts

        # Превышен лимит — отсекаем
        if current_conflicts > max_conflicts_allowed:
            return

        # Если уже нашли вариант с меньшим количеством конфликтов — можно не продолжать
        if found_target_conflicts is not None and current_conflicts > found_target_conflicts:
            return

        # Дошли до конца — готовый вариант
        if pos >= n:
            checked_variants += 1
            if current_conflicts <= best_conflicts_seen:
                variant_copy = copy.deepcopy(current)
                best_heap.append((current_conflicts, variant_copy))
                best_heap.sort(key=lambda x: x[0])
                if len(best_heap) > max_results:
                    best_heap[:] = best_heap[:max_results]
                best_conflicts_seen = best_heap[0][0]

            # Проверяем, можно ли завершить поиск
            if found_target_conflicts is None or current_conflicts < found_target_conflicts:
                found_target_conflicts = current_conflicts

            # Если нашли 0 конфликтов — стоп
            if found_target_conflicts == 0:
                raise StopIteration
            return

        # Пропускаем фиксированные позиции
        if current[pos] is not None:
            backtrack(pos + 1, current_conflicts)
            return

        for i, elem in enumerate(movable_elems):
            if used[i]:
                continue
            current[pos] = elem
            used[i] = True

            added = 0
            if pos - 1 >= 0 and current[pos - 1] is not None:
                added = _adjacent_conflict(current[pos - 1], current[pos])
            new_conflicts = current_conflicts + added

            if new_conflicts <= max_conflicts_allowed:
                if max_checked is None or checked_variants < max_checked:
                    backtrack(pos + 1, new_conflicts)

            used[i] = False
            current[pos] = None

    try:
        backtrack(0, 0)
    except StopIteration:
        pass

    best_heap.sort(key=lambda x: x[0])
    return best_heap[:max_results], checked_variants


# ============================================================
# ➕ Вставка тянучек (максимум 3)
# ============================================================

def _insert_tyanuchki(program, max_tyanuchki=3):
    """Добавляет максимум три тянучки в конфликтные места."""
    tcount = 0
    actors_priority = ["Пушкин", "Исаев", "Рожков"]

    def has_gk(item, actor):
        for a in item.get("actors", []):
            if a.get("name") == actor and "gk" in (a.get("tags") or []):
                return True
        return False

    i = 0
    while i < len(program) - 1:
        if tcount >= max_tyanuchki:
            break

        left = program[i]
        right = program[i + 1]
        if left.get("type") == "тянучка" or right.get("type") == "тянучка":
            i += 1
            continue

        left_names = {a["name"] for a in left.get("actors", [])}
        right_names = {a["name"] for a in right.get("actors", [])}
        shared = left_names & right_names

        if shared:
            for actor in actors_priority:
                if not has_gk(left, actor) and not has_gk(right, actor):
                    tyan = {
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
                    program.insert(i + 1, tyan)
                    tcount += 1
                    logger.info(
                        f"➕ Вставлена тянучка ({actor}) между «{left.get('title')}» и «{right.get('title')}». "
                        f"Всего тянучек: {tcount}"
                    )
                    break
        i += 1
    return program, tcount


# ============================================================
# 🎯 Основная функция генерации
# ============================================================

def generate_program_variants(program, top_n=5, max_checked_variants=None):
    """
    Оптимизированная генерация вариантов:
    - не генерирует все перестановки;
    - выполняет backtracking с отсечением и ранним стопом по количеству конфликтов;
    - вставляет максимум три тянучки.
    """
    logger.info("🧩 Запуск оптимизированной генерации перестановок программы...")

    if not program or len(program) < 2:
        logger.warning("⚠️ Программа слишком короткая для перестановок.")
        base_conflicts = _count_conflicts(program)
        return [program], {
            "checked_variants": 0,
            "initial_conflicts": base_conflicts,
            "final_conflicts": base_conflicts,
            "tyanuchki_added": 0,
        }

    top_results, checked = _search_best_variants(
        program, max_results=top_n, max_checked=max_checked_variants, max_conflicts_allowed=3
    )

    if not top_results:
        logger.warning("⚠️ Не найдено ни одного допустимого варианта (≤3 конфликтов).")
        base_conflicts = _count_conflicts(program)
        return [program], {
            "checked_variants": checked,
            "initial_conflicts": base_conflicts,
            "final_conflicts": base_conflicts,
            "tyanuchki_added": 0,
        }

    logger.info(f"✅ Проверено вариантов: {checked}")
    logger.info(f"📋 Топ-{len(top_results)} лучших вариантов (до тянучек):")
    for i, (c, v) in enumerate(top_results, start=1):
        titles = [item.get('title') for item in v]
        logger.info(f"  {i}. Конфликтов: {c} → {' | '.join(titles)}")

    best_conflicts, best_program = top_results[0]
    best_program = copy.deepcopy(best_program)
    best_program, t_added = _insert_tyanuchki(best_program, max_tyanuchki=3)

    final_conflicts = _count_conflicts(best_program)
    logger.success(
        f"🎯 Финал: было конфликтов {best_conflicts}, добавлено тянучек {t_added}, "
        f"осталось конфликтов {final_conflicts}"
    )

    stats = {
        "checked_variants": checked,
        "initial_conflicts": best_conflicts,
        "final_conflicts": final_conflicts,
        "tyanuchki_added": t_added,
    }
    return [best_program], stats
