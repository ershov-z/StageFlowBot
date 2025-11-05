import itertools
from loguru import logger
import copy

# ============================================================
# 🔧 Подсчёт конфликтов между соседними номерами
# ============================================================

def _count_conflicts(program):
    """Подсчёт количества конфликтов в программе"""
    conflicts = 0
    for i in range(len(program) - 1):
        current_actors = {a["name"] for a in program[i]["actors"]}
        next_actors = {a["name"] for a in program[i + 1]["actors"]}

        # Игнорируем "тянучки"
        if program[i]["type"] == "тянучка" or program[i + 1]["type"] == "тянучка":
            continue

        # Проверяем, если актёр в обоих номерах без (гк)
        shared = [
            a for a in current_actors.intersection(next_actors)
            if not any(t.get("tags") and "gk" in t.get("tags", []) for t in program[i]["actors"])
        ]
        if shared:
            conflicts += 1

    return conflicts


# ============================================================
# 🔁 Генерация всех возможных перестановок
# ============================================================

def _generate_permutations(program):
    """Создаёт все перестановки допустимых номеров, не трогая фиксированные"""
    fixed_indices = []
    movable_indices = []

    for idx, item in enumerate(program):
        if item["type"] in ["предкулисье", "спонсоры"]:
            fixed_indices.append(idx)
        elif item.get("num") in ["1", "2", "13"]:
            fixed_indices.append(idx)
        else:
            movable_indices.append(idx)

    fixed_indices = sorted(set(fixed_indices))
    movable = [program[i] for i in movable_indices]

    permutations = list(itertools.permutations(movable))
    logger.info(f"🔢 Сгенерировано {len(permutations)} перестановок для {len(movable)} элементов.")
    return permutations, fixed_indices, movable_indices


# ============================================================
# ➕ Вставка тянучек
# ============================================================

def _insert_tyanuchki(program):
    """Добавляет тянучки в конфликтные места"""
    tcount = 0
    actors_priority = ["Пушкин", "Исаев", "Рожков"]

    i = 0
    while i < len(program) - 1:
        current = program[i]
        nxt = program[i + 1]

        current_actors = {a["name"] for a in current["actors"]}
        next_actors = {a["name"] for a in nxt["actors"]}

        if current["type"] != "тянучка" and nxt["type"] != "тянучка":
            shared = current_actors.intersection(next_actors)
            if shared:
                for actor in actors_priority:
                    prev_has_gk = any(
                        actor == a["name"] and "gk" in a["tags"] for a in current["actors"]
                    )
                    next_has_gk = any(
                        actor == a["name"] and "gk" in a["tags"] for a in nxt["actors"]
                    )
                    if not prev_has_gk and not next_has_gk:
                        tyanuchka = {
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
                        program.insert(i + 1, tyanuchka)
                        tcount += 1
                        logger.info(f"➕ Вставлена тянучка ({actor}) между {current['title']} и {nxt['title']}")
                        break
        i += 1

    return program, tcount


# ============================================================
# 🎯 Основная функция генерации
# ============================================================

def generate_program_variants(program):
    """Создаёт все перестановки, находит лучшие и добавляет тянучки"""
    logger.info("🧩 Запуск генерации всех перестановок программы...")

    permutations, fixed_indices, movable_indices = _generate_permutations(program)

    evaluated = []
    checked_variants = 0
    for perm in permutations:
        # Собираем полный порядок с фиксами
        new_program = []
        movable_iter = iter(perm)
        for i in range(len(program)):
            if i in fixed_indices:
                new_program.append(program[i])
            else:
                new_program.append(next(movable_iter))

        conflicts = _count_conflicts(new_program)
        evaluated.append((conflicts, new_program))
        checked_variants += 1

    if not evaluated:
        logger.warning("⚠️ Не удалось сгенерировать ни одного варианта!")
        return [program], {"checked_variants": 0, "tyanuchki_added": 0}

    # Сортировка по количеству конфликтов
    evaluated.sort(key=lambda x: x[0])
    best_conflicts = evaluated[0][0]
    top_variants = evaluated[: min(5, len(evaluated))]

    logger.info(f"✅ Проверено {checked_variants} вариантов.")
    logger.info(f"🏆 Лучший результат: {best_conflicts} конфликт(ов).")
    logger.info("📋 Топ-5 вариантов с минимальными конфликтами:")
    for i, (c, v) in enumerate(top_variants, start=1):
        titles = [item["title"] for item in v]
        logger.info(f"  {i}. Конфликтов: {c} → {' | '.join(titles)}")

    # Берём лучший вариант
    best_program = copy.deepcopy(evaluated[0][1])
    best_program, tenuchki_count = _insert_tyanuchki(best_program)

    final_conflicts = _count_conflicts(best_program)
    logger.success(f"🎯 Финальный вариант готов. Конфликтов после тянучек: {final_conflicts}")

    stats = {
        "checked_variants": checked_variants,
        "initial_conflicts": best_conflicts,
        "final_conflicts": final_conflicts,
        "tyanuchki_added": tenuchki_count,
    }

    return [best_program], stats
