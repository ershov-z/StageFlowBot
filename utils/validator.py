# utils/validator.py

import itertools
from loguru import logger
import copy

# ============================================================
# 🔧 Подсчёт конфликтов между соседними номерами
# ============================================================

def _count_conflicts(program):
    """Подсчёт количества конфликтов в программе."""
    conflicts = 0
    for i in range(len(program) - 1):
        left = program[i]
        right = program[i + 1]

        # тянучки не считаем как конфликтующие блоки
        if left["type"] == "тянучка" or right["type"] == "тянучка":
            continue

        left_names = {a["name"] for a in left["actors"]}
        right_names = {a["name"] for a in right["actors"]}

        shared = left_names & right_names
        if not shared:
            continue

        # конфликт считается только если для конкретного пересекающегося актёра
        # нет тега gk в левом ИЛИ правом номере (нам нужен запрет: «если (гк) рядом – тянучку ставить нельзя»)
        def has_gk(item, actor):
            for a in item["actors"]:
                if a["name"] == actor and "gk" in (a.get("tags") or []):
                    return True
            return False

        # Если нашёлся любой общий актёр без gk по обеим сторонам – это конфликт.
        for actor in shared:
            if not has_gk(left, actor) and not has_gk(right, actor):
                conflicts += 1
                break

    return conflicts


# ============================================================
# 🧱 Какие элементы фиксируем
# ============================================================

def _compute_fixed_indices(program):
    """
    Фиксируем по РАСПОЛОЖЕНИЮ:
      - 0 (предкулисье)
      - 1 (первый номер)
      - 2 (второй номер)
      - len-2 (предпоследний)
      - len-1 (последний)
    + отдельно фиксируем все элементы с type == 'спонсоры' (где бы они ни стояли).
    """
    n = len(program)
    fixed = {0, 1, 2, max(0, n - 2), max(0, n - 1)}

    for idx, item in enumerate(program):
        if item.get("type") == "спонсоры":
            fixed.add(idx)

    fixed_indices = sorted(i for i in fixed if 0 <= i < n)
    movable_indices = [i for i in range(n) if i not in fixed_indices]
    return fixed_indices, movable_indices


# ============================================================
# 🔁 Генерация всех возможных перестановок (только для «движимых»)
# ============================================================

def _generate_permutations(program):
    """Создаёт все перестановки только для подмножества «движимых» индексов."""
    fixed_indices, movable_indices = _compute_fixed_indices(program)
    movable = [program[i] for i in movable_indices]

    permutations = list(itertools.permutations(movable))
    logger.info(
        f"🔢 Сгенерировано {len(permutations)} перестановок для {len(movable)} движимых элементов. "
        f"(Фикс: {fixed_indices})"
    )
    return permutations, fixed_indices, movable_indices


# ============================================================
# ➕ Вставка тянучек
# ============================================================

def _insert_tyanuchki(program):
    """Добавляет тянучки в конфликтные места по приоритету актёров, уважая (гк)."""
    tcount = 0
    actors_priority = ["Пушкин", "Исаев", "Рожков"]

    def has_gk(item, actor):
        for a in item["actors"]:
            if a["name"] == actor and "gk" in (a.get("tags") or []):
                return True
        return False

    i = 0
    while i < len(program) - 1:
        left = program[i]
        right = program[i + 1]

        if left["type"] == "тянучка" or right["type"] == "тянучка":
            i += 1
            continue

        left_names = {a["name"] for a in left["actors"]}
        right_names = {a["name"] for a in right["actors"]}
        shared = left_names & right_names

        if shared:
            # Подбираем ведущего тянучки по приоритету, запрещая если у него (гк) слева/справа
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
                    logger.info(f"➕ Вставлена тянучка ({actor}) между «{left['title']}» и «{right['title']}».")
                    break
        i += 1

    return program, tcount


# ============================================================
# 🎯 Основная функция генерации
# ============================================================

def generate_program_variants(program):
    """
    1) Перебираем ВСЕ перестановки только для допускаемых к движению элементов.
    2) Оцениваем число конфликтов, выбираем лучший (минимум).
    3) В выбранный вариант вставляем тянучки там, где остались конфликты.
    4) Возвращаем один лучший вариант + статистику.
    """
    logger.info("🧩 Запуск генерации всех перестановок программы...")

    permutations, fixed_indices, movable_indices = _generate_permutations(program)

    evaluated = []
    checked_variants = 0
    for perm in permutations:
        # Собираем полный порядок с сохранением зафиксированных позиций
        new_program = []
        it = iter(perm)
        for i in range(len(program)):
            if i in fixed_indices:
                new_program.append(program[i])
            else:
                new_program.append(next(it))

        conflicts = _count_conflicts(new_program)
        evaluated.append((conflicts, new_program))
        checked_variants += 1

    if not evaluated:
        logger.warning("⚠️ Не удалось сгенерировать ни одного варианта!")
        return [program], {
            "checked_variants": 0,
            "initial_conflicts": _count_conflicts(program),
            "final_conflicts": _count_conflicts(program),
            "tyanuchki_added": 0,
        }

    evaluated.sort(key=lambda x: x[0])
    best_conflicts = evaluated[0][0]
    top_variants = evaluated[: min(5, len(evaluated))]

    logger.info(f"✅ Проверено вариантов: {checked_variants}")
    logger.info(f"🏆 Лучший результат без тянучек: {best_conflicts} конфликт(ов)")
    logger.info("📋 Топ-5 порядков:")
    for i, (c, v) in enumerate(top_variants, start=1):
        titles = [item["title"] for item in v]
        logger.info(f"  {i}. Конфликтов: {c} → {' | '.join(titles)}")

    # Берём лучший порядок и добавляем тянучки
    best_program = copy.deepcopy(evaluated[0][1])
    best_program, t_added = _insert_tyanuchki(best_program)

    final_conflicts = _count_conflicts(best_program)
    logger.success(
        f"🎯 Финал: было конфликтов {best_conflicts}, добавлено тянучек {t_added}, осталось конфликтов {final_conflicts}"
    )

    stats = {
        "checked_variants": checked_variants,
        "initial_conflicts": best_conflicts,
        "final_conflicts": final_conflicts,
        "tyanuchki_added": t_added,
    }
    return [best_program], stats
