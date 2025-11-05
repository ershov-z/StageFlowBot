import itertools
from copy import deepcopy
from loguru import logger

# ============================================================
# 🔹 Проверка конфликтов между номерами
# ============================================================

def has_conflict(item_a, item_b):
    """Возвращает True, если между двумя номерами конфликт."""
    actors_a = {a["name"] for a in item_a.get("actors", [])}
    actors_b = {a["name"] for a in item_b.get("actors", [])}

    # Игнорируем "Все"
    if "Все" in actors_a or "Все" in actors_b:
        return False

    common = actors_a & actors_b
    if not common:
        return False

    # Проверяем, есть ли тег (гк)
    for actor in item_a.get("actors", []) + item_b.get("actors", []):
        if "gk" in actor.get("tags", []):
            return False

    return True


# ============================================================
# 🔹 Вставка тянучки
# ============================================================

def make_tyanuchka(between_a, between_b, used_pull=None):
    """Создаёт тянучку между двумя конфликтующими номерами."""
    used_pull = used_pull or set()
    candidates = ["Пушкин", "Исаев", "Рожков"]

    for cand in candidates:
        if cand not in used_pull:
            used_pull.add(cand)
            return {
                "order": None,
                "num": "",
                "title": f"Тянучка, ждём {cand}",
                "actors_raw": cand,
                "pp": "",
                "hire": "",
                "responsible": cand,
                "kv": False,
                "type": "тянучка",
                "actors": [{"name": cand, "tags": []}],
            }

    # fallback
    return {
        "order": None,
        "num": "",
        "title": "Тянучка (резерв)",
        "actors_raw": "Пушкин",
        "pp": "",
        "hire": "",
        "responsible": "Пушкин",
        "kv": False,
        "type": "тянучка",
        "actors": [{"name": "Пушкин", "tags": []}],
    }


# ============================================================
# 🔹 Основной валидатор
# ============================================================

def generate_program_variants(program):
    """
    Перебирает все допустимые перестановки номеров,
    выбирает вариант с минимальным числом конфликтов,
    вставляет тянучки при необходимости.
    """

    logger.info("🧩 Запуск валидации программы...")

    # 1️⃣ Разделяем неизменяемые и переставляемые блоки
    immovable = []
    movable = []

    for idx, item in enumerate(program):
        title = item.get("title", "").lower()
        if any(
            key in title
            for key in ["предкулисье", "спонсоры", "финальная", "конец"]
        ) or idx in [0, 1, len(program) - 1, len(program) - 2]:
            immovable.append((idx, item))
        else:
            movable.append((idx, item))

    logger.info(
        f"📌 Фиксированные позиции: {[i for i, _ in immovable]}, "
        f"переставляемых: {len(movable)}"
    )

    movable_items = [x[1] for x in movable]
    permutations = list(itertools.permutations(movable_items))
    total_checked = len(permutations)
    logger.info(f"🔄 Всего перестановок для проверки: {total_checked}")

    best_variant = None
    min_conflicts = float("inf")

    # 2️⃣ Проверяем каждую перестановку
    for perm_index, perm in enumerate(permutations, 1):
        candidate = deepcopy(program)
        movable_iter = iter(perm)
        for idx, _ in movable:
            candidate[idx] = next(movable_iter)

        # Считаем количество конфликтов
        conflicts = 0
        for i in range(len(candidate) - 1):
            if has_conflict(candidate[i], candidate[i + 1]):
                conflicts += 1

        if conflicts < min_conflicts:
            best_variant = candidate
            min_conflicts = conflicts
            logger.debug(f"🔎 Новая лучшая перестановка #{perm_index}: {conflicts} конфликтов")

        if min_conflicts == 0:
            logger.info(f"✅ Найден идеальный вариант без конфликтов на перестановке #{perm_index}")
            break

    if best_variant is None:
        logger.error("❌ Не удалось сгенерировать перестановки.")
        return [], {
            "initial_conflicts": 0,
            "final_conflicts": 0,
            "tyanuchki_added": 0,
            "checked_variants": total_checked,
        }

    logger.info(f"🎯 Лучшая перестановка найдена, конфликтов: {min_conflicts}")

    # 3️⃣ Добавляем тянучки при конфликтах
    result = []
    used_pull = set()
    tcount = 0
    conflicts_before = 0
    conflicts_after = 0

    for i in range(len(best_variant) - 1):
        a = best_variant[i]
        b = best_variant[i + 1]
        result.append(a)

        if has_conflict(a, b):
            conflicts_before += 1
            tyan = make_tyanuchka(a, b, used_pull)
            result.append(tyan)
            tcount += 1
            logger.info(f"➕ Добавлена тянучка между «{a['title']}» и «{b['title']}».")
        else:
            logger.debug(f"✅ Без конфликта: {a['title']} → {b['title']}")

    result.append(best_variant[-1])

    logger.success(f"✅ Программа собрана. Тянучек добавлено: {tcount}")
    stats = {
        "initial_conflicts": min_conflicts,
        "final_conflicts": 0,
        "tyanuchki_added": tcount,
        "checked_variants": total_checked,
    }

    return [result], stats
