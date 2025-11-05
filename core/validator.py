import itertools
import json
import time
from copy import deepcopy
from loguru import logger
from datetime import datetime
from pathlib import Path


# ============================================================
# 🔹 Константы
# ============================================================

TENUCHKA_ACTORS = ["Пушкин", "Исаев", "Рожков"]


# ============================================================
# 🔹 Проверка конфликта между номерами
# ============================================================

def _has_conflict(item_a, item_b):
    """Возвращает True, если между item_a и item_b конфликт"""
    if not item_a or not item_b:
        return False

    actors_a = {a["name"] for a in item_a.get("actors", [])}
    actors_b = {a["name"] for a in item_b.get("actors", [])}

    # Если есть пересечение актёров
    if actors_a & actors_b:
        # Проверяем наличие "гк" у любого из них
        has_gk_a = any("gk" in a["tags"] for a in item_a["actors"])
        has_gk_b = any("gk" in a["tags"] for a in item_b["actors"])
        if has_gk_a or has_gk_b:
            return True
        # Проверяем близкие теги
        tags_a = set(t for a in item_a["actors"] for t in a["tags"])
        tags_b = set(t for a in item_b["actors"] for t in a["tags"])
        if ("later" in tags_a and "later" in tags_b) or ("early" in tags_a and "early" in tags_b):
            return True
        return True

    return False


# ============================================================
# 🔹 Подсчёт конфликтов в последовательности
# ============================================================

def _count_conflicts(program):
    """Подсчёт количества конфликтов между соседними номерами"""
    count = 0
    for i in range(len(program) - 1):
        if _has_conflict(program[i], program[i + 1]):
            count += 1
    return count


# ============================================================
# 🔹 Вставка тянучек для устранения конфликтов
# ============================================================

def _insert_tenuchki(program):
    """Добавляет тянучки между конфликтными номерами"""
    fixed_program = []
    tenuchki_count = 0

    for i in range(len(program)):
        fixed_program.append(program[i])
        if i < len(program) - 1 and _has_conflict(program[i], program[i + 1]):
            for actor in TENUCHKA_ACTORS:
                prev_has_gk = any(a["name"] == actor and "gk" in a["tags"] for a in program[i]["actors"])
                next_has_gk = any(a["name"] == actor and "gk" in a["tags"] for a in program[i + 1]["actors"])
                if not (prev_has_gk or next_has_gk):
                    tenuchka = {
                        "order": f"T-{i+1}",
                        "num": "",
                        "title": f"Тянучка ({actor})",
                        "actors_raw": actor,
                        "pp": actor,
                        "hire": "",
                        "responsible": actor,
                        "kv": False,
                        "type": "тянучка",
                        "actors": [{"name": actor, "tags": []}]
                    }
                    fixed_program.append(tenuchka)
                    logger.info(f"➕ Вставлена тянучка ({actor}) между «{program[i]['title']}» и «{program[i+1]['title']}».")
                    tenuchki_count += 1
                    break

    return fixed_program, tenuchki_count


# ============================================================
# 🔹 Основная функция генерации вариантов
# ============================================================

def generate_program_variants(program):
    """
    Генерирует все возможные перестановки, считает конфликты,
    выбирает лучшие 5 и добавляет тянучки в лучший вариант.
    """
    logger.info("🧩 Запуск валидации программы...")

    start_time = time.time()

    # === Выделяем фиксированные и подвижные элементы ===
    fixed_indices = []
    movable_items = []

    for i, item in enumerate(program):
        title = item["title"].lower()
        if "предку" in title or "спонсор" in title:
            fixed_indices.append(i)
        elif i in (1, 2, len(program) - 2, len(program) - 1):
            fixed_indices.append(i)
        else:
            movable_items.append(item)

    # === Генерация всех перестановок ===
    permutations = list(itertools.permutations(movable_items))
    checked_variants = len(permutations)
    logger.info(f"📊 Проверяется {checked_variants} перестановок...")

    results = []
    for perm in permutations:
        variant = deepcopy(program)
        movable_iter = iter(perm)
        for i in range(len(variant)):
            if i not in fixed_indices:
                variant[i] = next(movable_iter)

        conflicts = _count_conflicts(variant)
        results.append({"conflicts": conflicts, "variant": variant})

    # === Сортировка и топ-5 ===
    results.sort(key=lambda x: x["conflicts"])
    best_variants = results[:5]

    logger.info(f"✅ Всего вариантов: {checked_variants}")
    logger.info(f"🏆 Лучшие варианты:")

    for i, var in enumerate(best_variants, 1):
        titles = [v["title"] for v in var["variant"]]
        logger.info(f"  #{i}: {var['conflicts']} конфликтов → {titles}")

    # === Сохраняем топ-5 в JSON ===
    Path("logs").mkdir(exist_ok=True)
    best_data = {
        "checked_variants": checked_variants,
        "best_variants": [
            {
                "conflicts": var["conflicts"],
                "sequence": [v["title"] for v in var["variant"]],
            }
            for var in best_variants
        ],
    }
    out_path = Path(f"logs/best_variants_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(best_data, f, ensure_ascii=False, indent=2)

    # === Выбираем лучший вариант и добавляем тянучки ===
    best_variant = best_variants[0]["variant"]
    final_program, tenuchki_count = _insert_tenuchki(best_variant)

    elapsed = time.time() - start_time
    logger.success(f"🎯 Валидация завершена. Проверено {checked_variants} вариантов за {elapsed:.2f} сек.")
    logger.success(f"Добавлено тянучек: {tenuchki_count}.")

    return final_program, tenuchki_count, checked_variants
