from loguru import logger
import copy

# ============================================================
# 🔹 НАСТРОЙКИ
# ============================================================

# Приоритет ведущих тянучек
TENUCHKA_PRIORITY = ["Пушкин", "Исаев", "Рожков"]

# Типы номеров, которые нельзя трогать (фиксация позиции)
ANCHOR_TYPES = {"предкулисье", "спонсоры"}

# ============================================================
# 🔹 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def has_tag(actor, tag):
    """Проверяет наличие тега у актёра"""
    if not actor or not actor.get("tags"):
        return False
    return tag in actor["tags"]


def find_actor(entry, name):
    """Ищет актёра по имени в entry['actors']"""
    for a in entry.get("actors", []):
        if a["name"].strip().lower() == name.strip().lower():
            return a
    return None


def can_host(actor_name, prev_entry, next_entry):
    """
    Проверяет, может ли актёр вести тянучку между двумя номерами.
    Актёр не может вести тянучку, если:
    - он есть в предыдущем или следующем номере
    - и имеет тег (гк)
    """
    if not actor_name:
        return False

    actor_name = actor_name.strip().lower()

    for entry in (prev_entry, next_entry):
        for a in entry.get("actors", []):
            if a["name"].strip().lower() == actor_name:
                if has_tag(a, "gk"):
                    return False  # актёр занят с гк, нельзя
    return True


def create_tenuchka(actor_name):
    """Создаёт структуру тянучки"""
    return {
        "order": None,
        "num": "",
        "title": f"Тянучка ({actor_name})",
        "actors_raw": "",
        "pp": actor_name,
        "hire": "",
        "responsible": actor_name,
        "kv": False,
        "type": "тянучка",
        "actors": [{"name": actor_name, "tags": []}],
    }


def is_conflict(entry_a, entry_b):
    """
    Проверяет, есть ли конфликт актёров между двумя номерами.
    Конфликт — если один и тот же актёр встречается подряд
    (исключая случаи с разными тегами early/later).
    """
    actors_a = [a["name"].strip().lower() for a in entry_a.get("actors", [])]
    actors_b = [a["name"].strip().lower() for a in entry_b.get("actors", [])]

    for a in actors_a:
        if a in actors_b:
            a1 = find_actor(entry_a, a)
            a2 = find_actor(entry_b, a)
            # если нет тегов early/later — это конфликт
            if not (has_tag(a1, "early") or has_tag(a2, "later")):
                return True
    return False


def is_anchor(entry, index, total_len):
    """Проверяет, можно ли двигать элемент"""
    if (entry.get("type") or "").lower() in ANCHOR_TYPES:
        return True
    if index in (0, 1, total_len - 2, total_len - 1):  # 1-й, 2-й, предпоследний, последний
        return True
    return False


# ============================================================
# 🔹 ГЛАВНАЯ ЛОГИКА ВАЛИДАЦИИ
# ============================================================

def generate_program_variants(entries):
    """
    Проверяет сетку и формирует один или несколько валидных вариантов.
    Возвращает:
        (variants, tenuchka_count)
    """

    if not entries:
        logger.warning("⚠️ Пустая программа, нечего валидировать.")
        return [], 0

    logger.info("🧩 Запуск валидации программы...")
    entries = copy.deepcopy(entries)
    tenuchka_count = 0

    result = []
    total_len = len(entries)

    for i, entry in enumerate(entries):
        result.append(entry)

        # пропускаем последний
        if i >= total_len - 1:
            continue

        next_entry = entries[i + 1]

        # якоря не трогаем
        if is_anchor(entry, i, total_len) or is_anchor(next_entry, i + 1, total_len):
            continue

        # проверяем конфликт между entry и next_entry
        if is_conflict(entry, next_entry):
            logger.info(f"⚠️ Конфликт между «{entry['title']}» и «{next_entry['title']}».")
            inserted = False

            # выбираем ведущего по приоритету
            for actor in TENUCHKA_PRIORITY:
                if can_host(actor, entry, next_entry):
                    tenuchka = create_tenuchka(actor)
                    result.append(tenuchka)
                    tenuchka_count += 1
                    inserted = True
                    logger.info(f"➕ Вставлена тянучка ({actor}) между {entry['title']} и {next_entry['title']}.")
                    break

            if not inserted:
                logger.warning(f"❌ Не удалось вставить тянучку между {entry['title']} и {next_entry['title']}.")

    logger.success(f"🎯 Валидация завершена. Добавлено {tenuchka_count} тянучек.")
    return [result], tenuchka_count
