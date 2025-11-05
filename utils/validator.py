from itertools import permutations
from loguru import logger
import copy


# ============================================================
# 🔹 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def normalize_actors(entry):
    """Возвращает set имён актёров (без тегов) из entry["actors"]"""
    return {actor["name"].strip() for actor in entry.get("actors", []) if actor["name"].strip()}


def actor_has_tag(entry, actor_name, tag):
    """Проверяет, есть ли у актёра указанный тег"""
    for actor in entry.get("actors", []):
        if actor["name"].strip().lower() == actor_name.strip().lower():
            if tag in actor["tags"]:
                return True
    return False


# ============================================================
# 🔹 ПРОВЕРКИ
# ============================================================

def check_conflicts(a, b):
    """
    Проверяет, можно ли ставить номер B после A.
    Возвращает (bool, reason)
    """
    actors_a = normalize_actors(a)
    actors_b = normalize_actors(b)

    # 1. Проверка КВ (локация "квартира")
    if a.get("kv") and b.get("kv"):
        return False, "⚠ два номера с kv подряд"

    # 2. Проверка пересечения актёров
    for name in actors_a & actors_b:
        # если актёр в обоих номерах
        tag_a_gk = actor_has_tag(a, name, "gk")
        tag_b_gk = actor_has_tag(b, name, "gk")
        tag_a_later = actor_has_tag(a, name, "later")
        tag_b_later = actor_has_tag(b, name, "later")
        tag_a_early = actor_has_tag(a, name, "early")
        tag_b_early = actor_has_tag(b, name, "early")

        # (гк) всегда конфликтует, если подряд
        if tag_a_gk or tag_b_gk:
            return False, f"🎭 актёр {name} с (гк) подряд"

        # если нет разрешающих тегов — конфликт
        if not (tag_a_early or tag_b_later):
            return False, f"👥 актёр {name} подряд без тегов"

    return True, "ok"


# ============================================================
# 🔹 ТЯНУЧКА
# ============================================================

def get_tyanuchka_actor():
    """Возвращает приоритетного актёра для тянучки"""
    for name in ["Пушкин", "Исаев", "Рожков"]:
        return name  # всегда начинаем с Пушкина
    return "—"


def insert_tyanuchka(seq, index):
    """Вставляет тянучку между индексами index и index+1"""
    actor_name = get_tyanuchka_actor()

    tyan = {
        "order": 999,
        "num": "",
        "title": "Тянучка",
        "actors_raw": actor_name,
        "pp": "",
        "hire": "",
        "responsible": "",
        "kv": False,
        "type": "тянучка",
        "actors": [{"name": actor_name, "tags": []}],
    }

    seq.insert(index + 1, tyan)
    logger.info(f"🧩 Добавлена тянучка ({actor_name}) между {seq[index]['title']} и {seq[index + 1]['title']}")
    return seq


def can_place_tyanuchka_between(a, b, actor_name):
    """
    Проверяет, можно ли вставить тянучку с данным актёром между A и B.
    Тянучка невозможна, если актёр имеет (гк) в одном из соседних номеров.
    """
    return not (actor_has_tag(a, actor_name, "gk") or actor_has_tag(b, actor_name, "gk"))


# ============================================================
# 🔹 ОСНОВНАЯ ЛОГИКА ГЕНЕРАЦИИ
# ============================================================

def generate_program_variants(data):
    """
    Генерирует все допустимые комбинации программы.
    Возвращает кортеж: (список_комбинаций, количество_тянучек)
    """

    fixed = []
    movable = []

    # разделяем фиксированные и подвижные номера
    for entry in data:
        if entry["type"] in ["предкулисье", "спонсоры"]:
            fixed.append(entry)
        else:
            movable.append(entry)

    logger.info(f"📋 Фиксировано {len(fixed)} номеров, можно двигать {len(movable)}")

    valid_variants = []
    min_tyan_count = float("inf")

    for perm in permutations(movable):
        seq = []
        seq.extend(fixed[:1])  # Предкулисье всегда в начале
        seq.extend(list(perm))
        seq.extend(fixed[1:])  # Спонсоры и финал — в конце

        tyan_count = 0
        i = 0
        while i < len(seq) - 1:
            ok, reason = check_conflicts(seq[i], seq[i + 1])
            if not ok:
                actor = get_tyanuchka_actor()
                if can_place_tyanuchka_between(seq[i], seq[i + 1], actor):
                    seq = insert_tyanuchka(seq, i)
                    tyan_count += 1
                    i += 1
                else:
                    logger.debug(f"🚫 Невозможно вставить тянучку между {seq[i]['title']} и {seq[i + 1]['title']}")
                    break
            i += 1

        # Проверим после вставки
        all_ok = all(check_conflicts(seq[j], seq[j + 1])[0] for j in range(len(seq) - 1))
        if all_ok:
            valid_variants.append(seq)
            min_tyan_count = min(min_tyan_count, tyan_count)

    if not valid_variants:
        logger.warning("❌ Не найдено корректных комбинаций даже с тянучками.")
        return [], 0

    logger.info(f"✅ Найдено {len(valid_variants)} корректных комбинаций. Мин. тянучек: {min_tyan_count}")
    return valid_variants, min_tyan_count


# ============================================================
# 🔹 ОТЛАДОЧНЫЙ ЗАПУСК
# ============================================================

if __name__ == "__main__":
    # пример тестовых данных
    example = [
        {
            "title": "Предкулисье",
            "type": "предкулисье",
            "kv": False,
            "actors": [{"name": "Пушкин", "tags": []}],
        },
        {
            "title": "Номер 1",
            "type": "обычный",
            "kv": False,
            "actors": [{"name": "Исаев", "tags": []}],
        },
        {
            "title": "Номер 2",
            "type": "обычный",
            "kv": False,
            "actors": [{"name": "Исаев", "tags": ["gk"]}],
        },
        {
            "title": "Спонсоры",
            "type": "спонсоры",
            "kv": False,
            "actors": [{"name": "Пушкин", "tags": []}],
        },
    ]

    variants, tcount = generate_program_variants(example)
    print(f"Вариантов: {len(variants)}, тянучек: {tcount}")
