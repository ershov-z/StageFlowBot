# utils/validator.py
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


def _has_actor(item, actor_name):
    for a in item.get("actors", []) or []:
        if a.get("name") == actor_name:
            return True
    return False


def _actor_tags(item, actor_name):
    for a in item.get("actors", []) or []:
        if a.get("name") == actor_name:
            return set(a.get("tags") or [])
    return set()


def _has_gk(item, actor_name):
    return "gk" in _actor_tags(item, actor_name)


def _is_kv(item):
    return bool(item.get("kv"))


# ============================================================
# ⚔️ Конфликты и «запрещённые» соседства
# ============================================================

def _adjacent_conflict(left, right):
    """
    Возвращает 1 если между left и right есть обычный конфликт, иначе 0.

    Обычный конфликт:
      - считаем только между ПОЛНЫМИ НОМЕРАМИ (тянучки не конфликтуют);
      - если есть общий актёр и у него НЕТ 'gk' ни слева, ни справа → конфликт = 1.
    """
    if left is None or right is None:
        return 0
    if not _is_number(left) or not _is_number(right):
        return 0

    left_names = {a["name"] for a in (left.get("actors") or [])}
    right_names = {a["name"] for a in (right.get("actors") or [])}
    shared = left_names & right_names
    for actor in shared:
        if not _has_gk(left, actor) and not _has_gk(right, actor):
            return 1
    return 0


def _adjacency_forbidden(left, right):
    """
    Жёсткое запрещение соседства (НЕ устраняется тянучкой):
      1) KV подряд: два полных номера с kv == True не могут быть рядом.
      2) 'gk'-разрыв: если общий актёр встречается в обоих соседних ПОЛНЫХ номерах
         и у него есть тег 'gk' хотя бы в одном из них — такие номера НЕ могут быть соседями,
         требуется ПОЛНЫЙ НОМЕР между ними (тянучка не считается перерывом).
    """
    if left is None or right is None:
        return False
    if not _is_number(left) or not _is_number(right):
        # Жёсткие запреты применяются только к паре "номер↔номер".
        return False

    # KV подряд запрещено
    if _is_kv(left) and _is_kv(right):
        return True

    # gk-разрыв запрет на соседство
    left_names = {a["name"] for a in (left.get("actors") or [])}
    right_names = {a["name"] for a in (right.get("actors") or [])}
    shared = left_names & right_names
    for actor in shared:
        if "gk" in _actor_tags(left, actor) or "gk" in _actor_tags(right, actor):
            return True

    return False


def _count_conflicts(program):
    """Подсчёт обычных конфликтов по всей программе."""
    conflicts = 0
    for i in range(len(program) - 1):
        conflicts += _adjacent_conflict(program[i], program[i + 1])
    return conflicts


# ============================================================
# 🧱 Фиксированные элементы
# ============================================================

def _compute_fixed_indices(program):
    """
    Фиксируем:
      - индекс 0 (пролог/предкулисье),
      - индексы 1 и 2,
      - индексы len-2 и len-1,
      - все элементы с type == "спонсоры".

    Примечание: если между 1-м и 2-м или между предпоследним и последним есть тянучка,
    она автоматически попадает в фиксируемые позиции (индекс 2 / len-2) и не трогается.
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
# 🔁 Backtracking c отсечением, рандомизацией и новым стопом
# ============================================================

def _search_best_variants(
    program,
    max_results=5,
    max_checked=None,
    max_conflicts_allowed=3,
    randomize=True,
    rand_seed=None,
):
    """
    Поиск выполняется поэлементным backtracking:
      - считаем конфликты инкрементально (только по левому соседу);
      - сразу отбрасываем запрещённые соседства (KV подряд, gk-разрыв);
      - варианты с > max_conflicts_allowed конфликтов отбрасываются.

    Ранний стоп:
      - если найден вариант с 0 конфликтов → немедленно завершаем поиск;
      - если найдены варианты с 1–2 конфликтами, ПОИСК ПРОДОЛЖАЕТСЯ (вдруг найдём 0),
        пока не переберём оставшиеся допустимые ветки (или не упремся в max_checked).
      - >3 конфликтов не рассматриваем.

    Рандомизация:
      - порядок перебора подвижных элементов и конкретных веток случайный,
        чтобы на одинаковом входе решения могли отличаться между запусками.
    """
    if rand_seed is not None:
        random.seed(rand_seed)

    n = len(program)
    fixed_indices, movable_indices = _compute_fixed_indices(program)
    movable_elems = [program[i] for i in movable_indices]

    # Лёгкая рандомизация набора элементов
    if randomize:
        random.shuffle(movable_elems)

    best_heap = []              # [(conflicts, variant)]
    checked_variants = 0
    best_conflicts_seen = float("inf")
    found_zero = False

    current = [None] * n
    for i in fixed_indices:
        current[i] = program[i]
    used = [False] * len(movable_elems)

    def backtrack(pos, current_conflicts):
        nonlocal checked_variants, best_conflicts_seen, found_zero

        # Отсечения по лимиту конфликтов
        if current_conflicts > max_conflicts_allowed:
            return

        # Если уже нашли идеал (0), можно не продолжать
        if found_zero:
            return

        # Пропуск фиксированных позиций
        while pos < n and current[pos] is not None:
            # Проверка запрещённого соседства "предыдущий↔текущий" уже произошла при постановке предыдущего.
            pos += 1

        if pos >= n:
            # Сформировали полный вариант
            checked_variants += 1
            if current_conflicts <= best_conflicts_seen:
                variant_copy = copy.deepcopy(current)
                best_heap.append((current_conflicts, variant_copy))
                best_heap.sort(key=lambda x: x[0])
                if len(best_heap) > max_results:
                    best_heap[:] = best_heap[:max_results]
                best_conflicts_seen = best_heap[0][0]

            if current_conflicts == 0:
                found_zero = True
            return

        # Сосед слева (для локальных проверок)
        left = current[pos - 1] if pos - 1 >= 0 else None

        # Индексы неиспользованных элементов; рандомизируем порядок перебора
        choices = [i for i, u in enumerate(used) if not u]
        if randomize:
            random.shuffle(choices)

        for idx in choices:
            elem = movable_elems[idx]

            # Проверки «на лету» до постановки:
            # 1) Запрещённое соседство с левым элементом
            if left is not None:
                if _adjacency_forbidden(left, elem):
                    continue

            # Поставим и посчитаем локальный вклад в конфликты (только если оба — номера)
            added = 0
            if left is not None:
                added = _adjacent_conflict(left, elem)
            new_conflicts = current_conflicts + added

            # Отсечения по лучшему найденному: если уже хуже — смысла нет (конфликты не уменьшаются)
            if new_conflicts > min(best_conflicts_seen, max_conflicts_allowed):
                continue

            # Принять решение и углубиться
            current[pos] = elem
            used[idx] = True
            backtrack(pos + 1, new_conflicts)
            used[idx] = False
            current[pos] = None

            if found_zero:
                return  # можно закончить совсем рано

    backtrack(0, 0)

    best_heap.sort(key=lambda x: x[0])
    return best_heap[:max_results], checked_variants


# ============================================================
# ➕ Вставка тянучек (максимум 3) с правилами ведущих
# ============================================================

def _can_actor_host_tyan(left, right, actor):
    """
    Ограничения на ведущего тянучки между left и right:
      - актёр НЕ может вести тянучку, если он участвует в СЛЕДУЮЩЕМ номере right;
      - если в следующем (right) у этого актёра есть 'gk' — тем более нельзя;
      - если актёр выходит ПОЗЖЕ (не в next) — может (тег 'later' трактуем как информационный).
    """
    # Нельзя, если актёр идёт в следующем номере
    if _has_actor(right, actor):
        # Явно запрещено, а также запрещено, если в следующем у него 'gk'
        if "gk" in _actor_tags(right, actor):
            return False
        return False

    # В следующем не участвует → можно
    return True


def _insert_tyanuchki(program, max_tyanuchki=3):
    """
    Добавляет тянучки в места обычных конфликтов (НЕ заменяет жёсткие запреты KV/gk),
    соблюдая ограничения на ведущих и общий лимит.
    """
    tcount = 0
    actors_priority = ["Пушкин", "Исаев", "Рожков"]

    i = 0
    while i < len(program) - 1:
        if tcount >= max_tyanuchki:
            break

        left = program[i]
        right = program[i + 1]

        # Несдвигаемые тянучки сохраняем как есть (их мы не трогаем и не удаляем)
        # Здесь мы ничего с ними не делаем; они зафиксированы ранее на уровне индексов.

        # Тянучки не конфликтуют — интересны только пары "номер↔номер"
        if not (_is_number(left) and _is_number(right)):
            i += 1
            continue

        # Обычный конфликт?
        if _adjacent_conflict(left, right) == 1:
            left_names = {a["name"] for a in (left.get("actors") or [])}
            right_names = {a["name"] for a in (right.get("actors") or [])}
            shared = left_names & right_names

            # Выбираем ведущего только из реально общих актёров
            for actor in actors_priority:
                if actor not in shared:
                    continue
                if not _can_actor_host_tyan(left, right, actor):
                    continue

                # Если прошло — вставляем тянучку
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
                break  # вставили одну — идём дальше

        i += 1

    return program, tcount


# ============================================================
# 🎯 Основная функция генерации
# ============================================================

def generate_program_variants(
    program,
    top_n=5,
    max_checked_variants=None,
    randomize=True,
    rand_seed=None,
):
    """
    Оптимизированная генерация вариантов:
      - backtracking с отсечением, жёсткими ограничениями KV/gk и рандомизацией;
      - стоп ТОЛЬКО при найденном варианте с 0 конфликтов;
      - собирает top_n лучших (по числу конфликтов ≤3);
      - добавляет максимум три тянучки (не нарушая правил ведущих).
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
        program,
        max_results=top_n,
        max_checked=max_checked_variants,
        max_conflicts_allowed=3,
        randomize=randomize,
        rand_seed=rand_seed,
    )

    if not top_results:
        logger.warning("⚠️ Не найдено ни одного допустимого варианта (≤3 конфликтов или нарушены жёсткие правила).")
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

    # Берём лучший найденный вариант
    best_conflicts, best_program = top_results[0]
    best_program = copy.deepcopy(best_program)

    # Вставляем тянучки (максимум 3) только для обычных конфликтов
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
