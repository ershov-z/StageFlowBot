from __future__ import annotations
import asyncio
import random
from typing import List, Optional, Tuple
from copy import deepcopy
import gc
from math import inf

from core.types import Block, Arrangement, Actor
from core.conflicts import strong_conflict, weak_conflict, kv_conflict
from core.fillers import pick_filler_actor
from service.hash_utils import arrangement_hash
from service.timing import measure_time
from service.logger import get_logger

log = get_logger("stageflow.optimizer")

MAX_FILLERS_TOTAL = 3
MAX_VARIANTS = 5
MAX_TRIES = 10000


# ============================================================
# Вспомогательные функции
# ============================================================

def _copy_block(block: Block) -> Block:
    return Block(
        id=block.id,
        name=block.name,
        type=block.type,
        actors=[Actor(a.name, list(a.tags)) for a in block.actors],
        kv=block.kv,
        fixed=block.fixed,
        meta=deepcopy(block.meta) if block.meta else None,
        num=block.num,
        actors_raw=block.actors_raw,
        pp_raw=block.pp_raw,
        hire=block.hire,
        responsible=block.responsible,
    )


def _make_filler(prev: Block, nxt: Block, actor_name: str, next_id: int) -> Block:
    actor = Actor(actor_name)
    return Block(
        id=next_id,
        name=f"[filler] {actor_name}",
        type="filler",
        actors=[actor],
        kv=False,
        fixed=False,
        num="",
        actors_raw=actor_name,
        pp_raw="",
        hire="",
        responsible="",
        meta={"auto": True, "between": (prev.name, nxt.name)},
    )


def _count_weak_conflicts(blocks: List[Block]) -> int:
    count = 0
    for i in range(len(blocks) - 1):
        a, b = blocks[i], blocks[i + 1]
        if a.type == "performance" and b.type == "performance":
            if weak_conflict(a, b) or weak_conflict(b, a):
                count += 1
    return count


def _has_strong_conflicts(blocks: List[Block]) -> bool:
    for i in range(len(blocks) - 1):
        a, b = blocks[i], blocks[i + 1]
        if a.type == "performance" and b.type == "performance":
            if strong_conflict(a, b) or strong_conflict(b, a) or kv_conflict(a, b):
                return True
    return False


def _insert_fillers(blocks: List[Block], max_fillers: int, seed: int) -> List[Block]:
    rng = random.Random(seed)
    result: List[Block] = []
    next_id = max((b.id for b in blocks), default=0) + 1
    fillers_used = 0

    for i, b in enumerate(blocks):
        if result:
            prev = result[-1]
            if prev.type == "performance" and b.type == "performance":
                if weak_conflict(prev, b):
                    if fillers_used < max_fillers:
                        actor_name = pick_filler_actor(prev, b, seed=seed ^ (i << 10))
                        if actor_name:
                            filler = _make_filler(prev, b, actor_name, next_id)
                            next_id += 1
                            result.append(filler)
                            fillers_used += 1
                        else:
                            log.warning(f"⚠️ Не найден актёр для тянучки между '{prev.name}' и '{b.name}'")
                    else:
                        log.debug("🚫 Достигнут лимит тянучек (%d)", max_fillers)
        result.append(b)
    return result


# ============================================================
# Теоретическая проверка и построение «идеального» варианта (DP)
# ============================================================

def _edge_cost(a: Block, b: Block) -> int:
    """
    Стоимость ребра a→b:
      ∞ — запрещено (сильный конфликт или kv:true подряд),
       1 — слабый конфликт,
       0 — нет конфликта.
    Неформанс-блоки считаем безопасными границами (стоимость 0).
    """
    if a.type == "performance" and b.type == "performance":
        if strong_conflict(a, b) or strong_conflict(b, a) or kv_conflict(a, b):
            return inf
        return 1 if (weak_conflict(a, b) or weak_conflict(b, a)) else 0
    return 0


def _segment_min_path(movable: List[Block], L: Block, R: Block) -> Tuple[bool, int, List[Block]]:
    """
    DP по подмножествам (Held-Karp для пути):
    находит минимальную «слабую стоимость» и сам порядок для сегмента между якорями L и R.
    Возвращает (feasible, min_cost, ordered_blocks).
    """
    n = len(movable)
    if n == 0:
        return True, 0, []

    # Быстрая необходимая проверка по kv: kv:true подряд недопустимо.
    kv_cnt = sum(1 for b in movable if b.kv)
    nonkv_cnt = n - kv_cnt
    if kv_cnt > nonkv_cnt + 1:
        return False, 999, []

    # Предрасчёт стоимостей
    w = [[inf] * n for _ in range(n)]
    cost_L = [inf] * n
    cost_R = [inf] * n
    for j in range(n):
        cost_L[j] = _edge_cost(L, movable[j])
        cost_R[j] = _edge_cost(movable[j], R)
    for i in range(n):
        for j in range(n):
            if i != j:
                w[i][j] = _edge_cost(movable[i], movable[j])

    size = 1 << n
    DP = [[inf] * n for _ in range(size)]
    PREV = [[-1] * n for _ in range(size)]

    # старт от левого якоря
    for j in range(n):
        if cost_L[j] < inf:
            DP[1 << j][j] = cost_L[j]
            PREV[1 << j][j] = -2  # маркер старта

    for mask in range(size):
        for j in range(n):
            if not (mask & (1 << j)):
                continue
            cur = DP[mask][j]
            if cur == inf:
                continue
            rest = (~mask) & (size - 1)
            k = rest
            while k:
                lsb = k & -k
                i = (lsb.bit_length() - 1)
                k ^= lsb
                if w[j][i] == inf:
                    continue
                nm = mask | (1 << i)
                val = cur + w[j][i]
                if val < DP[nm][i]:
                    DP[nm][i] = val
                    PREV[nm][i] = j

    full = size - 1
    best_cost = inf
    best_end = -1
    for j in range(n):
        if DP[full][j] < inf and cost_R[j] < inf:
            val = DP[full][j] + cost_R[j]
            if val < best_cost:
                best_cost = val
                best_end = j

    if best_cost == inf:
        return False, 999, []

    # Восстановление пути
    order_idx: List[int] = []
    mask = full
    j = best_end
    while j != -2 and j != -1:
        order_idx.append(j)
        pj = PREV[mask][j]
        if pj == -2:
            break
        mask ^= (1 << j)
        j = pj
    order_idx.reverse()

    ordered = [movable[i] for i in order_idx]
    return True, int(best_cost), ordered


def _build_ideal_order(blocks: List[Block]) -> Tuple[bool, int, List[Block]]:
    """
    Режем последовательность на сегменты между «якорями» (фикс-блоками),
    внутри каждого сегмента переставляем только movable performance-блоки,
    минимизируя слабые конфликты при запрете сильных.
    Возвращает (feasible, total_min_weak, new_order_without_fillers).
    """
    # Базовая копия и правила фиксации (как в стохастике)
    base_seq: List[Block] = [_copy_block(b) for b in blocks]
    for b in base_seq:
        if b.type in {"prelude", "sponsor"} or b.type == "filler":
            b.fixed = True
    perf_indices = [i for i, b in enumerate(base_seq) if b.type == "performance"]
    for i in perf_indices[:2]:
        base_seq[i].fixed = True
    for i in perf_indices[-4:]:
        base_seq[i].fixed = True

    # Список индексов фиксированных блоков — якоря
    anchors = [i for i, b in enumerate(base_seq) if b.fixed]
    if not anchors or anchors[0] != 0:
        # гарантируем, что есть левый якорь в начале
        anchors = sorted(set(anchors + [0]))
    if anchors[-1] != len(base_seq) - 1:
        # и правый якорь в конце
        anchors = sorted(set(anchors + [len(base_seq) - 1]))

    total_min_weak = 0
    new_order: List[Block] = []
    for s in range(len(anchors) - 1):
        left_i, right_i = anchors[s], anchors[s + 1]
        L, R = base_seq[left_i], base_seq[right_i]
        # Собираем movable performance в промежутке (исключая любые fixed/не performance)
        segment_movable = [
            base_seq[k] for k in range(left_i + 1, right_i)
            if base_seq[k].type == "performance" and not base_seq[k].fixed
        ]

        # Добавляем левый якорь (один раз на сегмент)
        if s == 0:
            new_order.append(L)

        feasible_seg, minw, ordered_seg = _segment_min_path(segment_movable, L, R)
        if not feasible_seg:
            return False, 999, blocks  # невозможен идеал

        total_min_weak += minw
        new_order.extend(ordered_seg)
        new_order.append(R)

    # Соединённый порядок получен без тянучек
    return True, total_min_weak, new_order


def theoretical_feasibility_exact(blocks: List[Block], max_fillers_total: int) -> dict:
    """
    DP-оценка и построение: есть ли порядок БЕЗ сильных конфликтов с минимальным числом слабых.
    Возвращает словарь с полями:
      feasible, min_weak_needed, available_fillers, strong_possible, order (List[Block])
    """
    # Сколько тянучек уже есть во входе
    existing_fillers = sum(1 for b in blocks if b.type == "filler")
    available = max(0, max_fillers_total - existing_fillers)

    feasible, min_weak_needed, ideal_order = _build_ideal_order(blocks)

    return {
        "feasible": feasible and (min_weak_needed <= available),
        "min_weak_needed": int(min_weak_needed if feasible else 999),
        "available_fillers": int(available),
        "strong_possible": bool(feasible),
        "order": ideal_order if feasible else blocks,
    }


@measure_time("optimizer.theoretical_check")
async def theoretical_check(blocks: List[Block]) -> Arrangement:
    """
    Возвращает готовый «математически идеальный» вариант:
      • перестановка без сильных конфликтов, с минимальным числом слабых;
      • вставленные тянучки (до лимита);
      • meta.status = "ideal".
    Если теоретически невозможно уложиться в лимит тянучек — meta.status = "infeasible".
    """
    existing_fillers = sum(1 for b in blocks if b.type == "filler")
    feasibility = theoretical_feasibility_exact(blocks, MAX_FILLERS_TOTAL)

    if not feasibility["feasible"]:
        log.error(
            f"❌ Теоретически неразрешимо: нужно {feasibility['min_weak_needed']} тянучек, "
            f"а доступно только {feasibility['available_fillers']}."
        )
        return Arrangement(
            seed=0,
            blocks=blocks,
            fillers_used=existing_fillers,
            strong_conflicts=0,
            weak_conflicts=0,
            meta={
                "status": "infeasible",
                "message": (
                    f"Эту программу невозможно разрешить: "
                    f"нужно минимум {feasibility['min_weak_needed']} тянучек, "
                    f"а доступно {feasibility['available_fillers']}."
                ),
                "min_weak_needed": feasibility["min_weak_needed"],
                "available_fillers": feasibility["available_fillers"],
            },
        )

    # Есть теоретический идеал: берём найденный порядок, вставляем тянучки по лимиту
    base_order: List[Block] = feasibility["order"]
    allowed_fillers = max(0, MAX_FILLERS_TOTAL - existing_fillers)
    with_fillers = _insert_fillers(base_order, allowed_fillers, seed=0)

    strong_cnt = sum(
        strong_conflict(with_fillers[i], with_fillers[i + 1])
        or kv_conflict(with_fillers[i], with_fillers[i + 1])
        for i in range(len(with_fillers) - 1)
        if with_fillers[i].type == "performance" and with_fillers[i + 1].type == "performance"
    )
    weak_cnt_final = sum(
        weak_conflict(with_fillers[i], with_fillers[i + 1])
        for i in range(len(with_fillers) - 1)
        if with_fillers[i].type == "performance" and with_fillers[i + 1].type == "performance"
    )

    log.info(
        f"🌟 Математически идеальный вариант построен: "
        f"вставлено тянучек={len(with_fillers) - len(base_order)} | "
        f"сильных={strong_cnt} | слабых={weak_cnt_final}"
    )

    return Arrangement(
        seed=0,
        blocks=with_fillers,
        fillers_used=(len(with_fillers) - len(base_order)),
        strong_conflicts=strong_cnt,
        weak_conflicts=weak_cnt_final,
        meta={"status": "ideal"},
    )


# ============================================================
# Основной алгоритм стохастического перебора
# ============================================================

@measure_time("optimizer.stochastic_branch_and_bound")
async def stochastic_branch_and_bound(blocks: List[Block], seed: int) -> Arrangement:
    rng = random.Random(seed)
    log.info("🧮 Оптимайзер запущен (seed=%s)", seed)

    existing_fillers = sum(1 for b in blocks if b.type == "filler")
    max_weak_allowed = max(0, MAX_FILLERS_TOTAL - existing_fillers)
    log.info(f"[SEED={seed}] исходных тянучек={existing_fillers}, допустимо слабых конфликтов={max_weak_allowed}")

    # === Фиксированные блоки ===
    base_seq: List[Block] = [_copy_block(b) for b in blocks]
    for b in base_seq:
        if b.type in {"prelude", "sponsor"} or b.type == "filler":
            b.fixed = True
    perf_indices = [i for i, b in enumerate(base_seq) if b.type == "performance"]
    for i in perf_indices[:2]:
        base_seq[i].fixed = True
    for i in perf_indices[-4:]:
        base_seq[i].fixed = True

    fixed_blocks = [b for b in base_seq if b.fixed]
    movable_blocks = [b for b in base_seq if (b.type == "performance" and not b.fixed)]

    if not movable_blocks:
        log.warning(f"⚠️ Все блоки фиксированы (seed={seed}), перестановка невозможна.")
        return Arrangement(seed=seed, blocks=blocks, fillers_used=existing_fillers)

    best_variant: Optional[List[Block]] = None
    best_weak = 999
    tries = 0

    for attempt in range(1, MAX_TRIES + 1):
        tries = attempt
        shuffled = movable_blocks[:]
        rng.shuffle(shuffled)

        new_order: List[Block] = []
        m_idx = 0
        for b in base_seq:
            if b.fixed:
                new_order.append(b)
            else:
                new_order.append(shuffled[m_idx])
                m_idx += 1

        if _has_strong_conflicts(new_order):
            continue
        weak_cnt = _count_weak_conflicts(new_order)
        if weak_cnt <= max_weak_allowed:
            if weak_cnt == 0:
                best_variant = new_order
                best_weak = 0
                log.info(f"✅ Найден идеальный вариант без слабых конфликтов (seed={seed}, attempt={attempt})")
                break
            if weak_cnt < best_weak:
                best_variant = new_order
                best_weak = weak_cnt

    if not best_variant:
        log.error(f"❌ Оптимайзер не смог найти допустимую перестановку (seed={seed}) после {MAX_TRIES} попыток.")
        return Arrangement(seed=seed, blocks=blocks, fillers_used=existing_fillers)

    log.info(f"✅ Найден вариант после {tries} попыток (weak={best_weak}, seed={seed})")

    allowed_fillers = max(0, MAX_FILLERS_TOTAL - existing_fillers)
    with_fillers = _insert_fillers(best_variant, allowed_fillers, seed)

    strong_cnt = sum(
        strong_conflict(with_fillers[i], with_fillers[i + 1]) or kv_conflict(with_fillers[i], with_fillers[i + 1])
        for i in range(len(with_fillers) - 1)
        if with_fillers[i].type == "performance" and with_fillers[i + 1].type == "performance"
    )
    weak_cnt_final = sum(
        weak_conflict(with_fillers[i], with_fillers[i + 1])
        for i in range(len(with_fillers) - 1)
        if with_fillers[i].type == "performance" and with_fillers[i + 1].type == "performance"
    )

    log.info(
        f"🎬 Итог: вставлено тянучек={len(with_fillers) - len(best_variant)} | "
        f"сильных={strong_cnt} | слабых={weak_cnt_final}"
    )

    return Arrangement(
        seed=seed,
        blocks=with_fillers,
        fillers_used=(len(with_fillers) - len(best_variant)),
        strong_conflicts=strong_cnt,
        weak_conflicts=weak_cnt_final,
    )


# ============================================================
# Генерация нескольких вариантов
# ============================================================

@measure_time("optimizer.generate_arrangements")
async def generate_arrangements(blocks: List[Block], n_variants: int = MAX_VARIANTS) -> List[Arrangement]:
    # 1️⃣ Теоретический идеал (строим и сразу отдаём пользователю первым)
    ideal_arr = await theoretical_check(blocks)
    if ideal_arr.meta and ideal_arr.meta.get("status") == "infeasible":
        return [ideal_arr]

    if ideal_arr.meta and ideal_arr.meta.get("status") == "ideal":
        log.info("🌟 Отправляем математически идеальный вариант пользователю. Ищу альтернативы...")

    # 2️⃣ Генерация стохастических альтернатив (ровно n_variants штук поверх идеала)
    seeds = [random.randint(1000, 99999) for _ in range(n_variants)]
    log.info(f"🧬 Seeds: {seeds}")

    unique: List[Arrangement] = [ideal_arr]
    seen_hashes = {arrangement_hash(ideal_arr.blocks)}

    for s in seeds:
        arr = await stochastic_branch_and_bound(blocks, s)
        h = arrangement_hash(arr.blocks)
        if h not in seen_hashes:
            seen_hashes.add(h)
            unique.append(arr)
        await asyncio.sleep(0)
        gc.collect()

    log.info(f"✅ Сгенерировано уникальных вариантов (включая идеальный): {len(unique)} / {len(seeds) + 1}")
    return unique
