from __future__ import annotations
import asyncio
import random
from typing import List, Tuple
from copy import deepcopy
import gc
from itertools import permutations

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
MAX_OPT_ORDERS_PER_SEGMENT = 32


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


def _edge_cost(a: Block, b: Block) -> int:
    if a.type == b.type == "performance":
        if strong_conflict(a, b) or kv_conflict(a, b):
            return float("inf")
        return 1 if (weak_conflict(a, b) or weak_conflict(b, a)) else 0
    return 0


def _segment_cost_with_bounds(order: List[Block], L: Block, R: Block) -> int:
    """Стоимость L -> order -> R: число слабых конфликтов; inf если где-то сильный."""
    if not order:
        return _edge_cost(L, R)
    total = _edge_cost(L, order[0])
    if total == float("inf"):
        return total
    for i in range(len(order) - 1):
        c = _edge_cost(order[i], order[i + 1])
        if c == float("inf"):
            return c
        total += c
    end_cost = _edge_cost(order[-1], R)
    if end_cost == float("inf"):
        return end_cost
    return total


# ============================================================
# Новый адаптивный поиск оптимальных перестановок
# ============================================================

def _segment_optimal_orders(movable: List[Block], L: Block, R: Block, seed: int) -> Tuple[bool, int, List[List[Block]]]:
    n = len(movable)
    if n == 0:
        return True, 0, [[]]

    rng = random.Random(seed)
    if n <= 7:
        sample_limit = None
    elif n <= 10:
        sample_limit = 512
    else:
        sample_limit = 256

    best_cost = float("inf")
    best_orders: List[List[Block]] = []

    perms = permutations(movable, n) if sample_limit is None else (
        (rng.sample(movable, n) for _ in range(sample_limit))
    )

    checked = 0
    for perm in perms:
        checked += 1
        perm_list = list(perm)
        cost = _segment_cost_with_bounds(perm_list, L, R)
        if cost == float("inf"):
            continue
        if cost < best_cost:
            best_cost = cost
            best_orders = [perm_list]
        elif cost == best_cost and len(best_orders) < MAX_OPT_ORDERS_PER_SEGMENT:
            best_orders.append(perm_list)

    if not best_orders or best_cost == float("inf"):
        return False, 999, []

    log.debug(f"🧩 Сегмент {len(movable)} номеров, проверено {checked}, min_cost={best_cost}, вариантов={len(best_orders)}")
    return True, int(best_cost), best_orders


# ============================================================
# Основной алгоритм сборки идеального порядка
# ============================================================

def _build_ideal_order(blocks: List[Block], seed: int) -> Tuple[bool, int, List[Block]]:
    """Строит идеальный порядок с выбором случайного оптимума из множества."""
    rng = random.Random(seed)
    base = [_copy_block(b) for b in blocks]

    for b in base:
        if b.type in {"prelude", "sponsor"} or b.type == "filler":
            b.fixed = True

    perf_idx = [i for i, b in enumerate(base) if b.type == "performance"]
    if len(perf_idx) >= 6:
        for i in perf_idx[:2] + perf_idx[-4:]:
            base[i].fixed = True
    elif len(perf_idx) >= 2:
        for i in perf_idx[:2]:
            base[i].fixed = True

    anchors = [i for i, b in enumerate(base) if b.fixed]
    anchors = sorted(set(anchors + [0, len(base) - 1]))

    total_min_weak = 0
    new_order: List[Block] = []
    for s in range(len(anchors) - 1):
        L, R = base[anchors[s]], base[anchors[s + 1]]
        segment = [
            base[k] for k in range(anchors[s] + 1, anchors[s + 1])
            if base[k].type == "performance" and not base[k].fixed
        ]
        if s == 0:
            new_order.append(L)

        feasible_seg, minw, opt_orders = _segment_optimal_orders(segment, L, R, seed)
        if not feasible_seg:
            return False, 999, blocks
        total_min_weak += minw

        choice = rng.choice(opt_orders)
        new_order.extend(choice)
        new_order.append(R)

    return True, total_min_weak, new_order


# ============================================================
# Вставка тянучек
# ============================================================

def _insert_fillers(blocks: List[Block], max_fillers: int, seed: int) -> List[Block]:
    rng = random.Random(seed)
    res: List[Block] = []
    next_id = max((b.id for b in blocks), default=0) + 1
    used = 0
    for i, b in enumerate(blocks):
        if res:
            prev = res[-1]
            if prev.type == b.type == "performance" and weak_conflict(prev, b):
                if used < max_fillers:
                    actor_name = pick_filler_actor(prev, b, seed ^ (i << 8))
                    if actor_name:
                        filler = _make_filler(prev, b, actor_name, next_id)
                        next_id += 1
                        res.append(filler)
                        used += 1
                    else:
                        log.warning(f"⚠️ Нет актёра для тянучки между '{prev.name}' и '{b.name}'")
        res.append(b)
    return res


def _has_strong_conflicts(blocks: List[Block]) -> bool:
    for i in range(len(blocks) - 1):
        a, b = blocks[i], blocks[i + 1]
        if a.type == b.type == "performance":
            if strong_conflict(a, b) or kv_conflict(a, b):
                return True
    return False


# ============================================================
# Теоретическая проверка
# ============================================================

@measure_time("optimizer.theoretical_check")
async def theoretical_check(blocks: List[Block]) -> Arrangement:
    feasible, min_weak, order = _build_ideal_order(blocks, seed=0)
    if not feasible:
        log.error("❌ Теоретически неразрешимо (сильные конфликты)")
        return Arrangement(
            seed=0, blocks=blocks, fillers_used=0, meta={"status": "infeasible"}
        )

    allowed_new = MAX_FILLERS_TOTAL
    with_fillers = _insert_fillers(order, allowed_new, seed=0)

    strong_cnt = sum(
        (strong_conflict(with_fillers[i], with_fillers[i + 1]) or kv_conflict(with_fillers[i], with_fillers[i + 1]))
        for i in range(len(with_fillers) - 1)
        if with_fillers[i].type == with_fillers[i + 1].type == "performance"
    )
    weak_cnt = sum(
        weak_conflict(with_fillers[i], with_fillers[i + 1])
        for i in range(len(with_fillers) - 1)
        if with_fillers[i].type == with_fillers[i + 1].type == "performance"
    )

    log.info(f"🌟 Математический идеал: слабых={min_weak} → вставлено тянучек={len(with_fillers) - len(order)}")
    return Arrangement(
        seed=0,
        blocks=with_fillers,
        fillers_used=len(with_fillers) - len(order),
        strong_conflicts=strong_cnt,
        weak_conflicts=weak_cnt,
        meta={"status": "ideal"},
    )


# ============================================================
# Стохастика и генерация всех вариантов
# ============================================================

@measure_time("optimizer.generate_arrangements")
async def generate_arrangements(blocks: List[Block], n_variants: int = MAX_VARIANTS) -> List[Arrangement]:
    ideal0 = await theoretical_check(blocks)
    if str(ideal0.meta.get("status", "")).lower() == "infeasible":
        return [ideal0]

    results = [ideal0]
    seen = {arrangement_hash(ideal0.blocks)}

    # 🟢 Генерируем дополнительные идеальные варианты
    ideal_count = 0
    for _ in range(6):
        s = random.randint(1000, 99999)
        feasible, minw, order = _build_ideal_order(blocks, s)
        if feasible:
            with_fillers = _insert_fillers(order, MAX_FILLERS_TOTAL, seed=s)
            arr = Arrangement(
                seed=s,
                blocks=with_fillers,
                fillers_used=len(with_fillers) - len(order),
                strong_conflicts=0,
                weak_conflicts=0,
                meta={"status": "ideal"},
            )
            h = arrangement_hash(arr.blocks)
            if h not in seen:
                results.append(arr)
                seen.add(h)
                ideal_count += 1

    if ideal_count == 0:
        log.warning("⚠️ Обнаружен только один уникальный идеальный порядок — все перестановки идентичны.")

    # 🎲 Стохастические варианты
    for _ in range(n_variants):
        s = random.randint(1000, 99999)
        arr = await stochastic_branch_and_bound(blocks, s)
        h = arrangement_hash(arr.blocks)
        if h not in seen:
            results.append(arr)
            seen.add(h)
        await asyncio.sleep(0)
        gc.collect()

    ideal_generated = sum(
        1 for r in results if str(r.meta.get("status", "")).lower() == "ideal"
    )
    stochastic_generated = len(results) - ideal_generated
    log.info(
        "✅ Сгенерировано вариантов: идеальных=%s, стохастических=%s",
        ideal_generated,
        stochastic_generated,
    )
    return results
