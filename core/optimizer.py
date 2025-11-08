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
    c = 0
    for i in range(len(blocks) - 1):
        a, b = blocks[i], blocks[i + 1]
        if a.type == b.type == "performance" and (weak_conflict(a, b) or weak_conflict(b, a)):
            c += 1
    return c


def _has_strong_conflicts(blocks: List[Block]) -> bool:
    for i in range(len(blocks) - 1):
        a, b = blocks[i], blocks[i + 1]
        if a.type == b.type == "performance":
            if strong_conflict(a, b) or kv_conflict(a, b):
                return True
    return False


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
                else:
                    log.debug("🚫 Лимит тянучек достигнут (%d)", max_fillers)
        res.append(b)
    return res


# ============================================================
# Теоретическая проверка (DP)
# ============================================================

def _edge_cost(a: Block, b: Block) -> int:
    if a.type == b.type == "performance":
        if strong_conflict(a, b) or kv_conflict(a, b):
            return inf
        return 1 if (weak_conflict(a, b) or weak_conflict(b, a)) else 0
    return 0


def _segment_min_path(movable: List[Block], L: Block, R: Block) -> Tuple[bool, int, List[Block]]:
    n = len(movable)
    if n == 0:
        return True, 0, []

    kv_cnt = sum(1 for b in movable if b.kv)
    if kv_cnt > n - kv_cnt + 1:
        return False, 999, []

    w = [[inf] * n for _ in range(n)]
    cost_L = [inf] * n
    cost_R = [inf] * n
    for j in range(n):
        cost_L[j] = _edge_cost(L, movable[j])
        cost_R[j] = _edge_cost(movable[j], R)
        for i in range(n):
            if i != j:
                w[i][j] = _edge_cost(movable[i], movable[j])

    size = 1 << n
    DP = [[inf] * n for _ in range(size)]
    PREV = [[-1] * n for _ in range(size)]

    for j in range(n):
        if cost_L[j] < inf:
            DP[1 << j][j] = cost_L[j]
            PREV[1 << j][j] = -2

    for mask in range(size):
        for j in range(n):
            if not (mask & (1 << j)) or DP[mask][j] == inf:
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
                val = DP[mask][j] + w[j][i]
                if val < DP[nm][i]:
                    DP[nm][i] = val
                    PREV[nm][i] = j

    full = size - 1
    best_cost, best_end = inf, -1
    for j in range(n):
        if DP[full][j] < inf and cost_R[j] < inf:
            val = DP[full][j] + cost_R[j]
            if val < best_cost:
                best_cost, best_end = val, j

    if best_cost == inf:
        return False, 999, []

    # восстановление
    order_idx: List[int] = []
    mask, j = full, best_end
    while j not in (-1, -2):
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
    base = [_copy_block(b) for b in blocks]
    for b in base:
        if b.type in {"prelude", "sponsor"} or b.type == "filler":
            b.fixed = True
    perf_idx = [i for i, b in enumerate(base) if b.type == "performance"]
    for i in perf_idx[:2] + perf_idx[-4:]:
        base[i].fixed = True

    anchors = [i for i, b in enumerate(base) if b.fixed]
    anchors = sorted(set(anchors + [0, len(base) - 1]))

    total_min_weak = 0
    new_order: List[Block] = []
    for s in range(len(anchors) - 1):
        L, R = base[anchors[s]], base[anchors[s + 1]]
        segment = [base[k] for k in range(anchors[s] + 1, anchors[s + 1])
                   if base[k].type == "performance" and not base[k].fixed]

        if s == 0:
            new_order.append(L)

        feasible_seg, minw, ordered = _segment_min_path(segment, L, R)
        if not feasible_seg:
            return False, 999, blocks
        total_min_weak += minw
        new_order.extend(ordered)
        new_order.append(R)

    return True, total_min_weak, new_order


def theoretical_feasibility_exact(blocks: List[Block], max_fillers_total: int) -> dict:
    existing = sum(1 for b in blocks if b.type == "filler")
    available = max(0, max_fillers_total - existing)
    feasible, min_weak, ideal_order = _build_ideal_order(blocks)

    # ✅ главное изменение — разрешаем равенство
    can_fit = feasible and min_weak <= available
    return {
        "feasible": can_fit,
        "min_weak_needed": int(min_weak if feasible else 999),
        "available_fillers": int(available),
        "strong_possible": feasible,
        "order": ideal_order if feasible else blocks,
    }


@measure_time("optimizer.theoretical_check")
async def theoretical_check(blocks: List[Block]) -> Arrangement:
    existing = sum(1 for b in blocks if b.type == "filler")
    feas = theoretical_feasibility_exact(blocks, MAX_FILLERS_TOTAL)

    if not feas["feasible"]:
        log.error(f"❌ Теоретически неразрешимо: нужно {feas['min_weak_needed']} тянучек, а доступно {feas['available_fillers']}.")
        return Arrangement(
            seed=0,
            blocks=blocks,
            fillers_used=existing,
            strong_conflicts=0,
            weak_conflicts=0,
            meta={
                "status": "infeasible",
                "message": (
                    f"Эту программу невозможно разрешить: "
                    f"нужно минимум {feas['min_weak_needed']} тянучек, "
                    f"а доступно {feas['available_fillers']}."
                ),
                "min_weak_needed": feas["min_weak_needed"],
                "available_fillers": feas["available_fillers"],
            },
        )

    base_order = feas["order"]
    allowed = max(0, MAX_FILLERS_TOTAL - existing)
    with_fillers = _insert_fillers(base_order, allowed, seed=0)

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

    log.info(f"🌟 Идеальный вариант построен: тянучек={len(with_fillers) - len(base_order)} | сильных={strong_cnt} | слабых={weak_cnt}")
    return Arrangement(
        seed=0,
        blocks=with_fillers,
        fillers_used=(len(with_fillers) - len(base_order)),
        strong_conflicts=strong_cnt,
        weak_conflicts=weak_cnt,
        meta={"status": "ideal"},
    )


# ============================================================
# Стохастический перебор и генерация вариантов
# ============================================================

@measure_time("optimizer.stochastic_branch_and_bound")
async def stochastic_branch_and_bound(blocks: List[Block], seed: int) -> Arrangement:
    rng = random.Random(seed)
    existing = sum(1 for b in blocks if b.type == "filler")
    max_weak = max(0, MAX_FILLERS_TOTAL - existing)
    log.info(f"🧮 Оптимайзер запущен (seed={seed}) | исходных тянучек={existing}, слабых≤{max_weak}")

    base = [_copy_block(b) for b in blocks]
    for b in base:
        if b.type in {"prelude", "sponsor"} or b.type == "filler":
            b.fixed = True
    perf_idx = [i for i, b in enumerate(base) if b.type == "performance"]
    for i in perf_idx[:2] + perf_idx[-4:]:
        base[i].fixed = True

    movable = [b for b in base if b.type == "performance" and not b.fixed]
    if not movable:
        log.warning(f"⚠️ Все блоки фиксированы (seed={seed})")
        return Arrangement(seed=seed, blocks=blocks, fillers_used=existing)

    best, best_weak = None, 999
    for attempt in range(1, MAX_TRIES + 1):
        shuf = movable[:]
        rng.shuffle(shuf)
        new_order = []
        m_idx = 0
        for b in base:
            if b.fixed:
                new_order.append(b)
            else:
                new_order.append(shuf[m_idx])
                m_idx += 1
        if _has_strong_conflicts(new_order):
            continue
        w = _count_weak_conflicts(new_order)
        if w <= max_weak:
            best, best_weak = new_order, w
            if w == 0:
                break

    if not best:
        log.error(f"❌ Не найдено допустимых перестановок (seed={seed})")
        return Arrangement(seed=seed, blocks=blocks, fillers_used=existing)

    with_fillers = _insert_fillers(best, max(0, MAX_FILLERS_TOTAL - existing), seed)
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

    log.info(f"✅ Найден вариант (seed={seed}) | слабых={weak_cnt} | сильных={strong_cnt}")
    return Arrangement(
        seed=seed,
        blocks=with_fillers,
        fillers_used=len(with_fillers) - len(best),
        strong_conflicts=strong_cnt,
        weak_conflicts=weak_cnt,
    )


@measure_time("optimizer.generate_arrangements")
async def generate_arrangements(blocks: List[Block], n_variants: int = MAX_VARIANTS) -> List[Arrangement]:
    ideal = await theoretical_check(blocks)
    if ideal.meta.get("status") == "infeasible":
        return [ideal]
    log.info("🌟 Отправляю идеальный вариант пользователю, затем ищу альтернативы...")

    seeds = [random.randint(1000, 99999) for _ in range(n_variants)]
    uniq = [ideal]
    seen = {arrangement_hash(ideal.blocks)}
    for s in seeds:
        arr = await stochastic_branch_and_bound(blocks, s)
        h = arrangement_hash(arr.blocks)
        if h not in seen:
            uniq.append(arr)
            seen.add(h)
        await asyncio.sleep(0)
        gc.collect()

    log.info(f"✅ Сгенерировано вариантов (включая идеальный): {len(uniq)}")
    return uniq
