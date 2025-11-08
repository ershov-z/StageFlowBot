from __future__ import annotations
import asyncio
import random
from typing import List, Optional
from copy import deepcopy
import gc
from collections import defaultdict

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
# Теоретическая проверка (новая логика)
# ============================================================

def theoretical_feasibility_exact(blocks: List[Block], max_fillers_total: int) -> dict:
    """Проверяет теоретическую возможность разрешения с учётом перестановки."""
    from itertools import permutations

    # Отбираем только переставляемые номера
    movable = [b for b in blocks if b.type == "performance"]
    existing_fillers = sum(1 for b in blocks if b.type == "filler")

    # Быстрая проверка: если нет вообще strong-конфликтов между любыми парами — 0 strong достижим
    # Ищем минимум weak-конфликтов в любой перестановке (эвристически для малых входов)
    best_weak = float("inf")
    strong_impossible = False
    all_blocks = movable[:]
    limit = 8  # для больших входов не взрываем память
    if len(all_blocks) <= limit:
        for perm in permutations(all_blocks):
            if _has_strong_conflicts(list(perm)):
                strong_impossible = True
                continue
            w = _count_weak_conflicts(list(perm))
            best_weak = min(best_weak, w)
    else:
        # случайный выбор 500 перестановок
        rng = random.Random(42)
        for _ in range(500):
            rng.shuffle(all_blocks)
            if _has_strong_conflicts(all_blocks):
                strong_impossible = True
                continue
            w = _count_weak_conflicts(all_blocks)
            best_weak = min(best_weak, w)

    available = max_fillers_total - existing_fillers
    feasible = best_weak <= available and not strong_impossible
    return {
        "feasible": feasible,
        "min_weak_needed": int(best_weak if best_weak != float("inf") else 999),
        "available_fillers": int(available),
        "strong_possible": not strong_impossible,
    }


@measure_time("optimizer.theoretical_check")
async def theoretical_check(blocks: List[Block]) -> Arrangement:
    """Возвращает математически идеальный вариант, если он возможен."""
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
            },
        )

    log.info(
        f"🌟 Математически идеальный вариант возможен (weak ≤ {feasibility['min_weak_needed']}, "
        f"доступно {feasibility['available_fillers']})."
    )

    return Arrangement(
        seed=0,
        blocks=blocks,
        fillers_used=existing_fillers,
        strong_conflicts=0,
        weak_conflicts=feasibility["min_weak_needed"],
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

    # Убрана прежняя теоретическая проверка — теперь делается отдельно

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

    strong_cnt = sum(strong_conflict(with_fillers[i], with_fillers[i + 1]) for i in range(len(with_fillers) - 1))
    weak_cnt_final = sum(weak_conflict(with_fillers[i], with_fillers[i + 1]) for i in range(len(with_fillers) - 1))

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
    # 1️⃣ Проверка математической разрешимости
    ideal_arr = await theoretical_check(blocks)
    if ideal_arr.meta["status"] == "infeasible":
        return [ideal_arr]

    if ideal_arr.meta["status"] == "ideal":
        log.info("🌟 Отправляем математически идеальный вариант пользователю. Ищу альтернативы...")

    # 2️⃣ Генерация стохастических альтернатив
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
