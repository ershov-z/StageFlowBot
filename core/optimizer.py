# core/optimizer.py
from __future__ import annotations
import asyncio
import random
import logging
from typing import List, Optional, Tuple
from copy import deepcopy
import gc

from core.types import Block, Arrangement, Actor
from core.conflicts import strong_conflict, weak_conflict, kv_conflict
from core.fillers import pick_filler_actor
from service.hash_utils import arrangement_hash, is_duplicate, register_hash
from service.timing import measure_time

log = logging.getLogger("stageflow.optimizer")

MAX_FILLERS_TOTAL = 3
MAX_VARIANTS = 5


# ============================================================
# 🧩 Вспомогательные функции
# ============================================================

def _copy_block(block: Block) -> Block:
    """Полное копирование блока (включая все raw-поля)."""
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
    """Создаёт новый filler-блок."""
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


def _needs_filler(prev_perf: Optional[Block], cand: Block) -> Tuple[bool, bool]:
    """
    Проверяет соседство двух performance-блоков.
    Возвращает (запретить, нужен_филлер).
    """
    if prev_perf is None or cand.type != "performance":
        return (False, False)
    if strong_conflict(prev_perf, cand) or kv_conflict(prev_perf, cand):
        return (True, False)
    if weak_conflict(prev_perf, cand):
        return (False, True)
    return (False, False)


def _last_performance(seq: List[Block]) -> Optional[Block]:
    for b in reversed(seq):
        if b.type == "performance":
            return b
    return None


# ============================================================
# ⚙️ Жадная оптимизация порядка перестановки
# ============================================================

def _conflict_score(a: Block, b: Block) -> int:
    """1, если слабый конфликт между a и b, иначе 0."""
    return 1 if weak_conflict(a, b) or weak_conflict(b, a) else 0


def _preorder_pool(pool: List[Block]) -> List[Block]:
    """
    Жадная предоптимизация порядка переставляемых номеров:
    выбирает порядок, минимизирующий количество слабых конфликтов подряд.
    """
    if len(pool) <= 1:
        return pool[:]

    remaining = pool[:]
    # старт — тот, у кого меньше всего конфликтов
    remaining.sort(key=lambda b: sum(_conflict_score(b, x) for x in remaining))
    order = [remaining.pop(0)]

    while remaining:
        last = order[-1]
        remaining.sort(key=lambda b: (_conflict_score(last, b),
                                      sum(_conflict_score(b, x) for x in remaining)))
        order.append(remaining.pop(0))
    return order


# ============================================================
# 🎛️ Основной стохастический алгоритм
# ============================================================

@measure_time("optimizer.stochastic_branch_and_bound")
async def stochastic_branch_and_bound(blocks: List[Block], seed: int) -> Arrangement:
    rng = random.Random(seed)
    seen_hashes: set[str] = set()

    # 1️⃣ Подсчёт уже существующих тянучек
    existing_fillers = sum(1 for b in blocks if b.type == "filler")
    allowed_to_insert = max(0, MAX_FILLERS_TOTAL - existing_fillers)
    log.info(f"[SEED={seed}] исходных тянучек={existing_fillers}, можно вставить ещё={allowed_to_insert}")

    # 2️⃣ Фиксация по правилам v2.4
    for b in blocks:
        if b.type in {"prelude", "sponsor"}:
            b.fixed = True
        if b.type == "filler":
            b.fixed = True  # уже существующие тянучки фиксируем

    perf_indices = [i for i, b in enumerate(blocks) if b.type == "performance"]
    for i in perf_indices[:2]:
        blocks[i].fixed = True
    for i in perf_indices[-4:]:
        blocks[i].fixed = True

    # 3️⃣ Рабочая копия
    base_seq: List[Block] = [_copy_block(b) for b in blocks]
    fixed_positions = {i for i, b in enumerate(base_seq) if b.fixed}
    fixed_at_index = {i: base_seq[i] for i in fixed_positions}
    variable_pool: List[Block] = [b for b in base_seq if (b.type == "performance" and not b.fixed)]

    # 4️⃣ Предоптимизация порядка переставляемых
    variable_pool = _preorder_pool(variable_pool)

    max_id = max((b.id for b in blocks), default=0)
    next_new_id = max_id + 1

    best_arrangement: Optional[List[Block]] = None
    best_fillers_used: int = 99
    found_perfect = False

    # ============================================================
    # 🌲 DFS (с отсечениями)
    # ============================================================
    def dfs(pos: int, pool: List[Block], assembled: List[Block], fillers_used: int) -> None:
        nonlocal best_arrangement, best_fillers_used, found_perfect, next_new_id

        if fillers_used > allowed_to_insert:
            return
        if fillers_used >= best_fillers_used or found_perfect:
            return

        if pos == len(base_seq):
            candidate = assembled.copy()
            h = arrangement_hash(candidate)
            if not is_duplicate(candidate, seen_hashes):
                register_hash(candidate, seen_hashes)
                best_arrangement = candidate
                best_fillers_used = fillers_used
                log.info(f"[RESULT] seed={seed} | вставлено_тянучек={fillers_used} | hash={h[:8]}")
                if best_fillers_used == 0:
                    found_perfect = True
            return

        # фиксированные позиции
        if pos in fixed_positions:
            cand = fixed_at_index[pos]
            prev_perf = _last_performance(assembled)
            forbid, need_fill = (False, False)
            if cand.type == "performance":
                forbid, need_fill = _needs_filler(prev_perf, cand)

            if forbid:
                return
            if need_fill and fillers_used < allowed_to_insert:
                actor_name = pick_filler_actor(prev_perf, cand, seed=seed ^ (pos << 8))
                if not actor_name:
                    return
                filler_block = _make_filler(prev_perf, cand, actor_name, next_new_id)
                next_new_id += 1
                assembled.append(filler_block)
                assembled.append(cand)
                dfs(pos + 1, pool, assembled, fillers_used + 1)
                assembled.pop()
                assembled.pop()
                return

            assembled.append(cand)
            dfs(pos + 1, pool, assembled, fillers_used)
            assembled.pop()
            return

        # переставляемые блоки
        try_order = pool.copy()
        rng.shuffle(try_order)

        for cand in try_order:
            prev_perf = _last_performance(assembled)
            forbid, need_fill = _needs_filler(prev_perf, cand)
            if forbid:
                continue

            if need_fill and fillers_used < allowed_to_insert:
                actor_name = pick_filler_actor(prev_perf, cand, seed=seed ^ (pos << 12))
                if not actor_name:
                    continue
                filler_block = _make_filler(prev_perf, cand, actor_name, next_new_id)
                next_new_id += 1
                assembled.append(filler_block)
                assembled.append(cand)
                new_pool = [b for b in pool if b is not cand]
                dfs(pos + 1, new_pool, assembled, fillers_used + 1)
                assembled.pop()
                assembled.pop()
            else:
                assembled.append(cand)
                new_pool = [b for b in pool if b is not cand]
                dfs(pos + 1, new_pool, assembled, fillers_used)
                assembled.pop()

            if found_perfect:
                return

    log.info(f"▶️ Start BnB (seed={seed}) | fixed={len(fixed_positions)} | variable={len(variable_pool)}")
    dfs(0, variable_pool, [], 0)

    # ============================================================
    # 🧾 Результат
    # ============================================================
    if best_arrangement is None:
        log.warning(f"⚠️ Не удалось собрать вариант для seed={seed}. Возвращаю исходный порядок.")
        return Arrangement(seed=seed, blocks=blocks, fillers_used=0)

    strong_cnt = sum(strong_conflict(best_arrangement[i], best_arrangement[i + 1])
                     for i in range(len(best_arrangement) - 1))
    weak_cnt = sum(weak_conflict(best_arrangement[i], best_arrangement[i + 1])
                   for i in range(len(best_arrangement) - 1))

    log.info(f"✅ Done (seed={seed}) | вставлено_тянучек={best_fillers_used} | total={len(best_arrangement)}")
    return Arrangement(
        seed=seed,
        blocks=best_arrangement,
        fillers_used=best_fillers_used,
        strong_conflicts=strong_cnt,
        weak_conflicts=weak_cnt,
    )


# ============================================================
# 🧮 Генерация нескольких вариантов
# ============================================================

@measure_time("optimizer.generate_arrangements")
async def generate_arrangements(blocks: List[Block], n_variants: int = MAX_VARIANTS) -> List[Arrangement]:
    """Создаёт до 5 уникальных вариантов."""
    seeds = [random.randint(1000, 99999) for _ in range(n_variants)]
    log.info(f"🧬 Seeds: {seeds}")

    unique: List[Arrangement] = []
    seen_hashes = set()

    for s in seeds:
        arr = await stochastic_branch_and_bound(blocks, s)
        h = arrangement_hash(arr.blocks)
        if h not in seen_hashes:
            seen_hashes.add(h)
            unique.append(arr)
        else:
            log.debug(f"[DUPLICATE] вариант {arr.seed} пропущен")

        await asyncio.sleep(0)
        gc.collect()

    log.info(f"✅ Сгенерировано уникальных вариантов: {len(unique)} / {len(seeds)}")
    return unique
