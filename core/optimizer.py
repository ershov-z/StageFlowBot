# core/optimizer.py
from __future__ import annotations

import logging
import random
from dataclasses import replace
from typing import List, Optional, Tuple
import asyncio

from core.types import Block, Actor
from core.conflicts import strong_conflict, weak_conflict, kv_conflict
from core.fillers import pick_filler_actor
from service.hash_utils import arrangement_hash, is_duplicate, register_hash
from service.timing import measure_time

log = logging.getLogger("stageflow.optimizer")


# ============================================================
# 🧩 Вспомогательные функции
# ============================================================

def _last_performance(seq: List[Block]) -> Optional[Block]:
    """Возвращает последний performance-блок в уже собранной части программы."""
    for b in reversed(seq):
        if b.type == "performance":
            return b
    return None


def _make_filler(prev: Block, nxt: Block, actor_name: str, next_id: int) -> Block:
    """Создаёт filler-блок с выбранным актёром."""
    return Block(
        id=next_id,
        name=f"[filler] {actor_name}",
        type="filler",
        actors=[Actor(actor_name)],
        kv=False,
        fixed=False,
    )


def _needs_filler(prev_perf: Block, cand: Block) -> Tuple[bool, bool]:
    """
    Проверяет пару соседних performance-блоков.
    Возвращает кортеж (запретить, нужен_филлер):
      - запретить=True, если сильный конфликт или kv-соседство => нельзя ставить cand после prev_perf
      - нужен_филлер=True, если слабый конфликт => можно решить тянучкой
    """
    if prev_perf is None or cand.type != "performance":
        return (False, False)

    if strong_conflict(prev_perf, cand):
        return (True, False)
    if kv_conflict(prev_perf, cand):
        return (True, False)

    if weak_conflict(prev_perf, cand):
        return (False, True)

    return (False, False)


# ============================================================
# 🎛️ Основной стохастический backtracking с отсечением
# ============================================================

@measure_time("optimizer.stochastic_branch_and_bound")
async def stochastic_branch_and_bound(blocks: List[Block], seed: int) -> List[Block]:
    """
    Собирает один лучший вариант программы для заданного seed.
    - Фиксированные блоки (fixed=True) не переставляются.
    - Нефиксированные performance-блоки переставляются.
    - Сильные конфликты недопустимы.
    - Слабые конфликты разрешаются тянучками (до 3 шт) через pick_filler_actor.
    - Цель — минимизировать количество тянучек.
    """
    rng = random.Random(seed)
    seen_hashes: set[str] = set()

    base_seq: List[Block] = [b for b in blocks if b.type != "filler"]

    fixed_positions = {i for i, b in enumerate(base_seq) if b.fixed}
    fixed_at_index = {i: base_seq[i] for i in fixed_positions}

    variable_pool: List[Block] = [b for b in base_seq if not b.fixed]

    max_id = max((b.id for b in blocks), default=0)
    next_new_id = max_id + 1

    best_arrangement: Optional[List[Block]] = None
    best_fillers_used: int = 99
    found_perfect = False

    rng.shuffle(variable_pool)

    # --------------------------------------------------------
    # Рекурсивная сборка
    # --------------------------------------------------------
    def dfs(pos: int,
            pool: List[Block],
            assembled: List[Block],
            fillers_used: int) -> None:
        nonlocal best_arrangement, best_fillers_used, found_perfect, next_new_id

        # Отсечение по количеству тянучек
        if fillers_used >= best_fillers_used:
            return
        if fillers_used > 3:
            return
        if found_perfect:
            return

        # База: собрали весь каркас
        if pos == len(base_seq):
            candidate = assembled.copy()
            h = arrangement_hash(candidate)
            if not is_duplicate(candidate, seen_hashes):
                register_hash(candidate, seen_hashes)
                best_arrangement = candidate
                best_fillers_used = fillers_used
                log.info(f"[RESULT] seed={seed} | fillers={fillers_used} | hash={h[:8]}")
                if best_fillers_used == 0:
                    found_perfect = True
            else:
                log.debug(f"[DUPLICATE] seed={seed} вариант уже встречался ({h[:8]})")
            return

        # Фиксированные позиции
        if pos in fixed_positions:
            cand = fixed_at_index[pos]
            prev_perf = _last_performance(assembled)
            forbid, need_fill = _needs_filler(prev_perf, cand)
            if forbid:
                return

            if need_fill:
                if fillers_used >= 3:
                    return
                actor_name = pick_filler_actor(prev_perf, cand, seed=seed ^ (pos << 8))
                if actor_name is None:
                    return
                filler_block = _make_filler(prev_perf, cand, actor_name, next_new_id)
                next_new_id += 1
                assembled.append(filler_block)
                assembled.append(cand)
                dfs(pos + 1, pool, assembled, fillers_used + 1)
                assembled.pop()
                assembled.pop()
            else:
                assembled.append(cand)
                dfs(pos + 1, pool, assembled, fillers_used)
                assembled.pop()
            return

        # Нефиксированные
        try_order = pool.copy()
        rng.shuffle(try_order)

        for cand in try_order:
            prev_perf = _last_performance(assembled)
            forbid, need_fill = _needs_filler(prev_perf, cand)
            if forbid:
                continue

            if need_fill:
                if fillers_used >= 3:
                    continue
                actor_name = pick_filler_actor(prev_perf, cand, seed=seed ^ (pos << 12))
                if actor_name is None:
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

    # --------------------------------------------------------
    # Старт
    # --------------------------------------------------------
    log.info(f"▶️ Start BnB (seed={seed}) | fixed={len(fixed_positions)} | variable={len(variable_pool)}")
    dfs(pos=0, pool=variable_pool, assembled=[], fillers_used=0)

    if best_arrangement is None:
        log.warning(f"⚠️ Не удалось собрать вариант для seed={seed}. Возвращаю исходный порядок.")
        return blocks

    # Финальная проверка на сильные конфликты
    for i in range(len(best_arrangement) - 1):
        a, b = best_arrangement[i], best_arrangement[i + 1]
        if a.type == "performance" and b.type == "performance":
            if strong_conflict(a, b) or kv_conflict(a, b):
                log.error(f"❌ Post-check strong/kv conflict between '{a.name}' and '{b.name}'")
                break

    log.info(f"✅ Done (seed={seed}) | fillers={best_fillers_used} | total_blocks={len(best_arrangement)}")
    return best_arrangement
