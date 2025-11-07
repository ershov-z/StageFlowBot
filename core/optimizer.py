# core/optimizer.py
from __future__ import annotations
import asyncio
import random
import logging
from typing import List, Optional, Tuple
from copy import deepcopy
import gc  # PERF: для ручной сборки мусора после каждого seed'а

from core.types import Block, Arrangement, Actor
from core.conflicts import strong_conflict, weak_conflict, kv_conflict
from core.fillers import pick_filler_actor
from service.hash_utils import arrangement_hash, is_duplicate, register_hash
from service.timing import measure_time

log = logging.getLogger("stageflow.optimizer")

MAX_FILLERS = 3
MAX_VARIANTS = 5


# ============================================================
# 🧩 Вспомогательные функции
# ============================================================

def _copy_block(block: Block) -> Block:
    """Полное копирование блока (включая raw-поля)."""
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
    """Создаёт filler-блок с заполненными полями (v1-style)."""
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
# 🎛️ Основной стохастический backtracking
# ============================================================

@measure_time("optimizer.stochastic_branch_and_bound")
async def stochastic_branch_and_bound(blocks: List[Block], seed: int) -> Arrangement:
    """
    Собирает один вариант программы для заданного seed.
    Фиксированные блоки остаются на своих местах,
    переставляются только performance.
    """
    rng = random.Random(seed)
    seen_hashes: set[str] = set()

    # ------------------------------------------------------------------
    # 🔒 ДОСБОРКА ПРАВИЛ ФИКСАЦИИ (v2.4):
    # фиксируем: предкулисье; первые 2 номера + их тянучки;
    # последние 4 номера + их тянучки; спонсоры.
    # Делаем это ДО сборки списков fixed_positions / variable_pool,
    # чтобы флаги fixed корректно попали в рабочую копию. :contentReference[oaicite:1]{index=1}
    # ------------------------------------------------------------------
    # 1) заранее фиксируем типовые блоки
    for b in blocks:
        if b.type in {"prelude", "sponsor"}:
            b.fixed = True

    # 2) найдём индексы всех performance
    perf_indices = [i for i, b in enumerate(blocks) if b.type == "performance"]

    # 3) фиксируем первые 2 и последние 4 performance (если они есть)
    for i in perf_indices[:2]:
        blocks[i].fixed = True
    for i in perf_indices[-4:]:
        blocks[i].fixed = True

    # 4) фиксируем тянучки, которые находятся МЕЖДУ уже фиксированными блоками
    for i, b in enumerate(blocks):
        if b.type == "filler":
            prev_fixed = (i > 0) and blocks[i - 1].fixed
            next_fixed = (i < len(blocks) - 1) and blocks[i + 1].fixed
            if prev_fixed and next_fixed:
                b.fixed = True
    # ------------------------------------------------------------------

    # Рабочая копия блоков (filler не переставляем)
    base_seq: List[Block] = [_copy_block(b) for b in blocks if b.type != "filler"]
    fixed_positions = {i for i, b in enumerate(base_seq) if b.fixed}
    fixed_at_index = {i: base_seq[i] for i in fixed_positions}
    variable_pool: List[Block] = [b for b in base_seq if not b.fixed]

    max_id = max((b.id for b in blocks), default=0)
    next_new_id = max_id + 1

    best_arrangement: Optional[List[Block]] = None
    best_fillers_used: int = 99
    found_perfect = False

    rng.shuffle(variable_pool)

    def dfs(pos: int, pool: List[Block], assembled: List[Block], fillers_used: int) -> None:
        nonlocal best_arrangement, best_fillers_used, found_perfect, next_new_id
        if fillers_used >= best_fillers_used or fillers_used > MAX_FILLERS or found_perfect:
            return
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
            return

        if pos in fixed_positions:
            cand = fixed_at_index[pos]
            prev_perf = _last_performance(assembled)
            forbid, need_fill = _needs_filler(prev_perf, cand)
            if forbid:
                return
            # === изменено: строго отсекаем ветку, если filler обязателен, а лимит исчерпан
            if need_fill:
                if fillers_used < MAX_FILLERS:
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
                else:
                    return
            else:
                assembled.append(cand)
                dfs(pos + 1, pool, assembled, fillers_used)
                assembled.pop()
            return

        try_order = pool.copy()
        rng.shuffle(try_order)
        for cand in try_order:
            prev_perf = _last_performance(assembled)
            forbid, need_fill = _needs_filler(prev_perf, cand)
            if forbid:
                continue
            # === изменено: строго отсекаем ветку, если filler обязателен, а лимит исчерпан
            if need_fill:
                if fillers_used < MAX_FILLERS:
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
                    continue
            else:
                assembled.append(cand)
                new_pool = [b for b in pool if b is not cand]
                dfs(pos + 1, new_pool, assembled, fillers_used)
                assembled.pop()
            if found_perfect:
                return

    log.info(f"▶️ Start BnB (seed={seed}) | fixed={len(fixed_positions)} | variable={len(variable_pool)}")
    dfs(0, variable_pool, [], 0)

    if best_arrangement is None:
        log.warning(f"⚠️ Не удалось собрать вариант для seed={seed}. Возвращаю исходный порядок.")
        return Arrangement(seed=seed, blocks=blocks, fillers_used=0)

    # Финальная проверка конфликтов
    strong_cnt = sum(strong_conflict(best_arrangement[i], best_arrangement[i + 1])
                     for i in range(len(best_arrangement) - 1))
    weak_cnt = sum(weak_conflict(best_arrangement[i], best_arrangement[i + 1])
                   for i in range(len(best_arrangement) - 1))

    log.info(f"✅ Done (seed={seed}) | fillers={best_fillers_used} | total={len(best_arrangement)}")
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
    """Создаёт до 5 вариантов перестроенной программы."""
    seeds = [random.randint(1000, 99999) for _ in range(n_variants)]
    log.info(f"🧬 Seeds: {seeds}")

    # PERF: последовательная генерация вместо параллельной — экономим CPU/RAM на слабых инстансах.
    unique: List[Arrangement] = []
    seen_hashes = set()

    for s in seeds:
        arr = await stochastic_branch_and_bound(blocks, s)

        # Онлайновая фильтрация дублей (как раньше, но без накопления всего списка results)
        h = arrangement_hash(arr.blocks)
        if h not in seen_hashes:
            seen_hashes.add(h)
            unique.append(arr)
        else:
            log.debug(f"[DUPLICATE] вариант {arr.seed} пропущен")

        # Даём циклу событий подышать и просим GC освободить память от временных структур
        await asyncio.sleep(0)
        gc.collect()

    log.info(f"✅ Сгенерировано уникальных вариантов: {len(unique)} / {len(seeds)}")
    return unique
