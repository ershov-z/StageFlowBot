# core/validator.py
from __future__ import annotations
import logging
from core.types import Block
from core.conflicts import strong_conflict, weak_conflict, kv_conflict

log = logging.getLogger("stageflow.validator")

# ============================================================
# ✅ Основная функция валидации
# ============================================================

def validate_arrangement(blocks: list[Block]) -> bool:
    """
    Проверяет готовую программу на корректность.
    Возвращает True, если ошибок нет, иначе False.

    Требования:
    - не более 3 тянучек;
    - отсутствие сильных конфликтов и kv:true подряд;
    - отсутствие слабых конфликтов без тянучки между номерами.
    """
    ok = True
    total_fillers = sum(1 for b in blocks if b.type == "filler")

    # ------------------ 1. Проверка количества тянучек ------------------
    if total_fillers > 3:
        log.error(f"❌ Нарушение: слишком много тянучек ({total_fillers} > 3)")
        ok = False

    # ------------------ 2. Проверка конфликтов ------------------
    for i in range(len(blocks) - 1):
        a, b = blocks[i], blocks[i + 1]

        # --- пропускаем проверки между filler-блоками ---
        if a.type == "filler" or b.type == "filler":
            continue

        # === сильные конфликты ===
        if a.type == "performance" and b.type == "performance":
            if strong_conflict(a, b):
                log.error(f"❌ Сильный конфликт между '{a.name}' и '{b.name}'")
                ok = False
            if kv_conflict(a, b):
                log.error(f"❌ kv:true подряд ('{a.name}' и '{b.name}')")
                ok = False

        # === слабые конфликты без тянучки ===
        if a.type == "performance" and b.type == "performance":
            if weak_conflict(a, b):
                log.error(f"❌ Слабый конфликт без тянучки: '{a.name}' ↔ '{b.name}'")
                ok = False

    # ------------------ 3. Проверка порядка фиксированных ------------------
    fixed_blocks = [(i, b) for i, b in enumerate(blocks) if b.fixed]
    if fixed_blocks:
        ids_in_arrangement = [b.id for _, b in fixed_blocks]
        if ids_in_arrangement != sorted(ids_in_arrangement):
            log.error("❌ Фиксированные блоки нарушили исходный порядок:")
            for idx, b in fixed_blocks:
                log.error(f"   - {idx}: {b.name} (id={b.id})")
            ok = False

    # ------------------ Итог ------------------
    if ok:
        log.info(f"✅ Вариант корректен: {len(blocks)} блоков, {total_fillers} тянучек")
    else:
        log.warning("⚠️ Вариант не прошёл валидацию.")
    return ok


# ============================================================
# 🧪 Для локального теста
# ============================================================
if __name__ == "__main__":
    from core.types import Actor

    perf1 = Block(1, "Номер 1", "performance", [Actor("Пушкин")], kv=False, fixed=True)
    perf2 = Block(2, "Номер 2", "performance", [Actor("Исаев")], kv=False)
    filler = Block(3, "[filler] Пушкин", "filler", [Actor("Пушкин")])
    perf3 = Block(4, "Номер 3", "performance", [Actor("Рожков")], kv=True)
    perf4 = Block(5, "Номер 4", "performance", [Actor("Пушкин")], kv=True)

    blocks = [perf1, filler, perf2, perf3, perf4]
    validate_arrangement(blocks)
