from __future__ import annotations
import json
import logging
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from core.types import Arrangement, Block
from core.docx_builder import create_docx

log = logging.getLogger("stageflow.exporter")

# ============================================================
# JSON-экспорт вспомогательная функция
# ============================================================

def export_json(arr: Arrangement, path: Path, extra: dict | None = None):
    """Сохраняет JSON-файл с программой."""
    data = {
        "seed": arr.seed,
        "strong_conflicts": arr.strong_conflicts,
        "weak_conflicts": arr.weak_conflicts,
        "fillers_used": arr.fillers_used,
        "meta": arr.meta or {},
        "blocks": [b.to_dict() for b in arr.blocks],
    }
    if extra:
        data.update(extra)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"[EXPORT] JSON сохранён: {path.name}")


# ============================================================
# DOCX-экспорт одного варианта
# ============================================================

def export_variant(arr: Arrangement, index: int, output_dir: Path) -> tuple[str, str]:
    """Экспортирует один вариант в DOCX и JSON."""
    seed = arr.seed
    is_ideal = arr.meta.get("status") == "ideal"
    label = f"StageFlow_Variant_{index}_seed{seed}"
    if is_ideal:
        label += "_IDEAL"

    docx_path = output_dir / f"{label}.docx"
    json_path = output_dir / f"{label}.json"

    # Генерация документа
    create_docx(
        arr.blocks,
        docx_path,
        title=f"Вариант #{index} (seed={seed}){' — ИДЕАЛЬНЫЙ' if is_ideal else ''}"
    )

    # Экспорт JSON
    export_json(arr, json_path, extra={"is_ideal": is_ideal})

    log.info(f"[EXPORT] {'Идеальный ' if is_ideal else ''}вариант seed={seed} → {docx_path.name}")
    return str(docx_path), str(json_path)


# ============================================================
# Пакетный экспорт всех вариантов
# ============================================================

def export_all_variants(arrangements: list[Arrangement], output_dir: Path) -> Path:
    """
    Экспортирует все варианты в DOCX и JSON и собирает ZIP.
    Идеальные варианты помещаются в начало архива.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("[EXPORT_ALL] Начинается пакетный экспорт всех вариантов")

    # Сначала сортируем, чтобы идеальные шли первыми
    sorted_arrs = sorted(
        arrangements,
        key=lambda a: 0 if a.meta.get("status") == "ideal" else 1
    )

    exported_files: list[str] = []
    for idx, arr in enumerate(sorted_arrs, start=1):
        docx_path, json_path = export_variant(arr, idx, output_dir)
        exported_files.extend([docx_path, json_path])

    # Архивируем всё
    zip_path = output_dir / "StageFlow_Results.zip"
    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zipf:
        for fpath in exported_files:
            arcname = Path(fpath).name
            zipf.write(fpath, arcname)
            log.info(f"[EXPORT_ALL] Добавлен в архив: {arcname}")

    log.info(f"[EXPORT_ALL] Архив готов: {zip_path}")
    return zip_path


# ============================================================
# Для ручного теста
# ============================================================

if __name__ == "__main__":
    from core.types import Actor

    # Простая проверка экспорта
    test_block = Block(1, "Номер 1", "performance", [Actor("Пушкин")])
    arr = Arrangement(seed=0, blocks=[test_block], fillers_used=0, strong_conflicts=0, weak_conflicts=0, meta={"status": "ideal"})
    out_dir = Path("./test_export")
    export_all_variants([arr], out_dir)
