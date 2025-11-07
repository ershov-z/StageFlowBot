# core/exporter.py
from __future__ import annotations
from docx import Document
from pathlib import Path
import zipfile
import json
from typing import Dict, Optional, List

from core.types import Block, Arrangement
from service.logger import get_logger

logger = get_logger(__name__)


# ============================================================
# 🗂️ Определение схемы колонок таблицы шаблона
# ============================================================

def _normalize_header(s: str) -> str:
    return (s or "").strip().lower()


def _guess_mapping_by_header(header_cells: List[str]) -> Optional[Dict[str, int]]:
    """
    Возвращает индексы колонок по заголовку.
    Поддерживает:
      - Новую (v1-style): [№, Название, Актёры, ПП, Найм, Ответственный, Кв]
      - Старую (v2):     [№, Актёры, ПП, Найм, Ответственный, Кв]
    """
    h = [_normalize_header(x) for x in header_cells]
    idx = {name: i for i, name in enumerate(h)}

    def find(*aliases) -> Optional[int]:
        for a in aliases:
            if a in idx:
                return idx[a]
        return None

    num_i  = find("№", "номер", "num", "#", "n")
    name_i = find("название", "title", "назв")
    act_i  = find("актеры", "актёры", "actors", "участники")
    pp_i   = find("пп", "pp")
    hire_i = find("найм", "наим", "hire")
    rsp_i  = find("ответственный", "ответств", "responsible")
    kv_i   = find("кв", "kv")

    # Полная новая схема
    if all(x is not None for x in (num_i, name_i, act_i, pp_i, hire_i, rsp_i, kv_i)):
        return {"num": num_i, "name": name_i, "actors": act_i, "pp": pp_i, "hire": hire_i, "resp": rsp_i, "kv": kv_i}

    # Старая схема (без «Название»)
    if all(x is not None for x in (num_i, act_i, pp_i, kv_i)):
        # По умолчанию: 0 №, 1 Актёры, 2 ПП, 3 Найм, 4 Ответственный, 5 Кв
        # Но если в заголовке иначе — используем найденные индексы.
        return {
            "num": num_i,
            "name": None,          # нет колонки «Название»
            "actors": act_i,
            "pp": pp_i,
            "hire": hire_i if hire_i is not None else (3 if len(h) > 3 else None),
            "resp": rsp_i if rsp_i is not None else (4 if len(h) > 4 else None),
            "kv": kv_i
        }

    return None


def _fallback_mapping_by_count(n_cols: int) -> Dict[str, Optional[int]]:
    """Эвристика по количеству колонок, если заголовок странный или пустой."""
    if n_cols >= 7:
        return {"num": 0, "name": 1, "actors": 2, "pp": 3, "hire": 4, "resp": 5, "kv": 6}
    # старая 6-колоночная сетка
    return {"num": 0, "name": None, "actors": 1, "pp": 2, "hire": 3 if n_cols > 3 else None,
            "resp": 4 if n_cols > 4 else None, "kv": 5 if n_cols > 5 else None}


# ============================================================
# 🎨 Оформление строк (не меняем шаблон, только подсветка)
# ============================================================

def _apply_block_style(row, block: Block):
    """
    Минимальное оформление для типов блоков.
    Не трогает ширины, шрифты и стили — полностью сохраняет шаблон.
    """
    if block.type == "filler":
        _set_row_shading(row, "FFF2CC")
        _prefix_label(row, block, "[filler]")
    elif block.type == "prelude":
        _set_row_shading(row, "D9E1F2")
        _prefix_label(row, block, "[prelude]")
    elif block.type == "sponsor":
        _set_row_shading(row, "E2EFDA")
        _prefix_label(row, block, "[sponsor]")


def _prefix_label(row, block: Block, label: str):
    """
    Добавляет текстовую метку в начало колонки «Название» (если есть),
    иначе — в колонку «Актёры».
    """
    cells = row.cells
    # Предпочтительно — ячейка «Название» (index=1 в новой схеме),
    # но позицию мы заранее не знаем. Возьмём вторую ячейку, это «Название» в новой схеме.
    target_idx = 1 if len(cells) >= 7 else 1  # во «второй» колонке метка выглядит уместнее
    cell = cells[target_idx]
    current = (cell.text or "").strip()
    cell.text = f"{label} {current}" if current else label


def _set_row_shading(row, color_hex: str):
    """Устанавливает цвет фона для всей строки без изменения структуры."""
    from docx.oxml import parse_xml
    from docx.oxml.ns import nsdecls

    for cell in row.cells:
        cell._element.get_or_add_tcPr().append(
            parse_xml(rf'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>')
        )


# ============================================================
# 📦 Экспорт одного варианта
# ============================================================

def export_arrangement(arrangement: Arrangement, template_path: Path, output_path: Path) -> Path:
    """
    Преобразует Arrangement (список блоков) в .docx таблицу на основе шаблона.
    Сохраняет формат оригинала, добавляя:
      • сквозную нумерацию только для type == "performance";
      • значения «Актёры» = actors_raw, «ПП» = pp_raw (без повторного парсинга).
    """
    logger.info(f"[EXPORT] Начат экспорт seed={arrangement.seed}")

    doc = Document(template_path)
    if not doc.tables:
        raise RuntimeError("В шаблоне отсутствуют таблицы для экспорта")

    table = doc.tables[0]

    # --- Определяем маппинг колонок ---
    header_cells = [c.text for c in table.rows[0].cells] if table.rows else []
    mapping = _guess_mapping_by_header(header_cells)
    if mapping is None:
        mapping = _fallback_mapping_by_count(len(table.rows[0].cells))

    # --- Очищаем все строки кроме заголовка ---
    while len(table.rows) > 1:
        table._element.remove(table.rows[1]._element)

    seq = 0
    for block in arrangement.blocks:
        row = table.add_row()
        cells = row.cells

        def set_cell(key: str, text: str):
            idx = mapping.get(key)
            if idx is not None and idx < len(cells):
                cells[idx].text = text

        # № — только для выступлений
        if block.type == "performance":
            seq += 1
            set_cell("num", str(seq))
        else:
            set_cell("num", "")

        # Название
        if mapping.get("name") is not None:
            set_cell("name", block.name or "")

        # Актёры (сырой текст из исходной таблицы)
        set_cell("actors", getattr(block, "actors_raw", "") or "")

        # ПП (сырой текст из исходной таблицы)
        set_cell("pp", getattr(block, "pp_raw", "") or "")

        # Найм / Ответственный
        set_cell("hire", getattr(block, "hire", "") or "")
        set_cell("resp", getattr(block, "responsible", "") or "")

        # kv — метка
        set_cell("kv", "кв" if getattr(block, "kv", False) else "")

        _apply_block_style(row, block)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(output_path)
    logger.info(f"[EXPORT] Успешно сохранён DOCX: {output_path}")
    return output_path


# ============================================================
# 📦 Экспорт всех 5 вариантов + JSON
# ============================================================

def export_all(arrangements: list[Arrangement], template_path: Path, export_dir: Path) -> Path:
    """
    Экспортирует все варианты программы (DOCX + JSON) и упаковывает их в ZIP.
    JSON содержит все поля, по которым можно воспроизвести DOCX.
    """
    logger.info("[EXPORT_ALL] Начинается пакетный экспорт всех вариантов")

    export_dir.mkdir(parents=True, exist_ok=True)
    exported_files = []

    for i, arrangement in enumerate(arrangements, start=1):
        output_docx = export_dir / f"StageFlow_Variant_{i}_seed{arrangement.seed}.docx"
        output_json = export_dir / f"StageFlow_Variant_{i}_seed{arrangement.seed}.json"

        # DOCX
        export_arrangement(arrangement, template_path, output_docx)

        # JSON (полный)
        json_data = [
            {
                "id": b.id,
                "name": b.name,
                "type": b.type,
                "kv": b.kv,
                "fixed": b.fixed,
                "num": getattr(b, "num", ""),
                "actors_raw": getattr(b, "actors_raw", ""),
                "pp_raw": getattr(b, "pp_raw", ""),
                "hire": getattr(b, "hire", ""),
                "responsible": getattr(b, "responsible", ""),
                "actors": [{"name": a.name, "tags": list(a.tags)} for a in b.actors],
            }
            for b in arrangement.blocks
        ]
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        logger.info(f"[EXPORT_ALL] Сохранён JSON: {output_json}")
        exported_files.extend([output_docx, output_json])

    # ZIP
    zip_path = export_dir / "StageFlow_Results.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for path in exported_files:
            zipf.write(path, arcname=path.name)
            logger.info(f"[EXPORT_ALL] Добавлен в архив: {path.name}")

    logger.info(f"[EXPORT_ALL] Архив готов: {zip_path}")
    return zip_path
