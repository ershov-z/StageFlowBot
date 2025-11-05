# -*- coding: utf-8 -*-
# utils/docx_writer.py

from __future__ import annotations
from typing import Iterable, List, Tuple, Optional
from pathlib import Path
from docx import Document
from docx.shared import Pt, Inches
from docx.oxml import OxmlElement
from docx.oxml.ns import qn


# ============================================================
# 🔧 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def _clear_cell(cell):
    """Полностью очищает содержимое ячейки без лишних пустых параграфов"""
    for p in cell.paragraphs:
        p._element.getparent().remove(p._element)
    cell._element.append(cell._element._new_p())


def _add_shading(cell, fill="DDDDDD"):
    tc_pr = cell._element.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)


def _style_header_cell(cell):
    _add_shading(cell)
    _clear_cell(cell)
    run = cell.paragraphs[0].add_run()
    run.bold = True
    run.font.size = Pt(10)
    run.font.name = "Calibri"


def _style_regular_cell(cell):
    for p in cell.paragraphs:
        for r in p.runs:
            r.font.size = Pt(10)
            r.font.name = "Calibri"


def _extract_lines(value: str) -> List[str]:
    if not value:
        return []
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [l.strip() for l in text.split("\n")]
    return [l for l in lines if l]


def _is_non_number_type(t: str) -> bool:
    """True — если элемент не должен нумероваться"""
    t = (t or "").strip().lower()
    return any(x in t for x in ["тянучк", "спонсор", "предкулись"])


def _split_actors_for_columns(actors_raw: str, pp_field: str) -> Tuple[List[str], List[str]]:
    """Разделяет актёров на 2 колонки: без Пушкина/Пяткова и только Пушкин/Пятков"""
    raw_lines = _extract_lines(actors_raw)
    pp_lines = _extract_lines(pp_field)

    def is_pp(s: str) -> bool:
        low = s.lower()
        return "пушкин" in low or "пятков" in low

    actors_wo_pp = [a for a in raw_lines if not is_pp(a)]
    pp_candidates = [a for a in raw_lines if is_pp(a)] + [a for a in pp_lines if is_pp(a)]

    seen = set()
    pp_only = []
    for a in pp_candidates:
        key = a.lower()
        if key not in seen:
            seen.add(key)
            pp_only.append(a)
    return actors_wo_pp, pp_only


# ============================================================
# 🧩 ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def save_program_to_docx(
    program_data: Iterable[dict],
    output_path: str,
    template_path: Optional[str] = None,
    original_filename: Optional[str] = None
):
    """
    Генерирует DOCX с колонками:
    № | Название | Актёры | ПП | Найм | Ответственный | КВ

    🔹 Если передано имя исходного файла — итоговый DOCX получит суффикс "_ershobot".
       Например: "Программа_концерта.docx" → "Программа_концерта_ershobot.docx"
    """
    # Формируем итоговый путь
    if original_filename:
        base = Path(original_filename).stem
        out_dir = Path(output_path).parent
        output_path = out_dir / f"{base}_ershobot.docx"

    doc = Document(template_path) if template_path else Document()
    doc.add_heading("Программа концерта", level=1)

    table = doc.add_table(rows=1, cols=7)
    table.style = "Table Grid"

    headers = ["№", "Название", "Актёры", "ПП", "Найм", "Ответственный", "КВ"]
    for i, name in enumerate(headers):
        cell = table.rows[0].cells[i]
        _clear_cell(cell)
        cell.text = name
        _style_header_cell(cell)

    seq = 0
    for item in program_data:
        row = table.add_row()
        cells = row.cells
        for c in cells:
            _clear_cell(c)

        item_type = (item.get("type") or "").strip().lower()
        numbered = not _is_non_number_type(item_type)

        # № — только если элемент допускает нумерацию
        if numbered:
            seq += 1
            cells[0].text = str(seq)
        else:
            cells[0].text = ""

        # Название
        cells[1].text = str(item.get("title", "")).strip()

        # Актёры и ПП
        actors_raw = item.get("actors_raw", "") or ""
        pp_field = item.get("pp", "") or ""
        actors_wo_pp, pp_only = _split_actors_for_columns(actors_raw, pp_field)

        cells[2].text = "\n".join(actors_wo_pp)
        cells[3].text = "\n".join(pp_only)

        # Найм / Ответственный / КВ
        cells[4].text = str(item.get("hire", "") or "").strip()
        cells[5].text = str(item.get("responsible", "") or "").strip()
        cells[6].text = "Кв" if item.get("kv") else ""

        for c in cells:
            _style_regular_cell(c)

    # Ширины
    widths = [
        Inches(0.7), Inches(2.2), Inches(2.8),
        Inches(1.3), Inches(1.5), Inches(1.6), Inches(0.7)
    ]
    for row in table.rows:
        for i, w in enumerate(widths):
            row.cells[i].width = w

    # Сохраняем без подписи
    doc.save(output_path)
    return str(output_path)
