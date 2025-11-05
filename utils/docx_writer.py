
# -*- coding: utf-8 -*-
# utils/docx_writer.py
from __future__ import annotations

from typing import Iterable, Tuple, List, Optional
from docx import Document
from docx.shared import Pt, Inches
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

# =============== helpers ===============

def _add_shading(cell, fill: str = "DDDDDD"):
    tc_pr = cell._element.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)

def _style_header_cell(cell):
    _add_shading(cell, "DDDDDD")
    for p in cell.paragraphs:
        r = p.runs[0] if p.runs else p.add_run("")
        r.bold = True
        r.font.size = Pt(10)
        r.font.name = "Calibri"

def _style_regular_cell(cell):
    for p in cell.paragraphs:
        if not p.runs:
            p.add_run("")
        for r in p.runs:
            r.font.size = Pt(10)
            r.font.name = "Calibri"

def _extract_lines(value: str) -> List[str]:
    """Split by any newline, trim, keep order, drop empties."""
    if not value:
        return []
    value = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in value.split("\n")]
    return [ln for ln in lines if ln]

def _is_non_number_type(item_type: str) -> bool:
    t = (item_type or "").strip().lower()
    return t in {"тянучка", "спонсоры", "предкулисье"}

def _split_actors_for_columns(actors_raw: str, pp_field: str) -> Tuple[List[str], List[str]]:
    """
    Returns (actors_without_pp, pp_only) as lists of strings.
    - Keep tags (%, !, (гк)) as-is.
    - Exclude 'Пушкин' and 'Пятков' (any case) from actors_without_pp.
    - pp_only contains Pushkin/Pyatkov collected from actors_raw and pp, de-duplicated (stable order).
    """
    raw_lines = _extract_lines(actors_raw)
    pp_lines = _extract_lines(pp_field)

    def is_pp_name(s: str) -> bool:
        low = s.lower()
        return ("пушкин" in low) or ("пятков" in low)

    actors_wo_pp = [ln for ln in raw_lines if not is_pp_name(ln)]
    pp_candidates = [ln for ln in raw_lines if is_pp_name(ln)]
    pp_candidates += [ln for ln in pp_lines if is_pp_name(ln)]

    seen = set()
    pp_only: List[str] = []
    for ln in pp_candidates:
        key = ln.strip().lower()
        if key and key not in seen:
            seen.add(key)
            pp_only.append(ln.strip())

    return actors_wo_pp, pp_only

# =============== main ===============

def save_program_to_docx(program_data: Iterable[dict], output_path, template_path: Optional[str] = None):
    """
    Собирает финальный .docx по требованиям:
    Колонки: N | Название | Актёры | ПП | Найм | Ответственный | КВ

    Правила:
    - N — сквозная нумерация только для выступлений (type NOT IN {тянучка, спонсоры, предкулисье}).
    - Актёры — из actors_raw, с сохранением тегов, НО без Пушкина/Пяткова.
    - ПП — только Пушкин/Пятков (с тегами), собранные из actors_raw и pp, уникализированные.
    - Найм/Ответственный — как есть.
    - КВ — слово "Кв" при kv: true.

    Параметр template_path допускается, но по умолчанию создаётся чистый документ.
    """
    doc = Document(template_path) if template_path else Document()

    # Заголовок (не обязателен, но удобен)
    doc.add_heading("Программа концерта", level=1)

    table = doc.add_table(rows=1, cols=7)
    table.style = "Table Grid"
    headers = ["№", "Название", "Актёры", "ПП", "Найм", "Ответственный", "КВ"]

    # Header row
    for i, name in enumerate(headers):
        cell = table.rows[0].cells[i]
        cell.text = name
        _style_header_cell(cell)

    seq = 0
    for item in program_data:
        row = table.add_row()
        cells = row.cells

        item_type = (item.get("type") or "").strip().lower()
        is_numbered = not _is_non_number_type(item_type)

        # N
        if is_numbered:
            seq += 1
            cells[0].text = str(seq)
        else:
            cells[0].text = ""

        # Название
        cells[1].text = str(item.get("title", "") or "").strip()

        # Актёры / ПП
        actors_raw = item.get("actors_raw", "") or ""
        pp_field = item.get("pp", "") or ""
        actors_wo_pp, pp_only = _split_actors_for_columns(actors_raw, pp_field)
        cells[2].text = "\n".join(actors_wo_pp)
        cells[3].text = "\n".join(pp_only)

        # Найм
        cells[4].text = str(item.get("hire", "") or "").strip()

        # Ответственный
        cells[5].text = str(item.get("responsible", "") or "").strip()

        # КВ
        cells[6].text = "Кв" if bool(item.get("kv")) else ""

        for c in cells:
            _style_regular_cell(c)

    # Column widths (approx.)
    widths = [
        Inches(0.7),  # №
        Inches(2.4),  # Название
        Inches(2.8),  # Актёры
        Inches(1.4),  # ПП
        Inches(1.6),  # Найм
        Inches(1.8),  # Ответственный
        Inches(0.8),  # КВ
    ]
    for row in table.rows:
        for i, w in enumerate(widths):
            row.cells[i].width = w

    doc.add_paragraph("")
    doc.add_paragraph("Файл автоматически сгенерирован StageFlowBot", style="Intense Quote")

    doc.save(output_path)
    return output_path
