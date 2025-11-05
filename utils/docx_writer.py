from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.oxml.ns import qn
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from loguru import logger
from pathlib import Path


# ============================================================
# 🔹 СЛУЖЕБНЫЕ ФУНКЦИИ
# ============================================================

def _tags_to_symbols(tags):
    """Преобразует внутренние теги ['early','later','gk'] в символы для вывода"""
    if not tags:
        return ""
    result = []
    if "gk" in tags:
        result.append("(гк)")
    if "early" in tags:
        result.append("!")
    if "later" in tags:
        result.append("%")
    return "".join(result)


def _combine_actors(entry):
    """Создает текст для колонки 'Актеры'"""
    lines = []
    for a in entry.get("actors", []):
        name = a.get("name", "").strip()
        tag_symbols = _tags_to_symbols(a.get("tags", []))
        lines.append(f"{name}{tag_symbols}")
    return "\n".join(lines) if lines else ""


def _style_header_cell(cell):
    """Заголовок таблицы — жирный, центрированный, серый фон"""
    p = cell.paragraphs[0]
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    run = p.runs[0]
    run.bold = True
    run.font.size = Pt(10)
    shading = cell._element.xpath('.//w:shd')[0] if cell._element.xpath('.//w:shd') else None
    if not shading:
        cell._element.get_or_add_tcPr().append(cell._element._new_shd(fill="DDDDDD"))


def _style_row_cell(cell, align="center"):
    """Стилизация обычной ячейки"""
    for p in cell.paragraphs:
        if align == "center":
            p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        else:
            p.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
        for r in p.runs:
            r.font.size = Pt(10)
            r.font.name = "Calibri"
            r._element.rPr.rFonts.set(qn("w:eastAsia"), "Calibri")


def _apply_tenuchka_style(row):
    """Подсветка строки-тянучки"""
    for cell in row.cells:
        tc_pr = cell._element.get_or_add_tcPr()
        shd = cell._element._new_shd(fill="EDEDED")  # светло-серый фон
        tc_pr.append(shd)
        for p in cell.paragraphs:
            for r in p.runs:
                r.font.bold = True
                r.font.color.rgb = RGBColor(90, 90, 90)


# ============================================================
# 🔹 ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

def save_program_to_docx(program_data, output_path):
    """
    Сохраняет итоговую программу в .docx файл.
    :param program_data: список номеров (с тянучками)
    :param output_path: строка или Path для сохранения
    :return: Path
    """
    logger.info("📝 Начинаем запись итогового DOCX...")

    doc = Document()

    # Заголовок
    title = doc.add_paragraph("Программа концерта")
    title.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    run = title.add_run()
    run.bold = True
    run.font.size = Pt(16)

    # Создаем таблицу
    table = doc.add_table(rows=1, cols=7)
    table.style = "Table Grid"
    hdr_cells = table.rows[0].cells
    headers = ["№", "Название", "Актеры", "ПП", "Найм", "Ответственный", "КВ"]

    for i, h in enumerate(headers):
        hdr_cells[i].text = h
        _style_header_cell(hdr_cells[i])

    # Заполняем строки
    for entry in program_data:
        row = table.add_row()
        cells = row.cells

        num = str(entry.get("num", "")) if entry.get("num") else ""
        title = entry.get("title", "")
        actors = _combine_actors(entry)
        pp = entry.get("pp", "")
        hire = entry.get("hire", "")
        resp = entry.get("responsible", "")
        kv = "Да" if entry.get("kv") else ""

        cells[0].text = num
        cells[1].text = title
        cells[2].text = actors
        cells[3].text = pp
        cells[4].text = hire
        cells[5].text = resp
        cells[6].text = kv

        # базовое выравнивание
        _style_row_cell(cells[0])
        _style_row_cell(cells[1], align="left")
        _style_row_cell(cells[2], align="left")
        _style_row_cell(cells[3], align="left")
        _style_row_cell(cells[4], align="left")
        _style_row_cell(cells[5], align="left")
        _style_row_cell(cells[6])

        # если тянучка — серым цветом
        if (entry.get("type") or "").lower() == "тянучка":
            _apply_tenuchka_style(row)

    # сохраняем
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(output_path)
    logger.success(f"💾 Итоговый DOCX сохранён: {output_path}")
    return output_path
