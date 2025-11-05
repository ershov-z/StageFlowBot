from docx import Document
from docx.shared import Pt, Inches
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from loguru import logger


def _add_shading(cell, fill="DDDDDD"):
    """Добавляет заливку (background color) в ячейку."""
    tc_pr = cell._element.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)


def _style_header_cell(cell):
    """Применяет стиль к ячейке заголовка таблицы."""
    _add_shading(cell, fill="DDDDDD")
    for paragraph in cell.paragraphs:
        run = paragraph.runs[0] if paragraph.runs else paragraph.add_run()
        run.bold = True
        run.font.size = Pt(10)


def _style_regular_cell(cell):
    """Базовое форматирование ячеек таблицы."""
    for paragraph in cell.paragraphs:
        for run in paragraph.runs:
            run.font.size = Pt(10)


def save_program_to_docx(program_data, output_path, template_path=None):
    """
    Сохраняет программу в .docx виде.
    program_data — список номеров (dict)
    """
    try:
        doc = Document(template_path) if template_path else Document()

        # Заголовок
        doc.add_heading("Программа концерта", level=1)

        # Таблица
        table = doc.add_table(rows=1, cols=6)
        table.style = "Table Grid"

        hdr_cells = table.rows[0].cells
        headers = ["№", "Название", "Актёры", "ПП", "Ответственный", "Тип"]
        for i, h in enumerate(headers):
            hdr_cells[i].text = h
            _style_header_cell(hdr_cells[i])

        # Добавляем строки программы
        for item in program_data:
            row_cells = table.add_row().cells
            row_cells[0].text = str(item.get("num", "") or item.get("order", ""))
            row_cells[1].text = item.get("title", "")
            row_cells[2].text = "\n".join(
                [a.get("name", "") for a in item.get("actors", []) if a.get("name")]
            )
            row_cells[3].text = item.get("pp", "")
            row_cells[4].text = item.get("responsible", "")
            row_cells[5].text = item.get("type", "")

            for cell in row_cells:
                _style_regular_cell(cell)

        # Автоматически задаём ширину колонок (чтобы таблица не ломалась)
        widths = [Inches(0.4), Inches(2.2), Inches(2.0), Inches(1.0), Inches(1.2), Inches(1.0)]
        for row in table.rows:
            for i, width in enumerate(widths):
                row.cells[i].width = width

        # Сохраняем документ
        doc.save(output_path)
        logger.info(f"📁 DOCX сохранён: {output_path}")
        return output_path

    except Exception as e:
        logger.exception(f"Ошибка при сохранении DOCX: {e}")
        raise e
