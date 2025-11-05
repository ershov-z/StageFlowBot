from docx import Document
from docx.shared import Pt, Inches
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from loguru import logger


# ==============================================================
# 🔹 Стили и утилиты
# ==============================================================

def _add_shading(cell, fill="DDDDDD"):
    """
    Добавляет заливку (background color) в ячейку таблицы.
    Используем стандартный XML элемент <w:shd> вместо старого _new_shd().
    """
    tc_pr = cell._element.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)


def _style_header_cell(cell):
    """Форматирует ячейку заголовка таблицы."""
    _add_shading(cell, fill="DDDDDD")
    for paragraph in cell.paragraphs:
        run = paragraph.runs[0] if paragraph.runs else paragraph.add_run()
        run.bold = True
        run.font.size = Pt(10)
        run.font.name = "Calibri"


def _style_regular_cell(cell):
    """Форматирует обычные ячейки таблицы."""
    for paragraph in cell.paragraphs:
        for run in paragraph.runs:
            run.font.size = Pt(10)
            run.font.name = "Calibri"


# ==============================================================
# 🔹 Основная функция
# ==============================================================

def save_program_to_docx(program_data, output_path, template_path=None):
    """
    Сохраняет итоговую программу концерта в .docx.
    ✅ Актёры, теги и другие данные не изменяются.
    ✅ Меняется только порядок и возможные тянучки (добавленные валидатором).
    """
    try:
        logger.info("📝 Начинаем запись итогового DOCX...")
        doc = Document(template_path) if template_path else Document()

        # Заголовок
        doc.add_heading("Программа концерта", level=1)

        # Таблица
        table = doc.add_table(rows=1, cols=7)
        table.style = "Table Grid"

        # Заголовки
        headers = ["№", "Название", "Актёры", "ПП", "Ответственный", "Тип", "KV"]
        hdr_cells = table.rows[0].cells
        for i, text in enumerate(headers):
            hdr_cells[i].text = text
            _style_header_cell(hdr_cells[i])

        # Добавляем строки программы
        for item in program_data:
            row_cells = table.add_row().cells

            # Колонка № — сначала num, если есть, иначе order
            num_value = str(item.get("num") or item.get("order") or "")
            row_cells[0].text = num_value

            # Название
            row_cells[1].text = str(item.get("title", "")).strip()

            # Актёры — каждый на новой строке
            actors = item.get("actors", [])
            actor_lines = []
            for actor in actors:
                name = actor.get("name", "")
                tags = actor.get("tags", [])
                if tags:
                    tag_str = " ".join([f"({t})" for t in tags])
                    actor_lines.append(f"{name} {tag_str}")
                else:
                    actor_lines.append(name)
            row_cells[2].text = "\n".join(actor_lines)

            # ПП
            row_cells[3].text = str(item.get("pp", "")).strip()

            # Ответственный
            row_cells[4].text = str(item.get("responsible", "")).strip()

            # Тип
            row_cells[5].text = str(item.get("type", "")).strip()

            # KV (квартира)
            row_cells[6].text = "Да" if item.get("kv") else ""

            # Применяем стиль
            for c in row_cells:
                _style_regular_cell(c)

        # Задаём ширины колонок
        widths = [
            Inches(0.5),  # №
            Inches(2.2),  # Название
            Inches(2.5),  # Актёры
            Inches(1.2),  # ПП
            Inches(1.5),  # Ответственный
            Inches(1.0),  # Тип
            Inches(0.6),  # KV
        ]
        for row in table.rows:
            for i, w in enumerate(widths):
                row.cells[i].width = w

        # Добавляем подпись
        doc.add_paragraph("")
        doc.add_paragraph(
            "Файл автоматически сгенерирован StageFlowBot",
            style="Intense Quote"
        )

        # Сохраняем
        doc.save(output_path)
        logger.info(f"📁 DOCX сохранён: {output_path}")
        return output_path

    except Exception as e:
        logger.exception(f"Ошибка при сохранении DOCX: {e}")
        raise e
