# core/exporter.py
from docx import Document
from docx.shared import Pt
from docx.oxml import parse_xml
from docx.oxml.ns import nsdecls, qn
from pathlib import Path
from typing import List
import zipfile

from .types import Block, Arrangement
from service.logger import get_logger

logger = get_logger(__name__)


def export_arrangement(arrangement: Arrangement, template_path: Path, output_path: Path) -> Path:
    """
    Преобразует Arrangement (список блоков) в .docx таблицу на основе шаблона.

    Сохраняет формат и порядок колонок:
    1. № (только для выступлений)
    2. Актёры
    3. ПП
    4. Найм (пустая)
    5. Ответственный (пустая)
    6. kv
    """
    logger.info(f"[EXPORT] Начат экспорт seed={arrangement.seed}")

    doc = Document(template_path)
    table = doc.tables[0]

    # Удаляем старые строки кроме заголовков
    while len(table.rows) > 1:
        table._element.remove(table.rows[1]._element)

    for index, block in enumerate(arrangement.blocks, start=1):
        _append_block_row(table, block, index)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(output_path)

    logger.info(f"[EXPORT] Успешно сохранён: {output_path}")
    return output_path


def export_all(arrangements: List[Arrangement], template_path: Path, export_dir: Path) -> Path:
    """
    Экспортирует пять вариантов программы в .docx и упаковывает их в ZIP.

    :param arrangements: список из 5 Arrangement
    :param template_path: шаблон docx таблицы
    :param export_dir: директория для вывода
    :return: путь к zip архиву
    """
    logger.info("[EXPORT_ALL] Начинается пакетный экспорт пяти вариантов")

    export_dir.mkdir(parents=True, exist_ok=True)
    docx_paths = []

    for i, arrangement in enumerate(arrangements, start=1):
        output_file = export_dir / f"StageFlow_Variant_{i}_seed{arrangement.seed}.docx"
        export_arrangement(arrangement, template_path, output_file)
        docx_paths.append(output_file)

    # Создаём архив
    zip_path = export_dir / "StageFlow_Results.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for path in docx_paths:
            zipf.write(path, arcname=path.name)
            logger.info(f"[EXPORT_ALL] Добавлен в архив: {path.name}")

    logger.info(f"[EXPORT_ALL] Архив готов: {zip_path}")
    return zip_path


def _append_block_row(table, block: Block, index: int):
    """
    Добавляет строку для блока (выступление, филлер, предкулисье, спонсор).
    """
    row = table.add_row()
    cells = row.cells

    # 1. Номер
    cells[0].text = str(index) if block.type == "performance" else ""

    # 2. Актёры (в исходном виде — включая теги)
    actor_texts = getattr(block, "raw_actors", None)
    if not actor_texts:
        actor_texts = [getattr(a, "raw", a.name) for a in block.actors]
    cells[1].text = ", ".join(actor_texts)

    # 3. ПП (только актёры Пушкин и Пятков, с сохранением тегов)
    pp_actors = [a for a in actor_texts if "Пушкин" in a or "Пятков" in a]
    cells[2].text = ", ".join(pp_actors)

    # 4–5. Найм и Ответственный (пустые)
    cells[3].text = ""
    cells[4].text = ""

    # 6. kv (если kv: True)
    cells[5].text = "кв" if getattr(block, "kv", False) else ""

    _apply_block_style(row, block)


def _apply_block_style(row, block: Block):
    """
    Применяет визуальное оформление для особых типов блоков.
    """
    for cell in row.cells:
        for paragraph in cell.paragraphs:
            run = paragraph.runs[0] if paragraph.runs else paragraph.add_run()
            font = run.font
            font.name = "Calibri"
            font.size = Pt(11)
            r = run._element
            r.rPr.rFonts.set(qn("w:eastAsia"), "Calibri")

    # Цветовая схема по типам
    if block.type == "filler":
        _set_row_shading(row, "FFF2CC")
        _prefix_label(row, "[filler]")
    elif block.type == "prelude":
        _set_row_shading(row, "D9E1F2")
        _prefix_label(row, "[prelude]")
    elif block.type == "sponsor":
        _set_row_shading(row, "E2EFDA")
        _prefix_label(row, "[sponsor]")


def _prefix_label(row, label: str):
    """
    Добавляет текстовую метку в начало второй колонки (актёры).
    """
    cell = row.cells[1]
    cell.text = f"{label} {cell.text.strip()}"


def _set_row_shading(row, color_hex: str):
    """
    Устанавливает цвет фона для всей строки.
    """
    for cell in row.cells:
        cell._element.get_or_add_tcPr().append(_shading_xml(color_hex))


def _shading_xml(color_hex: str):
    return parse_xml(rf'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>')
