# core/exporter.py
from docx import Document
from pathlib import Path
import zipfile
import json

from .types import Block, Arrangement
from service.logger import get_logger

logger = get_logger(__name__)


# ============================================================
# 📦 Экспорт одного варианта
# ============================================================
def export_arrangement(arrangement: Arrangement, template_path: Path, output_path: Path) -> Path:
    """
    Преобразует Arrangement (список блоков) в .docx таблицу на основе шаблона.
    Полностью сохраняет формат оригинала, добавляя только сквозную нумерацию
    для выступлений (type == "performance").
    """
    logger.info(f"[EXPORT] Начат экспорт seed={arrangement.seed}")

    doc = Document(template_path)
    table = doc.tables[0]

    # Удаляем все строки кроме заголовков
    while len(table.rows) > 1:
        table._element.remove(table.rows[1]._element)

    seq = 0
    for block in arrangement.blocks:
        row = table.add_row()
        cells = row.cells

        # № — только для выступлений
        if block.type == "performance":
            seq += 1
            cells[0].text = str(seq)
        else:
            cells[0].text = ""

        # Актёры (в исходном виде, включая теги)
        actor_texts = getattr(block, "raw_actors", None)
        if not actor_texts:
            actor_texts = [getattr(a, "raw", a.name) for a in block.actors]
        cells[1].text = "\n".join(actor_texts)

        # ПП — только Пушкин и Пятков (с сохранением тегов)
        pp_actors = [a for a in actor_texts if "пушкин" in a.lower() or "пятков" in a.lower()]
        cells[2].text = "\n".join(pp_actors)

        # Найм / Ответственный — пустые
        cells[3].text = ""
        cells[4].text = ""

        # kv — пометка, если есть
        cells[5].text = "кв" if getattr(block, "kv", False) else ""

        # Цвет фона и префикс по типу блока
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
    """
    logger.info("[EXPORT_ALL] Начинается пакетный экспорт всех вариантов")

    export_dir.mkdir(parents=True, exist_ok=True)
    exported_files = []

    for i, arrangement in enumerate(arrangements, start=1):
        output_docx = export_dir / f"StageFlow_Variant_{i}_seed{arrangement.seed}.docx"
        output_json = export_dir / f"StageFlow_Variant_{i}_seed{arrangement.seed}.json"

        # DOCX
        export_arrangement(arrangement, template_path, output_docx)

        # JSON
        json_data = [
            {
                "id": b.id,
                "name": b.name,
                "type": b.type,
                "kv": b.kv,
                "fixed": b.fixed,
                "actors": [{"name": a.name, "tags": a.tags} for a in b.actors],
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


# ============================================================
# 🎨 Оформление строк
# ============================================================
def _apply_block_style(row, block: Block):
    """
    Минимальное оформление для типов блоков.
    Не трогает ширины, шрифты и стили — полностью сохраняет шаблон.
    """
    from docx.oxml import parse_xml
    from docx.oxml.ns import nsdecls

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
    """Добавляет текстовую метку в начало второй колонки."""
    cell = row.cells[1]
    current = cell.text.strip()
    cell.text = f"{label} {current}" if current else label


def _set_row_shading(row, color_hex: str):
    """Устанавливает цвет фона для всей строки без изменения структуры."""
    from docx.oxml import parse_xml
    from docx.oxml.ns import nsdecls

    for cell in row.cells:
        cell._element.get_or_add_tcPr().append(
            parse_xml(rf'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>')
        )
