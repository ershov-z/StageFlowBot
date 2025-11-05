from docx import Document
from loguru import logger
import re

def _extract_text_with_breaks(cell):
    """Извлекает текст из ячейки Word с сохранением переводов строк"""
    try:
        # Извлекаем все абзацы <w:p> и <w:br> внутри ячейки
        lines = []
        for p in cell._element.xpath(".//w:p"):
            buf = []
            for r in p.xpath(".//w:r"):
                for t in r.xpath(".//w:t"):
                    if t.text:
                        buf.append(t.text)
                # перенос строки <w:br>
                if r.xpath(".//w:br"):
                    buf.append("\n")
            line = "".join(buf).strip()
            if line:
                lines.append(line)
        text = "\n".join(lines)
        text = text.replace("\r", "\n").strip()
        return text
    except Exception as e:
        logger.warning(f"⚠ Ошибка извлечения текста из ячейки: {e}")
        return cell.text.strip() if cell.text else ""


# Универсальный сплиттер
_SPLIT_RE = re.compile(r"[\n\r\u000b\u2028\u2029;,/\\]+")

def _split_people_blob(blob: str) -> list[str]:
    if not blob:
        return []
    parts = [p.strip() for p in _SPLIT_RE.split(blob) if p.strip()]
    return parts


def parse_actors(raw: str) -> list[dict]:
    """
    Парсинг списка актёров с тегами:
      %  → later
      !  → early
      (гк) → gk (приоритет)
    """
    if not raw:
        return []
    result = []
    for token in _split_people_blob(raw):
        name = token.strip()
        tags = set()

        lname = name.lower()
        if "(гк)" in lname or "(г к)" in lname:
            tags.add("gk")
            name = name.replace("(гк)", "").replace("(ГК)", "").replace("(г к)", "").strip()

        if "%" in name:
            tags.add("later")
            name = name.replace("%", "").strip()
        if "!" in name:
            tags.add("early")
            name = name.replace("!", "").strip()

        name = " ".join(name.split())
        if name:
            result.append({"name": name, "tags": list(tags)})
    return result


def read_program(path: str):
    """Читает первую таблицу из DOCX и возвращает структурированные данные"""
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)
    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return []

    table = doc.tables[0]
    rows = table.rows
    if len(rows) < 2:
        logger.error("❌ Таблица пуста.")
        return []

    data = []
    for i, row in enumerate(rows[1:], start=1):
        cells = row.cells
        texts = [_extract_text_with_breaks(c) for c in cells]
        if not any(texts):
            continue

        num = texts[0] if len(texts) > 0 else ""
        title = texts[1] if len(texts) > 1 else ""
        actors_raw = texts[2] if len(texts) > 2 else ""
        pp = texts[3] if len(texts) > 3 else ""
        hire = texts[4] if len(texts) > 4 else ""
        responsible = texts[5] if len(texts) > 5 else ""
        kv = "кв" in (texts[6].lower() if len(texts) > 6 and texts[6] else "")

        entry = {
            "order": i,
            "num": num,
            "title": title,
            "actors_raw": actors_raw,
            "pp": pp,
            "hire": hire,
            "responsible": responsible,
            "kv": kv,
            "type": "обычный",
        }

        lower_title = title.lower()
        if "предкулисье" in lower_title:
            entry["type"] = "предкулисье"
        elif "спонсор" in lower_title:
            entry["type"] = "спонсоры"
        elif "тянуч" in lower_title:
            entry["type"] = "тянучка"

        entry["actors"] = parse_actors(actors_raw)
        data.append(entry)

    logger.info(f"✅ Прочитано {len(data)} строк.")
    return data


# --- УДАЛИТЬ после теста ---
if __name__ == "__main__":
    import json
    test_str = "Ксюша!(гк)\nИсаев%\nБрекоткин%%!\nЯрица\nСоколов"
    print(json.dumps(parse_actors(test_str), ensure_ascii=False, indent=2))
