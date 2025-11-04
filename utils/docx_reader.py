from docx import Document
from loguru import logger


def _extract_text_with_breaks(cell):
    """
    Извлекает текст из ячейки таблицы, включая символы перевода строки (<w:br/>).
    Это критично для корректного разделения актёров.
    """
    lines = []
    for paragraph in cell.paragraphs:
        text = ""
        for run in paragraph.runs:
            text += run.text
            # Добавляем перенос, если в run есть <w:br/> (внутренние переносы в docx)
            if run._element.xpath(".//w:br"):
                text += "\n"
        lines.append(text)
    return "\n".join(lines).strip()


def _split_people_blob(blob: str) -> list[str]:
    """
    Надёжно разбиваем строку актёров: поддерживаем \\n, ',', ';', '/', '\\'.
    Ничего не делим по пробелам, чтобы не ломать фразы типа 'Женщина 50+'.
    """
    if not blob:
        return []
    raw = (
        blob.replace("\r", "\n")
        .replace(";", "\n")
        .replace("/", "\n")
        .replace("\\", "\n")
    )
    out: list[str] = []
    for line in raw.split("\n"):
        for piece in line.split(","):
            piece = piece.strip()
            if piece:
                out.append(piece)
    return out


def parse_actors(raw: str) -> list[dict]:
    """
    Разбирает строку актёров и выделяет теги:
      %  -> 'later'
      !  -> 'early'
      (гк) -> 'gk'  (приоритетно)
    Тегов может быть несколько (например 'Брекоткин%%!').
    """
    if not raw:
        return []

    result: list[dict] = []
    for token in _split_people_blob(raw):
        name = token.strip()
        tags: set[str] = set()

        lname = name.lower()
        if "(гк)" in lname or "(г к)" in lname:
            tags.add("gk")
            name = (
                name.replace("(гк)", "")
                .replace("(ГК)", "")
                .replace("(г к)", "")
                .strip()
            )

        if "%" in name:
            tags.add("later")
            name = name.replace("%", "").strip()
        if "!" in name:
            tags.add("early")
            name = name.replace("!", "").strip()

        name = " ".join(name.split())
        if not name:
            continue

        result.append({"name": name, "tags": list(tags)})

    return result


def read_program(path: str):
    """
    Считывает первую таблицу из .docx и возвращает список номеров.
    """
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)

    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return []

    table = doc.tables[0]
    data = []

    rows = table.rows
    if not rows or len(rows) < 2:
        logger.error("❌ Таблица пустая или без данных.")
        return []

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
        kv_cell = texts[6] if len(texts) > 6 else ""
        kv = "кв" in kv_cell.lower()

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

        t = title.lower()
        if "предкулисье" in t:
            entry["type"] = "предкулисье"
        elif "спонсор" in t:
            entry["type"] = "спонсоры"
        elif "тянуч" in t:
            entry["type"] = "тянучка"

        entry["actors"] = parse_actors(actors_raw)
        data.append(entry)

    logger.info(f"✅ Прочитано {len(data)} строк.")
    return data


# ==== УДАЛИТЬ (локальная проверка) ====
if __name__ == "__main__":
    import json
    res = read_program("data/sample.docx")
    print(json.dumps(res, indent=2, ensure_ascii=False))
# ==== УДАЛИТЬ ====
