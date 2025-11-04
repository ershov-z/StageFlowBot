from docx import Document
from loguru import logger

# === Вспомогательная функция для полного извлечения текста с \n из ячейки ===
def _extract_text_with_real_breaks(cell):
    """
    Извлекает текст из ячейки таблицы, включая <w:br/> и <w:p>.
    Это гарантирует сохранение переводов строк.
    """
    texts = []
    for paragraph in cell._element.xpath(".//w:p", namespaces=cell._element.nsmap):
        line = ""
        for node in paragraph.xpath(".//w:r", namespaces=cell._element.nsmap):
            # Добавляем текст
            for t in node.xpath(".//w:t", namespaces=cell._element.nsmap):
                line += t.text or ""
            # Добавляем перенос при наличии <w:br/>
            if node.xpath(".//w:br", namespaces=cell._element.nsmap):
                line += "\n"
        texts.append(line.strip())
    return "\n".join([t for t in texts if t]).strip()


def _split_people_blob(blob: str) -> list[str]:
    """Разбиваем строку актёров на имена."""
    if not blob:
        return []
    raw = (
        blob.replace("\r", "\n")
        .replace(";", "\n")
        .replace("/", "\n")
        .replace("\\", "\n")
    )
    result = []
    for line in raw.split("\n"):
        for piece in line.split(","):
            piece = piece.strip()
            if piece:
                result.append(piece)
    return result


def parse_actors(raw: str) -> list[dict]:
    """
    Разбирает строку актёров и теги:
      % → later
      ! → early
      (гк) → gk
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
    """Парсит таблицу программы из docx."""
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)

    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return []

    table = doc.tables[0]
    data = []

    for i, row in enumerate(table.rows[1:], start=1):
        cells = row.cells
        texts = [_extract_text_with_real_breaks(c) for c in cells]
        if not any(texts):
            continue

        num = texts[0] if len(texts) > 0 else ""
        title = texts[1] if len(texts) > 1 else ""
        actors_raw = texts[2] if len(texts) > 2 else ""
        pp = texts[3] if len(texts) > 3 else ""
        hire = texts[4] if len(texts) > 4 else ""
        responsible = texts[5] if len(texts) > 5 else ""
        kv = False
        if len(texts) > 6:
            kv = "кв" in texts[6].lower()

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
    path = "data/sample.docx"
    res = read_program(path)
    print(json.dumps(res, indent=2, ensure_ascii=False))
# ==== УДАЛИТЬ ====
