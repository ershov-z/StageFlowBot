from docx import Document
from loguru import logger
import re

# ——— 1) извлекаем текст с реальными переводами строк из ячейки docx-таблицы ———
def _extract_text_with_breaks(cell):
    ns = cell._element.nsmap
    lines = []
    # каждый абзац как строка
    for p in cell._element.xpath(".//w:p", namespaces=ns):
        buf = []
        for r in p.xpath(".//w:r", namespaces=ns):
            # текстовые куски
            for t in r.xpath(".//w:t", namespaces=ns):
                if t.text:
                    buf.append(t.text)
            # «мягкие» переносы <w:br/> превращаем в \n
            if r.xpath(".//w:br", namespaces=ns):
                buf.append("\n")
        line = "".join(buf).strip()
        lines.append(line)
    # абзац — это тоже перенос
    text = "\n".join([ln for ln in lines if ln])
    # нормализуем редкие разделители из Word (на всякий случай)
    text = text.replace("\r", "\n")
    return text.strip()

# ——— 2) надёжный сплиттер — видит все типы «строковых» разделителей ———
_SPLIT_RE = re.compile(r"[\n\r\u000b\u2028\u2029;,/\\]+")

def _split_people_blob(blob: str) -> list[str]:
    if not blob:
        return []
    parts = [p.strip() for p in _SPLIT_RE.split(blob) if p.strip()]
    return parts

def parse_actors(raw: str) -> list[dict]:
    """
    теги:
      %  -> 'later'
      !  -> 'early'
      (гк) -> 'gk' (приоритет)
    может быть несколько тегов, порядок не важен.
    """
    if not raw:
        return []
    result: list[dict] = []
    for token in _split_people_blob(raw):
        name = token.strip()
        tags = set()

        lname = name.lower()
        if "(гк)" in lname or "(г к)" in lname:
            tags.add("gk")
            name = (name.replace("(гк)", "").replace("(ГК)", "").replace("(г к)", "")).strip()

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
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)
    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return []

    table = doc.tables[0]
    rows = table.rows
    if len(rows) < 2:
        logger.error("❌ Таблица пустая или без данных.")
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
            "actors_raw": actors_raw,   # исходник с тегами — сохраняем как есть
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

        # КЛЮЧЕВОЕ: строим структурный список актёров из actors_raw
        entry["actors"] = parse_actors(actors_raw)

        data.append(entry)

    logger.info(f"✅ Прочитано {len(data)} строк.")
    return data

# ===== УДАЛИТЬ: локальный тест =====
if __name__ == "__main__":
    import json
    sample = "Ксюша\nИсаев\x0bБрекоткин\u2028Ярица\u2029Соколов,Илана;Попов/(гк)!%%"
    print(_split_people_blob(sample))
    print(parse_actors(sample))
