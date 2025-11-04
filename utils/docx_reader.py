from docx import Document
from loguru import logger


def read_program(path: str):
    """
    Считывает первую таблицу в .docx и возвращает список номеров.
    Каждая строка -> dict со всеми колонками и метаданными.
    """
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)

    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return []

    table = doc.tables[0]
    data = []
    headers = [cell.text.strip().lower() for cell in table.rows[0].cells]

    # Столбцы (предполагаем): №, Номер, Актеры, ПП, Найм, Ответственный, КВ
    for i, row in enumerate(table.rows[1:], start=1):
        cells = [cell.text.strip() for cell in row.cells]
        if not any(cells):
            continue  # пропускаем полностью пустые строки

        entry = {
            "order": i,
            "num": cells[0] if len(cells) > 0 else "",
            "title": cells[1] if len(cells) > 1 else "",
            "actors_raw": cells[2] if len(cells) > 2 else "",
            "pp": cells[3] if len(cells) > 3 else "",
            "hire": cells[4] if len(cells) > 4 else "",
            "responsible": cells[5] if len(cells) > 5 else "",
            "kv": "кв" in cells[6].lower() if len(cells) > 6 else False,
        }

        # Определяем тип номера
        t = entry["title"].lower()
        if "предкулисье" in t:
            entry["type"] = "предкулисье"
        elif "спонсор" in t:
            entry["type"] = "спонсоры"
        elif "тянуч" in t:
            entry["type"] = "тянучка"
        else:
            entry["type"] = "обычный"

        # Разбор актёров
        entry["actors"] = parse_actors(entry["actors_raw"])
        data.append(entry)

    logger.info(f"✅ Прочитано {len(data)} строк.")
    return data


def parse_actors(raw: str):
    """
    Разбивает строку актёров, выделяет теги:
    % — появляется позже
    ! — уходит раньше
    (гк) — грим/костюм
    """
    if not raw:
        return []

    result = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue

        tags = set()
        name = part

        if "%" in name:
            tags.add("later")
            name = name.replace("%", "").strip()
        if "!" in name:
            tags.add("early")
            name = name.replace("!", "").strip()
        if "(гк)" in name.lower():
            tags.add("gk")
            name = name.replace("(гк)", "").strip()

        result.append({"name": name, "tags": tags})
    return result


# ==== УДАЛИТЬ (тест локального чтения) ====
if __name__ == "__main__":
    import json
    test_path = "data/Сетка,_Берегись_Ретромобиля,_7_марта,_пятница.docx"
    result = read_program(test_path)
    print(json.dumps(result, indent=2, ensure_ascii=False))
# ==== УДАЛИТЬ ====
