from docx import Document
from loguru import logger


def _split_people_blob(blob: str) -> list[str]:
    """
    Надёжно разбиваем список актёров: поддерживаем \\n, ',', ';', '/', '\\'.
    Ничего не делим по обычному пробелу, чтобы не ломать имена вроде 'Женщина 50+'.
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
    Разбивает строку актёров и выделяет теги:
      %  -> 'later'
      !  -> 'early'
      (гк) -> 'gk'  (приоритетно)
    Тегов может быть несколько (напр. 'Брекоткин%%!'), мы их все учитываем.
    Важно: исходная строка с тегами сохраняется в поле actors_raw на уровне записи — мы тут формируем только структурированное представление.
    """
    if not raw:
        return []

    result: list[dict] = []
    for token in _split_people_blob(raw):
        name = token.strip()
        tags: set[str] = set()

        # (гк) — приоритетно, убираем любые регистры/варианты
        lname = name.lower()
        if "(гк)" in lname or "(г к)" in lname:
            tags.add("gk")
            name = (
                name.replace("(гк)", "")
                .replace("(ГК)", "")
                .replace("(г к)", "")
                .strip()
            )

        # поддерживаем множественные % и !
        if "%" in name:
            tags.add("later")
            name = name.replace("%", "").strip()
        if "!" in name:
            tags.add("early")
            name = name.replace("!", "").strip()

        # финальная чистка лишних пробелов
        name = " ".join(name.split())
        if not name:
            continue

        result.append({"name": name, "tags": list(tags)})

    return result


def read_program(path: str):
    """
    Считывает ПЕРВУЮ таблицу из .docx и возвращает список номеров (dict на строку).
    Сохраняем исходные поля как есть, плюс структурируем актёров в поле 'actors'.
    Колонки ожидаются в порядке:
      0: № (num), 1: Номер (title), 2: Актеры (actors_raw), 3: ПП (pp),
      4: найм (hire), 5: ответственный (responsible), 6: КВ (kv — ячейка может содержать 'кв')
    """
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)

    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return []

    table = doc.tables[0]
    data = []

    # заголовки нам не обязательны, читаем по индексам
    rows = table.rows
    if not rows or len(rows) < 2:
        logger.error("❌ Таблица пустая или без данных.")
        return []

    for i, row in enumerate(rows[1:], start=1):
        cells = [c.text.strip() for c in row.cells]
        if not any(cells):
            continue

        num = cells[0] if len(cells) > 0 else ""
        title = cells[1] if len(cells) > 1 else ""
        actors_raw = cells[2] if len(cells) > 2 else ""
        pp = cells[3] if len(cells) > 3 else ""
        hire = cells[4] if len(cells) > 4 else ""
        responsible = cells[5] if len(cells) > 5 else ""
        kv_cell = cells[6] if len(cells) > 6 else ""
        kv = "кв" in kv_cell.lower()

        entry = {
            "order": i,
            "num": num,
            "title": title,
            "actors_raw": actors_raw,      # исходная строка с тегами — сохраняем
            "pp": pp,                      # сохраняем как есть
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

        # структурированные актёры для логики валидатора/сборщика
        entry["actors"] = parse_actors(actors_raw)

        data.append(entry)

    logger.info(f"✅ Прочитано {len(data)} строк.")
    return data


# ==== УДАЛИТЬ (локальная проверка) ====
if __name__ == "__main__":
    import json
    test_path = "data/Сетка,_Берегись_Ретромобиля,_7_марта,_пятница.docx"
    res = read_program(test_path)
    def _safe(o):
        if isinstance(o, set):
            return list(o)
        raise TypeError()
    print(json.dumps(res, indent=2, ensure_ascii=False, default=_safe))
# ==== УДАЛИТЬ ====
