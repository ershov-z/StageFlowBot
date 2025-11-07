# core/parser.py
from __future__ import annotations
from docx import Document
from pathlib import Path
import re
import json
from typing import List, Dict, Optional, Tuple
from loguru import logger
from core.types import Actor, Block, Program

# ============================================================
# 🎭 Загрузка списка актёров (как в v2, но оставляем)
# ============================================================

def _load_actor_names() -> set[str]:
    search_paths = [
        Path("/workspace/actors_list.json"),
        Path(__file__).resolve().parents[2] / "actors_list.json",
        Path(__file__).resolve().parents[1] / "actors_list.json",
        Path(__file__).resolve().parent / "actors_list.json",
        Path(__file__).resolve().parent / "data" / "actors_list.json",
    ]
    for path in search_paths:
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    names = {x.strip().lower() for x in json.load(f) if x.strip()}
                    logger.info(f"🎭 Загружено актёров: {len(names)} из {path}")
                    return names
            except Exception as e:
                logger.warning(f"⚠ Ошибка чтения {path}: {e}")
    logger.warning("⚠ actors_list.json не найден — fallback к базовому парсингу.")
    return set()

ACTOR_NAMES = _load_actor_names()

# ============================================================
# 🧩 Вспомогательные функции парсинга
# ============================================================

_SPLIT_RE = re.compile(r"[\n\r\u000b\u2028\u2029;,/\\]+")

def _split(blob: str) -> List[str]:
    if not blob:
        return []
    return [t.strip() for t in _SPLIT_RE.split(blob) if t.strip()]

def _clean_name(token: str) -> str:
    return re.sub(r"[%!\d.,]+", "", token).strip()

def _try_split_concatenated(token: str) -> List[str]:
    if not ACTOR_NAMES:
        return [token]
    low = token.lower()
    out, i = [], 0
    names_sorted = sorted(ACTOR_NAMES, key=len, reverse=True)
    while i < len(low):
        matched = False
        for name in names_sorted:
            if low.startswith(name, i):
                out.append(name)
                i += len(name)
                matched = True
                break
        if not matched:
            i += 1
    if len(out) > 1:
        return [s.capitalize() for s in out]
    return [token]

def _parse_actor_tokens(raw: str) -> List[Actor]:
    res: List[Actor] = []
    for tok in _split(raw):
        if not tok:
            continue
        tags = []
        name = tok
        if "%" in name:
            tags.append("later")
        if "!" in name:
            tags.append("early")
        if re.search(r"\(?\bг\s*к\b\)?", name, flags=re.IGNORECASE):
            tags.append("gk")
        name = re.sub(r"\(?\bг\s*к\b\)?", "", name, flags=re.IGNORECASE)
        name = _clean_name(name)
        for nm in _try_split_concatenated(name):
            nm = " ".join(nm.split())
            if nm:
                res.append(Actor(name=nm, tags=sorted(set(tags))))
    return res

def _merge_actors(main_list: List[Actor], pp_list: List[Actor]) -> List[Actor]:
    merged = {}
    for a in main_list:
        merged.setdefault(a.name, set()).update(a.tags)
    for a in pp_list:
        merged.setdefault(a.name, set()).update(a.tags)
    return [Actor(name=k, tags=sorted(v)) for k, v in merged.items()]

def _detect_type(title: str) -> str:
    t = (title or "").lower()
    if "[filler]" in t or "тянуч" in t:
        return "filler"
    if "предкулисье" in t:
        return "prelude"
    if "спонсор" in t or "sponsor" in t:
        return "sponsor"
    return "performance"

def _is_kv(cell_text: str) -> bool:
    return bool(re.search(r"\bкв\b", cell_text or "", flags=re.IGNORECASE))

# ============================================================
# 🗂️ Определение схемы колонок
# ============================================================

# Поддерживаем две схемы:
#  A) v2-старая: [0 №][1 Актёры][2 ПП][3 Найм][4 Ответств][5 Кв]        (6 колонок)
#  B) новая v1-стиль: [0 №][1 Название][2 Актёры][3 ПП][4 Найм][5 Отв][6 Кв] (7 колонок)

def _normalize_header(s: str) -> str:
    return (s or "").strip().lower()

def _guess_mapping_by_header(header_cells: List[str]) -> Optional[Dict[str, int]]:
    h = [_normalize_header(x) for x in header_cells]
    idx = {name: i for i, name in enumerate(h)}

    def find(*aliases) -> Optional[int]:
        for a in aliases:
            if a in idx:
                return idx[a]
        return None

    # Пытаемся распознать «новую» схему (наличие колонки «название»)
    title_i = find("название", "номер", "назв", "title")
    actors_i = find("актеры", "актёры", "участники", "actors")
    pp_i     = find("пп", "pp")
    hire_i   = find("наим", "найм", "hire")
    resp_i   = find("ответственный", "ответств", "responsible")
    kv_i     = find("кв", "kv")
    num_i    = find("№", "номер", "num", "#", "n")

    # Полная новая схема
    if title_i is not None and actors_i is not None and pp_i is not None and kv_i is not None:
        if num_i is None:
            num_i = 0  # чаще всего первая колонка
        if hire_i is None:
            hire_i = 4 if len(h) > 4 else None
        if resp_i is None:
            resp_i = 5 if len(h) > 5 else None
        return {"num": num_i, "title": title_i, "actors": actors_i, "pp": pp_i,
                "hire": hire_i, "resp": resp_i, "kv": kv_i}

    # Старая v2-схема (без «Название»)
    if actors_i is not None and pp_i is not None and kv_i is not None and title_i is None:
        num_i = num_i if num_i is not None else 0
        # Примем эвристику по позициям
        # 0 №, 1 Актёры, 2 ПП, 3 Найм, 4 Ответственный, 5 Кв
        return {"num": num_i, "title": None, "actors": actors_i, "pp": pp_i,
                "hire": 3 if len(h) > 3 else None,
                "resp": 4 if len(h) > 4 else None,
                "kv": kv_i}

    return None

def _fallback_mapping_by_count(n_cols: int) -> Dict[str, int | None]:
    if n_cols >= 7:
        # Новая схема по умолчанию
        return {"num": 0, "title": 1, "actors": 2, "pp": 3, "hire": 4, "resp": 5, "kv": 6}
    # Старая схема
    return {"num": 0, "title": None, "actors": 1, "pp": 2, "hire": 3 if n_cols > 3 else None,
            "resp": 4 if n_cols > 4 else None, "kv": 5 if n_cols > 5 else None}

# ============================================================
# 📘 Основной парсер
# ============================================================

def parse_docx(path: str) -> Program:
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)
    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return Program(blocks=[])

    table = doc.tables[0]
    rows = table.rows
    if not rows:
        logger.error("❌ Первая таблица пуста.")
        return Program(blocks=[])

    # Определяем маппинг колонок
    header_cells = [c.text for c in rows[0].cells]
    mapping = _guess_mapping_by_header(header_cells)
    if mapping is None:
        mapping = _fallback_mapping_by_count(len(rows[0].cells))
        logger.warning("⚠ Заголовки не распознаны однозначно — используем эвристику по позициям.")

    def get(cells: List[str], key: str) -> str:
        i = mapping.get(key)
        return cells[i].strip() if (i is not None and i < len(cells)) else ""

    blocks: List[Block] = []
    next_id = 1

    for row in rows[1:]:
        cells = [c.text or "" for c in row.cells]
        if not any(x.strip() for x in cells):
            continue

        num_raw = get(cells, "num")
        title   = get(cells, "title")
        actors_raw = get(cells, "actors")
        pp_raw     = get(cells, "pp")
        hire       = get(cells, "hire")
        resp       = get(cells, "resp")
        kv_raw     = get(cells, "kv")

        # Если колонки "Название" нет (старая схема) — пытаемся вывести его из других ячеек.
        # В v1 оно было отдельным, но если его нет — оставим пустым (экспортер потом заполнит).
        if not title:
            # мягкая эвристика: если в actors_raw явно «[filler] …»/«Предкулисье»/«Спонсоры», используем это как title
            maybe_title = actors_raw.strip()
            lowered = maybe_title.lower()
            if any(x in lowered for x in ("[filler]", "тянуч", "предкулисье", "спонсор", "sponsor")):
                title = maybe_title

        main_actors = _parse_actor_tokens(actors_raw)
        pp_actors   = _parse_actor_tokens(pp_raw)
        actors      = _merge_actors(main_actors, pp_actors)

        block_type = _detect_type(title or actors_raw)
        kv = _is_kv(kv_raw)

        blocks.append(Block(
            id=next_id,
            name=title or f"Блок {next_id}",
            type=block_type,
            actors=actors,
            kv=kv,
            fixed=(block_type in {"prelude", "sponsor"}),
            # --- Новые поля «v1-стиля» (см. types.py) ---
            num=num_raw or "",
            actors_raw=actors_raw or "",
            pp_raw=pp_raw or "",
            hire=hire or "",
            responsible=resp or "",
        ))
        next_id += 1

    # Фиксация первых двух и последних двух performance-блоков (как в требованиях)
    perf_indices = [i for i, b in enumerate(blocks) if b.type == "performance"]
    if len(perf_indices) >= 1:
        blocks[perf_indices[0]].fixed = True
    if len(perf_indices) >= 2:
        blocks[perf_indices[1]].fixed = True
    if len(perf_indices) >= 4:
        blocks[perf_indices[-2]].fixed = True
    if len(perf_indices) >= 3:
        blocks[perf_indices[-1]].fixed = True

    logger.info(f"✅ Прочитано блоков: {len(blocks)} | performance={len(perf_indices)}")
    return Program(blocks=blocks)
