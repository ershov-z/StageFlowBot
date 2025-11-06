# core/parser.py
from __future__ import annotations
from docx import Document
from pathlib import Path
import re
import json
from typing import List
from loguru import logger
from core.types import Actor, Block, Program


# ============================================================
# 🎭 Загрузка списка актёров
# ============================================================

def _load_actor_names() -> set[str]:
    """Пробует найти actors_list.json в корне проекта или стандартных местах."""
    search_paths = [
        Path("/workspace/actors_list.json"),  # 🟢 корень проекта (Koyeb / локальный)
        Path(__file__).resolve().parents[2] / "actors_list.json",  # ./actors_list.json на уровень выше core/
        Path(__file__).resolve().parents[1] / "actors_list.json",  # ../actors_list.json
        Path(__file__).resolve().parent / "actors_list.json",      # core/actors_list.json
        Path(__file__).resolve().parent / "data" / "actors_list.json",  # core/data/actors_list.json
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
# 🧩 Вспомогательные функции
# ============================================================

_SPLIT_RE = re.compile(r"[\n\r\u000b\u2028\u2029;,/\\]+")


def _split(blob: str) -> List[str]:
    if not blob:
        return []
    return [t.strip() for t in _SPLIT_RE.split(blob) if t.strip()]


def _clean_name(token: str) -> str:
    return re.sub(r"[%!\d.,]+", "", token).strip()


def _try_split_concatenated(token: str) -> List[str]:
    """Разбивает склеенные имена (например 'ИланаКсюша') по известным актёрам."""
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
    """Парсит строку с актёрами и извлекает теги (% ! гк)."""
    res: List[Actor] = []
    for tok in _split(raw):
        if not tok:
            continue

        tags = []
        name = tok

        # Определяем теги
        if "%" in name:
            tags.append("later")
        if "!" in name:
            tags.append("early")
        if re.search(r"\(?\bг\s*к\b\)?", name, flags=re.IGNORECASE):
            tags.append("gk")

        # Чистим имя
        name = re.sub(r"\(?\bг\s*к\b\)?", "", name, flags=re.IGNORECASE)
        name = _clean_name(name)

        for nm in _try_split_concatenated(name):
            nm = " ".join(nm.split())
            if nm:
                res.append(Actor(name=nm, tags=sorted(set(tags))))
    return res


def _merge_actors(main_list: List[Actor], pp_list: List[Actor]) -> List[Actor]:
    """Объединяет основную и ПП колонки актёров, слияние тегов."""
    merged = {}
    for a in main_list:
        merged.setdefault(a.name, set()).update(a.tags)
    for a in pp_list:
        merged.setdefault(a.name, set()).update(a.tags)
    return [Actor(name=k, tags=sorted(v)) for k, v in merged.items()]


def _detect_type(title: str) -> str:
    """Определяет тип блока по названию."""
    t = (title or "").lower()
    if "[filler]" in t or "тянуч" in t:
        return "filler"
    if "предкулисье" in t:
        return "prelude"
    if "спонсор" in t or "sponsor" in t:
        return "sponsor"
    return "performance"


# ============================================================
# 📘 Основной парсер программы
# ============================================================

def parse_docx(path: str) -> Program:
    """Читает .docx таблицу и возвращает структуру Program."""
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)
    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return Program(blocks=[])

    table = doc.tables[0]
    blocks: List[Block] = []
    next_id = 1

    # Ожидаем формат таблицы:
    # 0 — №
    # 1 — Актёры
    # 2 — ПП
    # 3 — Найм (игнор)
    # 4 — Ответственный (игнор)
    # 5 — kv
    for row in table.rows[1:]:
        cells = [c.text.strip() for c in row.cells]
        if not any(cells):
            continue

        title = cells[1] if len(cells) > 1 else ""
        actors_raw = cells[1] if len(cells) > 1 else ""
        pp_raw = cells[2] if len(cells) > 2 else ""
        kv_raw = cells[5] if len(cells) > 5 else ""

        main_actors = _parse_actor_tokens(actors_raw)
        pp_actors = _parse_actor_tokens(pp_raw)
        actors = _merge_actors(main_actors, pp_actors)

        block_type = _detect_type(title)
        kv = bool(re.search(r"\bкв\b", kv_raw, flags=re.IGNORECASE))

        blocks.append(Block(
            id=next_id,
            name=title or f"Блок {next_id}",
            type=block_type,
            actors=actors,
            kv=kv,
            fixed=(block_type in {"prelude", "sponsor"})
        ))
        next_id += 1

    # Фиксируем первые два и последние два performance-блока
    perf_indices = [i for i, b in enumerate(blocks) if b.type == "performance"]
    if len(perf_indices) >= 1:
        blocks[perf_indices[0]].fixed = True
    if len(perf_indices) >= 2:
        blocks[perf_indices[1]].fixed = True
    if len(perf_indices) >= 3:
        blocks[perf_indices[-1]].fixed = True
    if len(perf_indices) >= 4:
        blocks[perf_indices[-2]].fixed = True

    logger.info(f"✅ Прочитано блоков: {len(blocks)} | performance={len(perf_indices)}")
    return Program(blocks=blocks)
