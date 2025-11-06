from docx import Document
from pathlib import Path
import re
import json
from loguru import logger
from core.types import Actor, Block, Program

# ============================================================
# 🎭 Загрузка списка актёров
# ============================================================

def _load_actor_names() -> set[str]:
    """Пробует найти actors_list.json в нескольких стандартных местах"""
    search_paths = [
        Path(__file__).resolve().parent / "actors_list.json",
        Path(__file__).resolve().parent / "data" / "actors_list.json",
        Path(__file__).resolve().parents[1] / "data" / "actors_list.json",
        Path(__file__).resolve().parents[1] / "actors_list.json",
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


def _split_people_blob(blob: str) -> list[str]:
    """Разделяет строку актёров на отдельные токены"""
    if not blob:
        return []
    return [p.strip() for p in _SPLIT_RE.split(blob) if p.strip()]


def _clean_actor_token(token: str) -> str:
    """Удаляет мусорные символы и подготавливает имя"""
    return re.sub(r"[%!\d.,]+", "", token).strip()


def _try_split_concatenated(token: str) -> list[str]:
    """Разбивает склеенные имена (например 'ИланаКсюша') по словарю известных актёров"""
    if not ACTOR_NAMES:
        return [token]
    low = token.lower()
    found = []
    i = 0
    while i < len(low):
        match = None
        for name in sorted(ACTOR_NAMES, key=len, reverse=True):
            if low.startswith(name, i):
                found.append(name)
                i += len(name)
                match = True
                break
        if not match:
            i += 1
    if len(found) > 1:
        return [n.capitalize() for n in found]
    return [token]


def parse_actors(raw: str) -> list[Actor]:
    """Парсит список актёров и их теги"""
    if not raw:
        return []

    result: list[Actor] = []
    for token in _split_people_blob(raw):
        if not token.strip():
            continue

        tags = set()
        name = token.strip()

        # Определяем теги
        if "%" in name:
            tags.add("later")
        if "!" in name:
            tags.add("early")
        if re.search(r"\(?\bг\s*к\b\)?", name, flags=re.IGNORECASE):
            tags.add("gk")

        # Чистим имя
        name = re.sub(r"\(?\bг\s*к\b\)?", "", name, flags=re.IGNORECASE)
        name = _clean_actor_token(name)

        # Разбиваем склеенные имена
        for nm in _try_split_concatenated(name):
            nm = " ".join(nm.split())
            if nm:
                result.append(Actor(name=nm, tags=sorted(list(tags))))

    return result

# ============================================================
# 📘 Основной парсер программы
# ============================================================

def parse_docx(path: str) -> Program:
    """Читает таблицу .docx и возвращает Program"""
    logger.info(f"📄 Чтение документа: {path}")
    doc = Document(path)
    if not doc.tables:
        logger.error("❌ В документе нет таблиц.")
        return Program(blocks=[])

    table = doc.tables[0]
    blocks: list[Block] = []

    for i, row in enumerate(table.rows[1:], start=1):
        texts = [cell.text.strip() for cell in row.cells]
        if not any(texts):
            continue

        num = texts[0] if len(texts) > 0 else ""
        title = texts[1] if len(texts) > 1 else ""
        actors_raw = texts[2] if len(texts) > 2 else ""
        pp_raw = texts[3] if len(texts) > 3 else ""

        main_actors = parse_actors(actors_raw)
        pp_actors = parse_actors(pp_raw)

        # Сливаем актёров и их теги
        merged = {a.name: set(a.tags) for a in main_actors}
        for pa in pp_actors:
            merged.setdefault(pa.name, set()).update(pa.tags)

        actors = [Actor(name=k, tags=sorted(v)) for k, v in merged.items()]

        # Определяем тип блока
        block_type = "обычный"
        lt = title.lower()
        if "предкулисье" in lt:
            block_type = "предкулисье"
        elif "спонсор" in lt:
            block_type = "спонсоры"
        elif "тянуч" in lt:
            block_type = "тянучка"

        block = Block(
            index=i,
            pp=pp_raw,
            actors=actors,
            description=title,
            type=block_type
        )
        blocks.append(block)

    logger.info(f"✅ Прочитано блоков: {len(blocks)}")
    return Program(blocks=blocks)
