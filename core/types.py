from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Literal, Optional


# ============================================================
# 🎭 Актёр
# ============================================================

@dataclass
class Actor:
    """
    Представляет участника выступления.
    name  — имя актёра;
    tags  — метки:
        - "early"  → должен быть раньше;
        - "later"  → должен быть позже;
        - "vo"     → закадровое участие (озвучка);
        - "gk"     → устаревший тег (гала-концерт), не используется,
                     но может встречаться в данных и игнорируется.
    """
    name: str
    tags: List[str] = field(default_factory=list)

    def __hash__(self):
        return hash(self.name.lower())

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags


# ============================================================
# 🎬 Блок программы (один номер)
# ============================================================

@dataclass
class Block:
    """
    Один элемент программы (номер, тянучка, предкулисье, спонсор).
    """
    id: int
    name: str = ""
    type: Literal["performance", "filler", "sponsor", "prelude"] = "performance"
    actors: List[Actor] = field(default_factory=list)
    kv: bool = False
    fixed: bool = False
    meta: Optional[dict] = None

    # --- новые поля для «v1-стиля» ---
    num: str = ""                # колонка № (нумерация)
    actors_raw: str = ""         # исходная строка «Актеры»
    pp_raw: str = ""             # исходная строка «ПП»
    hire: str = ""               # колонка «Найм»
    responsible: str = ""        # колонка «Ответственный»

    def __hash__(self):
        return hash(self.id)

    def short(self) -> str:
        """Короткое описание блока для логов."""
        return f"[{self.id}:{self.type}] {self.name}"

    def actor_names(self) -> List[str]:
        """Возвращает список имён актёров (без дубликатов)."""
        return list({a.name for a in self.actors})

    def has_actor(self, name: str) -> bool:
        """Проверяет, есть ли актёр с указанным именем."""
        return any(a.name.lower() == name.lower() for a in self.actors)

    def to_dict(self) -> dict:
        """Сериализует блок в словарь для экспорта."""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "actors": [
                {
                    "name": actor.name,
                    "tags": list(actor.tags),
                }
                for actor in self.actors
            ],
            "kv": self.kv,
            "fixed": self.fixed,
            "meta": self.meta or {},
            "num": self.num,
            "actors_raw": self.actors_raw,
            "pp_raw": self.pp_raw,
            "hire": self.hire,
            "responsible": self.responsible,
        }


# ============================================================
# 📋 Программа (вся последовательность блоков)
# ============================================================

@dataclass
class Program:
    """Вся программа концерта, состоящая из блоков."""
    blocks: List[Block] = field(default_factory=list)

    def __len__(self):
        return len(self.blocks)

    def __iter__(self):
        return iter(self.blocks)

    def get_performances(self) -> List[Block]:
        """Возвращает только номера (type=performance)."""
        return [b for b in self.blocks if b.type == "performance"]

    def get_fillers(self) -> List[Block]:
        """Возвращает только тянучки."""
        return [b for b in self.blocks if b.type == "filler"]

    def get_actor_occurrences(self, name: str) -> List[int]:
        """Возвращает индексы блоков, где участвует указанный актёр."""
        indices = []
        for block in self.blocks:
            if block.has_actor(name):
                indices.append(block.id)
        return indices


# ============================================================
# 🧩 Результирующая перестановка
# ============================================================

@dataclass
class Arrangement:
    """Готовая перестроенная программа."""
    seed: int
    blocks: List[Block] = field(default_factory=list)
    fillers_used: int = 0
    strong_conflicts: int = 0
    weak_conflicts: int = 0
    meta: Optional[dict] = None  # ← добавлено поле для статусов/служебных данных

    def __len__(self):
        return len(self.blocks)

    def __iter__(self):
        return iter(self.blocks)


# ============================================================
# ⚔️ Конфликт и тянучки
# ============================================================

@dataclass
class Conflict:
    """Описание конфликта между двумя блоками."""
    index_a: int
    index_b: int
    type: Literal["weak", "strong"]
    reason: str


@dataclass
class FillerCandidate:
    """Кандидат на тянучку между двумя номерами."""
    prev_block: Block
    next_block: Block
    actor_name: str
    valid: bool
