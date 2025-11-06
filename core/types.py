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
        - "gk"     → гала-концерт.
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

    def actor_names(self) -> List[str]:
        """Возвращает список имён актёров (без дубликатов)."""
        return list({a.name for a in self.actors})

    def has_actor(self, name: str) -> bool:
        """Проверяет, есть ли актёр с указанным именем."""
        return any(a.name.lower() == name.lower() for a in self.actors)


# ============================================================
# 📋 Программа (вся последовательность блоков)
# ============================================================

@dataclass
class Program:
    """
    Вся программа концерта, состоящая из блоков.
    """
    blocks: List[Block] = field(default_factory=list)

    def get_actor_occurrences(self, name: str) -> List[int]:
        """Возвращает индексы блоков, где участвует указанный актёр."""
        indices = []
        for block in self.blocks:
            if block.has_actor(name):
                indices.append(block.id)
        return indices

    def __len__(self):
        return len(self.blocks)

    def __iter__(self):
        return iter(self.blocks)


# ============================================================
# 🧩 Результирующая перестановка
# ============================================================

@dataclass
class Arrangement:
    """Готовая перестроенная программа."""
    blocks: List[Block]
    seed: int
    score: float = 0.0
    fillers_count: int = 0
    strong_conflicts: int = 0
    weak_conflicts: int = 0


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
