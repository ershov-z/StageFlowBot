from dataclasses import dataclass, field
from typing import List


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
        # Позволяет использовать Actor в set() и dict()
        return hash(self.name.lower())

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags


# ============================================================
# 🎬 Блок программы (один номер)
# ============================================================

@dataclass
class Block:
    """
    Один номер программы концерта.
    index        — порядковый номер;
    pp           — строка из колонки "ПП";
    actors       — список актёров;
    description  — описание / название номера;
    type         — тип блока:
        "обычный", "предкулисье", "спонсоры", "тянучка".
    """
    index: int
    pp: str
    actors: List[Actor] = field(default_factory=list)
    description: str = ""
    type: str = "обычный"

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
                indices.append(block.index)
        return indices

    def __len__(self):
        return len(self.blocks)

    def __iter__(self):
        return iter(self.blocks)
