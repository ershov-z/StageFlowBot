from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple


# ====== Результат проверки ======
@dataclass
class CheckResult:
    ok: bool
    reasons: List[str]

    def __bool__(self) -> bool:
        return self.ok


# ====== Вспомогательные функции ======
def _split_people_blob(blob: str) -> List[str]:
    """
    Разбивает строку актёров на отдельные имена.
    Поддерживает разделители: перевод строки, запятая, точка с запятой, /, \\
    """
    if not blob:
        return []
    raw = (
        blob.replace("\r", "\n")
        .replace(";", "\n")
        .replace("/", "\n")
        .replace("\\", "\n")
    )
    parts = []
    for line in raw.split("\n"):
        for piece in line.split(","):
            piece = piece.strip()
            if piece:
                parts.append(piece)
    return parts


def _parse_actor_token(token: str) -> Tuple[str, Set[str]]:
    """
    Разбирает теги внутри имени: %, !, (гк)
    Возвращает (чистое имя, набор тегов: {'later','early','gk'})
    """
    name = token.strip()
    tags: Set[str] = set()

    # 💄 ищем все вхождения тегов, даже множественные
    # (гк) имеет приоритет — если он есть, то он главный
    if "(гк)" in name.lower() or "(г к)" in name.lower():
        tags.add("gk")
        name = name.replace("(гк)", "").replace("(ГК)", "").replace("(г к)", "").strip()

    # далее разбираем % и !
    if "%" in name:
        tags.add("later")
        name = name.replace("%", "").strip()

    if "!" in name:
        tags.add("early")
        name = name.replace("!", "").strip()

    # если встречались несколько %, это не ошибка — просто повторное подтверждение
    # финально чистим лишние пробелы
    name = " ".join(name.split())

    return name, tags


def normalize_actors(entry: Dict) -> Dict[str, Set[str]]:
    """
    Приводит информацию о актёрах к виду {имя: теги}.
    Приоритетно разбирает entry["actors_raw"], иначе использует entry["actors"].
    """
    found: Dict[str, Set[str]] = {}

    raw = (entry.get("actors_raw") or "").strip()
    tokens = _split_people_blob(raw)

    if not tokens:
        for a in entry.get("actors", []):
            name = a.get("name", "")
            tokens.extend(_split_people_blob(name))

    for tok in tokens:
        name, tags = _parse_actor_token(tok)
        if not name:
            continue
        if name not in found:
            found[name] = set()
        found[name].update(tags)

    return found


# ====== Проверки ======
def _kv_ok(prev: Dict, curr: Dict) -> Tuple[bool, str | None]:
    """Запрещает ставить два номера с КВ подряд."""
    if prev.get("kv") and curr.get("kv"):
        return False, "Два номера с КВ подряд запрещены"
    return True, None


def _actors_ok(prev: Dict, curr: Dict, *, tyanuchka_between: bool) -> Tuple[bool, List[str]]:
    """
    Проверка пересечения актёров между соседними номерами.
    Правила:
      - актёр не должен выступать подряд;
      - если в ПРЕДЫДУЩЕМ номере у актёра 'early' → можно подряд;
      - если в ТЕКУЩЕМ номере у актёра 'later' → можно подряд;
      - если где-либо 'gk' → нужен минимум один номер паузы;
      - если есть тянучка → снимает все ограничения, кроме (гк).
    """
    reasons: List[str] = []

    prev_actors = normalize_actors(prev)
    curr_actors = normalize_actors(curr)

    common = set(prev_actors.keys()) & set(curr_actors.keys())
    if not common:
        return True, reasons

    for name in sorted(common):
        prev_tags = prev_actors.get(name, set())
        curr_tags = curr_actors.get(name, set())

        # (гк) приоритетно
        if "gk" in prev_tags or "gk" in curr_tags:
            reasons.append(f"'{name}': (гк) требует паузы минимум в один номер")
            continue

        # если есть тянучка — снимаем остальные запреты
        if tyanuchka_between:
            continue

        # проверка обычного подряд
        allow_by_early = "early" in prev_tags
        allow_by_later = "later" in curr_tags

        if not (allow_by_early or allow_by_later):
            reasons.append(f"'{name}': выступает подряд без разрешающих тегов (!, %)")

    return len(reasons) == 0, reasons


def can_follow(prev: Dict, curr: Dict, *, tyanuchka_between: bool = False) -> CheckResult:
    """
    Проверяет, можно ли ставить номер curr сразу после prev.
    Возвращает CheckResult(ok, reasons)
    """
    ok_kv, kv_reason = _kv_ok(prev, curr)
    if not ok_kv:
        return CheckResult(False, [kv_reason])

    ok_act, act_reasons = _actors_ok(prev, curr, tyanuchka_between=tyanuchka_between)
    if not ok_act:
        return CheckResult(False, act_reasons)

    return CheckResult(True, [])


# ====== УДАЛИТЬ (самотест) ======
if __name__ == "__main__":
    # демонстрация разных кейсов
    prev = {
        "title": "Номер A",
        "kv": False,
        "actors_raw": "Брекоткин%%!\nСоколов!(гк)\nИсаев",
    }
    curr = {
        "title": "Номер B",
        "kv": True,
        "actors_raw": "Брекоткин%\nСоколов\nИсаев%",
    }

    # без тянучки
    r1 = can_follow(prev, curr)
    print("A→B без тянучки:", r1.ok, r1.reasons)

    # с тянучкой
    r2 = can_follow(prev, curr, tyanuchka_between=True)
    print("A→B с тянучкой:", r2.ok, r2.reasons)
# ====== УДАЛИТЬ ======
