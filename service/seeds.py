# service/seeds.py
import random
import time
from service.logger import get_logger

log = get_logger("stageflow.seeds")


def generate_unique_seeds(n: int = 5) -> list[int]:
    """
    Генерирует n уникальных seed-значений на основе времени и случайности.
    """
    base_seed = int(time.time() * 1000) % 10_000_000
    random.seed(base_seed)

    seeds = set()
    while len(seeds) < n:
        seeds.add(random.randint(1000, 99999))

    result = sorted(list(seeds))
    log.info(f"🌱 Сгенерированы seed’ы: {result}")
    return result


# ✅ алиас для совместимости с main.py
def generate_seeds(n: int = 5) -> list[int]:
    """Совместимый алиас, используется ботом (main.py)."""
    return generate_unique_seeds(n)
