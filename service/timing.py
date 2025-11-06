# service/timing.py
import asyncio
import time
import functools
from service.logger import get_logger

log = get_logger("stageflow.timing")


def measure_time(label: str = None):
    """
    Декоратор, измеряющий время выполнения функции и выводящий в лог.
    Работает с синхронными и асинхронными функциями.
    """
    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                tag = label or func.__name__
                start = time.perf_counter()
                log.info(f"⏱️ [{tag}] Начало выполнения...")
                result = await func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                log.info(f"✅ [{tag}] Завершено за {elapsed:.3f} сек.")
                return result

            return async_wrapper

        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                tag = label or func.__name__
                start = time.perf_counter()
                log.info(f"⏱️ [{tag}] Начало выполнения...")
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                log.info(f"✅ [{tag}] Завершено за {elapsed:.3f} сек.")
                return result

            return sync_wrapper

    return decorator
