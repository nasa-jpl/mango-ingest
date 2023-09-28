import functools
import logging
from datetime import datetime
from typing import Callable


def log_elapsed_time(label: str = None, log_f: Callable = logging.getLogger().info):
    def inner(f):
        task_label = label or f'{f.__name__}()'

        @functools.wraps(f)
        def wrapper():
            begin = datetime.now()
            result = f()
            log_f(f'{task_label} completed in {get_human_readable_elapsed_since(begin)}')
            return result

        return wrapper

    return inner


def get_human_readable_elapsed_since(begin: datetime) -> str:
    elapsed_seconds = (datetime.now() - begin).total_seconds()
    h = int(elapsed_seconds / 3600)
    m = int(elapsed_seconds % 3600 / 60)
    s = int(elapsed_seconds % 60)
    return (f"{h}h" if h else "") + (f"{m}m" if m else "") + f"{s}s"
