import functools
import logging
from datetime import datetime, timedelta
from typing import Callable

from masschange.utils.misc import get_human_readable_timedelta


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
    return get_human_readable_timedelta(datetime.now() - begin)
