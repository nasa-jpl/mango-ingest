import functools
import logging
import random
from datetime import timedelta, datetime
from typing import Callable


def get_human_readable_timedelta(td: timedelta) -> str:
    total_seconds = td.total_seconds()

    if total_seconds < 1:
        return '<1s'

    d = int(total_seconds / (3600 * 24))
    h = int(total_seconds % (3600 * 24) / 3600)
    m = int(total_seconds % 3600 / 60)
    s = int(total_seconds % 60)

    show_d = d > 0
    show_h = show_d or h > 0
    show_m = show_h or m > 0

    return (f"{d} days, " if show_d else "") + (f"{str(h).rjust(2, '0')}h" if show_h else "") + (
        f"{str(m).rjust(2, '0')}m" if show_m else "") + f"{str(s).rjust(2, '0')}s"


def get_random_hex_id(id_len: int = 6) -> str:
    val = random.randint(0, 16 ** id_len)
    return hex(val)[2:]


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
