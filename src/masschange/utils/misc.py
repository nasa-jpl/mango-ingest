import random
from datetime import timedelta


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
