from datetime import timedelta


def get_human_readable_timedelta(td: timedelta) -> str:
    total_seconds = td.total_seconds()
    h = int(total_seconds / 3600)
    m = int(total_seconds % 3600 / 60)
    s = int(total_seconds % 60)
    return (f"{h}h" if h else "") + (f"{m}m" if m else "") + f"{s}s"