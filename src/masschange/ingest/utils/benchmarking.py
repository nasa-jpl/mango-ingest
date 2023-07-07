from datetime import datetime


def get_human_readable_elapsed_since(begin: datetime) -> str:
    elapsed_seconds = (datetime.now() - begin).total_seconds()
    h = int(elapsed_seconds / 3600)
    m = int(elapsed_seconds % 3600 / 60)
    s = int(elapsed_seconds % 60)
    return (f"{h}h" if h else "") + (f"{m}m" if m else "") + f"{s}s"
