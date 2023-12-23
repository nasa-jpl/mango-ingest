import random

import masschange.utils.logging


def get_random_hex_id(id_len: int = 6) -> str:
    val = random.randint(0, 16 ** id_len)
    return hex(val)[2:]
