# Exists to compensate for https://github.com/python/cpython/issues/109534
# Adapted from https://www.softwareatscale.dev/p/run-python-servers-more-efficiently?open=false#%C2%A7how-do-i-use-it

import ctypes
import os
import random
import time
from threading import Thread

import psutil as psutil


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


def should_trim_memory() -> bool:
    # check if we're close to our OOM limit
    # through psutil
    MEMORY_MAX_GB = 16
    process = psutil.Process(os.getpid())
    return process.memory_info().rss > MEMORY_MAX_GB * 1024 ** 3


def trim_loop() -> None:
    while True:
        if should_trim_memory():
            ret = trim_memory()
            print("trim memory result: ", ret)

        time.sleep(random.randint(30, 60))  # jitter between 30 and 60s


def run_memleak_cleanup_thread():
    thread = Thread(name="TrimThread", target=trim_loop)
    thread.daemon = True
    thread.start()
