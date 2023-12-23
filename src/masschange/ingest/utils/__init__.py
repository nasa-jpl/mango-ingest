import logging
import os
import tempfile
import random
from datetime import datetime
from typing import Optional

from masschange.ingest.datasets.constants import LOG_ROOT_ENV_VAR_KEY


def get_configured_logger(name: Optional[str] = None, log_level: int = logging.DEBUG):
    logs_root = os.environ.get(LOG_ROOT_ENV_VAR_KEY) or tempfile.mkdtemp()

    os.makedirs(logs_root, exist_ok=True)

    logfile_path = os.path.join(logs_root, f'ingest_{datetime.now().isoformat()}.log')

    logging.root.handlers = []
    handlers = [logging.StreamHandler()]
    try:
        # only add the file handler if the path is accessible
        with open(logfile_path, 'w'):
            pass
        logging.info(f'writing logs to {logfile_path}')
        handlers.append(logging.FileHandler(logfile_path))
    except (OSError, PermissionError):
        logging.error(f'failed to add log handler for path due to permission error: {logfile_path}')

    # TODO: Enable differential format once initial implementation is complete - removing now for ease of dev
    # log_format = f'%(asctime)s [%(levelname)s] {"%(name)s:%(funcName)s " if log_level == logging.DEBUG else ""}- %(message)s'
    log_format = f'%(asctime)s [%(levelname)s] - %(message)s'

    logging.basicConfig(
        level=log_level,  # TODO: get from env or CLI variable
        format=log_format,
        handlers=handlers
    )

    logging.info(f'writing logs to {logfile_path}')

    # disable noisy spark logs
    logging.getLogger('py4j.clientserver').setLevel('WARN')

    return logging.getLogger(name or __name__)


def get_random_hex_id(id_len: int = 6) -> str:
    val = random.randint(0, 16 ** id_len)
    return hex(val)[2:]
