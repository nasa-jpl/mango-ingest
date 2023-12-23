import logging
import os
import tempfile
from datetime import datetime
from typing import Optional

from masschange import utils
from masschange.ingest.datasets.constants import LOG_ROOT_ENV_VAR_KEY

# only initialize log_filepath once to avoid erroneous logfile creationg
__logs_root = os.environ.get(LOG_ROOT_ENV_VAR_KEY) or tempfile.mkdtemp()
__log_filepath = os.path.join(__logs_root, f'ingest_{datetime.now().isoformat()}.log')


def get_configured_logger(name: Optional[str] = None, log_level: int = logging.DEBUG):
    return utils.logging.get_configured_logger(log_filepath=__log_filepath, logger_name=name, log_level=log_level)
