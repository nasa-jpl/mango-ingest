import logging
import os
from typing import Optional


def get_configured_logger(log_filepath: Optional[str] = None, logger_name: Optional[str] = None, log_level: int = logging.DEBUG):

    logging.root.handlers = []
    handlers = [logging.StreamHandler()]

    if log_filepath:
        try:
            log_parent_dir = os.path.split(log_filepath)[0]
            os.makedirs(log_parent_dir, exist_ok=True)

            # only add the file handler if the path is accessible
            with open(log_filepath, 'w'):
                pass
            logging.info(f'writing logs to {log_filepath}')
            handlers.append(logging.FileHandler(log_filepath))
        except (OSError, PermissionError):
            logging.error(f'failed to add log handler for path due to permission error: {log_filepath}')

    # TODO: Enable differential format once initial implementation is complete - removing now for ease of dev
    # log_format = f'%(asctime)s [%(levelname)s] {"%(name)s:%(funcName)s " if log_level == logging.DEBUG else ""}- %(message)s'
    log_format = f'%(asctime)s [%(levelname)s] - %(message)s'

    logging.basicConfig(
        level=log_level,  # TODO: get from env or CLI variable
        format=log_format,
        handlers=handlers
    )

    logging.info(f'writing logs to {log_filepath}')

    return logging.getLogger(logger_name or __name__)