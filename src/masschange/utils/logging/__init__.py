import logging
import os
from typing import Optional


def configure_root_logger(log_filepath: Optional[str] = None, log_level: int = logging.DEBUG,
                          log_format: str = f'%(asctime)s [%(levelname)s] - %(message)s'):
    logging.root.setLevel(log_level)
    logging.root.handlers.clear()

    formatter = logging.Formatter(log_format)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(formatter)

    logging.root.addHandler(stdout_handler)

    if log_filepath:
        try:
            log_parent_dir = os.path.split(log_filepath)[0]
            os.makedirs(log_parent_dir, exist_ok=True)

            # only add the file handler if the path is accessible
            with open(log_filepath, 'w'):
                pass
            logging.info(f'writing logs to {log_filepath}')

            file_handler = logging.FileHandler(log_filepath)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)

            logging.root.addHandler(file_handler)
        except (OSError, PermissionError):
            logging.error(f'failed to add log handler for path due to permission error: {log_filepath}')
