import configparser
import logging
import os


def get_config_root_env_key() -> str:
    return 'MASSCHANGE_CONFIG_ROOT'


def __load_config():
    config = configparser.ConfigParser()
    config_root_env_key = get_config_root_env_key()
    if os.environ.get(config_root_env_key):
        config_root = os.environ[config_root_env_key]
        logging.info(f'using configuration root path {config_root} specified by env-var {config_root_env_key}')
    else:
        config_root = os.path.expanduser(os.path.join('~', '.config', 'masschange'))
        logging.info(f'using default configuration root path {config_root}')

    default_config_path = os.path.join(config_root, 'defaults.conf.ini')  # defaults, overwritten by update
    if os.path.exists(default_config_path):
        logging.info(f'using config defaults from {default_config_path}')
    else:
        err_msg = f'no default configuration file found at {default_config_path}'
        logging.error(err_msg)
        raise ValueError(err_msg)
    user_config_path = os.path.join(config_root, 'conf.ini')  # user-specified values, retained during update

    config.read([default_config_path, user_config_path])

    return config


# initialized on module load as a singleton to avoid repeated initialization log messages
__config = __load_config()


def get_config() -> configparser.ConfigParser:
    return __config
