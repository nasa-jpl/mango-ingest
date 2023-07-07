from datetime import datetime

# This is the timestamp_epoch used by the GRACE-FO 1A timestamps
reference_epoch = datetime(2000, 1, 1, 12)

# TODO: Replace with configparser config

ZIPPED_INPUT_FILE_DEFAULT_REGEX = 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL04\.ascii\.noLRI\.tgz'
INPUT_FILE_DEFAULT_REGEX = '^ACC1A_\d{4}-\d{2}-\d{2}_(?P<satellite_id>[CD])_04\.txt$'
PARQUET_TEMPORAL_PARTITION_KEY = 'temporal_partition_key'

# Environment variable keys
LOG_ROOT_ENV_VAR_KEY = 'MASSCHANGE_LOGS_ROOT'