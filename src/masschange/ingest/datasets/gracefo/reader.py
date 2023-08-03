import os.path
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from masschange.ingest.datasets.gracefo.constants import PARQUET_TEMPORAL_PARTITION_KEY, INPUT_FILE_DEFAULT_REGEX, \
    reference_epoch


def get_header_line_count(filename: str) -> int:
    last_header_line_prefix = '# End of YAML header'

    header_rows = 0
    with open(filename) as f:
        for line in f:  # iterates lazily
            header_rows += 1
            if line.startswith(last_header_line_prefix):
                return header_rows


def load_data_from_file(filepath: str):
    raw_data = load_raw_data_from_file(filepath)
    df = pd.DataFrame(raw_data)

    satellite_id = parse_satellite_id(filepath)
    df = df.assign(satellite_id=satellite_id)
    df['rcv_timestamp'] = df.apply(populate_timestamp, axis=1)
    df[PARQUET_TEMPORAL_PARTITION_KEY] = df.apply(populate_temporal_partition_key, axis=1)

    return df


def parse_satellite_id(filepath: str) -> int:
    mappings = {
        'C': 1,
        'D': 2
    }

    filename = os.path.split(filepath)[-1]
    satellite_id_char = re.search(INPUT_FILE_DEFAULT_REGEX, filename).group('satellite_id')
    try:
        return mappings[satellite_id_char]
    except KeyError:
        raise ValueError(f'failed to parse satellite_id from {filename} with satellite_id_char="{satellite_id_char}" (valid values are {list(mappings.keys())})')

def load_raw_data_from_file(filename: str):
    header_line_count = get_header_line_count(filename)
    # TODO: extract indices, descriptions, units dynamically from the header?
    # TODO: use prodflag and/or QC for filtering measurements?

    float_value_dtype = np.double
    desired_columns_by_index = {
        0: {'label': 'rcvtime_intg', 'type': np.ulonglong},
        1: {'label': 'rcvtime_frac', 'type': np.uint},
        6: {'label': 'lin_accl_x', 'type': float_value_dtype},
        7: {'label': 'lin_accl_y', 'type': float_value_dtype},
        8: {'label': 'lin_accl_z', 'type': float_value_dtype},
        9: {'label': 'ang_accl_x', 'type': float_value_dtype},
        10: {'label': 'ang_accl_y', 'type': float_value_dtype},
        11: {'label': 'ang_accl_z', 'type': float_value_dtype}}

    data = np.loadtxt(
        fname=filename,
        skiprows=header_line_count,
        delimiter=None,  # split rows by whitespace chunks
        usecols=(desired_columns_by_index.keys()),
        dtype=[(col['label'], col['type']) for col in desired_columns_by_index.values()]
    )

    return data


def populate_timestamp(row) -> datetime:

    return reference_epoch + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)


def populate_temporal_partition_key(row) -> str:
    # WHEN ALTERING THIS, THE QUERY IN GraceFO1AFullResolutionDataset.select() MUST BE UPDATED OR PERFORMANCE WILL TANK
    dt: datetime = row.rcv_timestamp
    return dt.date().isoformat()
