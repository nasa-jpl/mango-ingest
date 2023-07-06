from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def get_header_line_count(filename: str) -> int:
    last_header_line_prefix = '# End of YAML header'

    header_rows = 0
    with open(filename) as f:
        for line in f:  # iterates lazily
            header_rows += 1
            if line.startswith(last_header_line_prefix):
                return header_rows


def load_data_from_file(filename: str):
    raw_data = load_raw_data_from_file(filename)
    df = pd.DataFrame(raw_data)

    df['timestamp'] = df.apply(populate_timestamp, axis=1)
    df_without_extraneous_columns = df.drop(['rcvtime_intg', 'rcvtime_frac'], axis=1)

    return df_without_extraneous_columns


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


def populate_timestamp(row):
    reference_timestamp = datetime(2000, 1, 1, 12)
    return reference_timestamp + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)
