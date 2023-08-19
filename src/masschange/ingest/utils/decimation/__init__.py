import functools
import os
import shutil
from datetime import datetime
from typing import Callable, List, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.ingest.utils import get_configured_logger
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since

log = get_configured_logger()


def decimate_by_constant_factor(factor: int, agg_config: [Dict[str, List[str | Callable]]], output_path: str, input_path: str):
    log.info(f'loading data for decimation from {input_path}')
    in_ds = (pq.ParquetDataset(input_path))
    in_table = in_ds.read()

    # subset_count = factor * 10
    # subset_df = in_table[:subset_count]
    
    df = in_table.drop_columns(['rcv_timestamp'])
    df = df.to_pandas()

    #  legacy step for data without rcvtime parts merge
    if 'rcvtime' not in set(df.columns):
        df['rcvtime'] = df['rcvtime_intg'] * 1000000 + df['rcvtime_frac']
        df = df.drop(['rcvtime_intg', 'rcvtime_frac'], axis=1)

    decimate_f = functools.partial(decimate_rowgroup, agg_config)
    decimated_df = df.groupby(df.index // factor).apply(decimate_f)

    # hack - this needs to be a partition key
    decimated_df['satellite_id'] = df['satellite_id'][0]

    out_table = pa.Table.from_pandas(decimated_df, preserve_index=False)
    pq.write_table(out_table, output_path)

    log.info('decimation complete!')

def decimate_rowgroup(agg_config, rowgroup):
    df = rowgroup.agg(agg_config)
    agg_dict = {}
    for property in df.columns:
        for agg_name in df[property].index:
            new_property = f'{property}_{agg_name}'
            value = df[property][agg_name]
            if not np.isnan(value):
                agg_dict[new_property] = [value]

    agg_df = pd.DataFrame.from_dict(agg_dict)

    # hack - need to incorporate this into the agg_config as a post-processing step or something
    agg_df['rcvtime'] = agg_df['rcvtime_mean'].apply(int)
    agg_df = agg_df.drop(['rcvtime_mean'], axis=1)


    return agg_df

def ensure_homogeneous_values(a):
    return a.max()
    # return series
    # values = set(series)
    # if len(values) > 1:
    #     raise ValueError(f'series failed homogeneity test (got multiple values {values})')
    #
    # return values.pop()
    print('blah')

if __name__ == '__main__':
    in_fp = '/nomount/masschange/data_volume_mount/full-resolution-temporally-sorted/temporal_partition_key=2023-06-01/part-00000-09d6f787-670c-483c-8bb2-c83813bfe4d9_00000.c000.snappy.parquet'
    out_fp = '/nomount/masschange/data_volume_mount/decimationtest.parquet'
    
    agg_config = {
        'lin_accl_x': ['min', 'max'],
        'lin_accl_y': ['min', 'max'],
        'lin_accl_z': ['min', 'max'],
        'ang_accl_x': ['min', 'max'],
        'ang_accl_y': ['min', 'max'],
        'ang_accl_z': ['min', 'max'],
        'rcvtime': np.mean
    }

    execution_start = datetime.now()
    decimate_by_constant_factor(24, agg_config, out_fp, in_fp)
    log.info(f"decimation of one day's data completed in {get_human_readable_elapsed_since(execution_start)}")
