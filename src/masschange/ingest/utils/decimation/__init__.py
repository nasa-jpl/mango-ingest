import functools
import multiprocessing
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


def decimate_file_by_constant_factor(factor: int, agg_config: [Dict[str, List[str | Callable]]], input_path: str,
                                     output_path: str):
    log.info(f'loading data for decimation from {input_path}')
    in_ds = (pq.ParquetDataset(input_path))
    in_table = in_ds.read()

    df = in_table.to_pandas()
    decimated_df = decimate_df_by_constant_factor(factor, agg_config, df)

    log.info(f'writing decimated output to {output_path}')
    out_table = pa.Table.from_pandas(decimated_df, preserve_index=False)
    pq.write_table(out_table, output_path)

    log.info('decimation complete!')


def decimate_df_by_constant_factor(factor: int, agg_config, df: pd.DataFrame):
    # N.B. THIS WILL RESULT IN CORES-1 ADDITIONAL AGGREGATIONS WITH INCONSISTENT INPUT ROW COUNTS.  THIS IS ACCEPTABLE FOR
    # PLOTTING BUT MAYBE NOT FOR EXACT DATA ANALYSIS.  KEEP AN EYE ON THIS.

    # TODO: IMPLEMENT SORTING ON TIMESTAMP TO ENSURE FAST TEMPORAL QUERYING - TEST HOW IMPORTANT THAT IS
    cores = 12
    chunks = np.array_split(df, cores)
    decimation_f = functools.partial(_decimate_df_by_constant_factor, factor, agg_config)
    with multiprocessing.Pool(cores)as mp_pool:
        decimated_chunks = mp_pool.map(decimation_f, chunks)

    result = pd.concat(decimated_chunks)
    return result

def _decimate_df_by_constant_factor(factor: int, agg_config, df: pd.DataFrame):
    # TODO: CONVERT df TO np.DataFrame, reinstate satellite_id
    decimate_f = functools.partial(decimate_rowgroup, agg_config)
    # create row-groups every {factor} rows, and apply decimation function to each row-group
    decimated_df = df.groupby(df.index // factor).apply(decimate_f)

    # hack - this needs to be a partition key.  Assumes file is homogeneous wrt satellite-id, which is currently valid
    # decimated_df['satellite_id'] = df['satellite_id'][0]
    return decimated_df


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

    # hack - need to incorporate this into AggregationConfig post_aggregation_column_casts
    agg_df['rcvtime_mean'] = agg_df['rcvtime_mean'].astype(int)
    # hack - need to incorporate this into AggregationConfig post_aggregation_column_renames
    agg_df.rename(columns={'rcvtime_mean': 'rcvtime'})

    return agg_df


if __name__ == '__main__':
    agg_config = {
        'lin_accl_x': ['min', 'max'],
        'lin_accl_y': ['min', 'max'],
        'lin_accl_z': ['min', 'max'],
        'ang_accl_x': ['min', 'max'],
        'ang_accl_y': ['min', 'max'],
        'ang_accl_z': ['min', 'max'],
        'rcvtime': np.mean
    }

    decimation_step_factors = [20, 20, 20, 3]  # results in 1:20, 1:400, 1:8000, 1:24000
    current_decimation_factor = 1
    for factor in decimation_step_factors[:1]:  # TODO: Remove slice
        next_decimation_factor = current_decimation_factor * factor
        input_root_path = f'/nomount/masschange/data_volume_mount/gracefo_1a/decimation_factor={current_decimation_factor}/temporal_partition_key=738849600000000/part-00000-ac6d05e5-3cf2-4769-89b9-b6613d1b50ec_00000.c000.snappy.parquet'
        # output_root_path = f'/nomount/masschange/data_volume_mount/gracefo_1a/decimation_factor={next_decimation_factor}/temporal_partition_key=738849600000000/part-00000-ac6d05e5-3cf2-4769-89b9-b6613d1b50ec_00000.c000.snappy.parquet'
        output_root_path = f'/nomount/masschange/data_volume_mount/gracefo_1a/decimation_factor={next_decimation_factor}/temporal_partition_key=738849600000000/'
        os.makedirs(output_root_path, exist_ok=True)
        output_filepath = os.path.join(output_root_path, 'output.parquet')

        execution_start = datetime.now()
        log.info(f'Decimating from 1:{current_decimation_factor} to 1:{next_decimation_factor}')
        decimate_file_by_constant_factor(next_decimation_factor, agg_config, input_root_path, output_filepath)
        log.info(f"decimation of one day's data completed in {get_human_readable_elapsed_since(execution_start)}")
