import functools
import multiprocessing
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from masschange.ingest.utils import get_configured_logger
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.decimation.aggregationrunconfig import AggregationRunConfig
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree

log = get_configured_logger()


def decimate_file_by_constant_factor(factor: int, run_config: AggregationRunConfig, input_path: str,
                                     output_path: str):
    log.info(f'loading data for decimation from {input_path}')
    in_ds = (pq.ParquetDataset(input_path))
    in_table = in_ds.read()

    df = in_table.to_pandas()
    decimated_df = decimate_df_by_constant_factor(factor, run_config, df)

    log.info(f'writing decimated output to {output_path}')
    out_table = pa.Table.from_pandas(decimated_df, preserve_index=False)
    pq.write_table(out_table, output_path)

    log.info('decimation complete!')


def decimate_df_by_constant_factor(factor: int, run_config: AggregationRunConfig, df: pd.DataFrame):
    # N.B. THIS WILL RESULT IN CORES-1 ADDITIONAL AGGREGATIONS WITH INCONSISTENT INPUT ROW COUNTS.  THIS IS ACCEPTABLE FOR
    # PLOTTING BUT MAYBE NOT FOR EXACT DATA ANALYSIS.  KEEP AN EYE ON THIS.

    # TODO: IMPLEMENT SORTING ON TIMESTAMP TO ENSURE FAST TEMPORAL QUERYING - TEST HOW IMPORTANT THAT IS
    cores = 12
    chunks = np.array_split(df, cores)
    decimation_f = functools.partial(_decimate_df_by_constant_factor, factor, run_config)
    with multiprocessing.Pool(cores) as mp_pool:
        decimated_chunks = mp_pool.map(decimation_f, chunks)

    result = pd.concat(decimated_chunks)
    return result


def _decimate_df_by_constant_factor(factor: int, run_config: AggregationRunConfig, df: pd.DataFrame):
    # TODO: CONVERT df TO np.DataFrame, reinstate satellite_id
    decimate_f = functools.partial(decimate_rowgroup, run_config)
    # create row-groups every {factor} rows, and apply decimation function to each row-group
    decimated_df = df.groupby(df.index // factor).apply(decimate_f)

    # hack - this needs to be a partition key.  Assumes file is homogeneous wrt satellite-id, which is currently valid
    # decimated_df['satellite_id'] = df['satellite_id'][0]
    return decimated_df


def decimate_rowgroup(run_config: AggregationRunConfig, rowgroup: pd.DataFrame):
    # Perform aggregations
    rowgroup = rowgroup.agg(run_config.aggregation_funcs)

    # Extract values into flat dict column mapping
    aggregate_dict = {}
    for property in rowgroup.columns:
        for agg_name in rowgroup[property].index:
            new_property = f'{property}_{agg_name}'
            value = rowgroup[property][agg_name]
            if not np.isnan(value):
                aggregate_dict[new_property] = [value]

    df = pd.DataFrame.from_dict(aggregate_dict)

    # Apply column type casts
    for column, type in run_config.column_type_mappings.items():
        df[column] = df[column].astype(type)

    # Apply column name remappings
    df = df.rename(columns=run_config.column_name_mappings)

    return df


def get_decimation_subpartition(dataset_root_path: str, decimation_factor: int) -> str:
    return os.path.join(dataset_root_path, f'decimation_factor={decimation_factor}')


def get_initial_runconfig() -> AggregationRunConfig:
    aggregation_funcs = {
        'lin_accl_x': ['min', 'max'],
        'lin_accl_y': ['min', 'max'],
        'lin_accl_z': ['min', 'max'],
        'ang_accl_x': ['min', 'max'],
        'ang_accl_y': ['min', 'max'],
        'ang_accl_z': ['min', 'max'],
        'rcvtime': np.mean
    }

    type_conversion_mappings = {
        'rcvtime_mean': 'long'
    }

    column_rename_mappings = {
        'rcvtime_mean': 'rcvtime'
    }

    return AggregationRunConfig(aggregation_funcs, type_conversion_mappings, column_rename_mappings)


def get_subsequent_runconfig() -> AggregationRunConfig:
    geometries = {'lin', 'ang'}
    dimensions = {'x', 'y', 'z'}
    measurement_field_names = {f'{geom}_accl_{dim}' for dim in dimensions for geom in geometries}
    measurement_aggregations = {'min', 'max'}
    aggregation_funcs = {f'{fn}_{agg}': [agg] for agg in measurement_aggregations for fn in measurement_field_names} | {
        'rcvtime': np.mean}

    type_conversion_mappings = {
        'rcvtime_mean': 'long'
    }

    column_rename_mappings = {f'{fn}_{agg}_{agg}': f'{fn}_{agg}' for agg in measurement_aggregations for fn in measurement_field_names} | {
        'rcvtime_mean': 'rcvtime'
    }

    return AggregationRunConfig(aggregation_funcs, type_conversion_mappings, column_rename_mappings)


if __name__ == '__main__':

    dataset_root_path = '/nomount/masschange/data_volume_mount/gracefo_1a/'

    decimation_step_factors = [20, 20, 20, 3]  # factor to decimate by in each step. yields results in 1:20, 1:400, 1:8000, 1:24000
    input_total_ratio = 1  # input decimation level as ratio of full-resolution
    for step_factor in decimation_step_factors:
        input_decimation_partition_path = get_decimation_subpartition(dataset_root_path, input_total_ratio)

        output_total_ratio = input_total_ratio * step_factor  # output decimation level as ratio of full-resolution
        output_decimation_partition_path = get_decimation_subpartition(dataset_root_path, output_total_ratio)

        target_files = enumerate_files_in_dir_tree(input_decimation_partition_path, '.*\.parquet$', match_filename_only=True)
        for input_filepath in target_files[:1]:
            output_filepath = input_filepath.replace(input_decimation_partition_path, output_decimation_partition_path)
            output_filepath_parent_dir = os.path.split(output_filepath)[0]
            os.makedirs(output_filepath_parent_dir, exist_ok=True)

            execution_start = datetime.now()
            log.info(f'Decimating from 1:{input_total_ratio} to 1:{output_total_ratio}')
            run_config = get_initial_runconfig() if input_total_ratio == 1 else get_subsequent_runconfig()
            decimate_file_by_constant_factor(step_factor, run_config, input_filepath, output_filepath)
            input_total_ratio = output_total_ratio
            log.info(f"decimation of one day's data completed in {get_human_readable_elapsed_since(execution_start)}")
