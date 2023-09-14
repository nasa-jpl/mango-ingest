import functools
import multiprocessing
import os
import shutil
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from masschange.ingest.datasets.constants import PARQUET_TEMPORAL_PARTITION_KEY
from masschange.ingest.utils import get_configured_logger
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.decimation.aggregationrunconfig import AggregationRunConfig
from masschange.ingest.utils.decimation.partitioning import get_partition_id
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


def get_decimation_subpartition_path(dataset_root_path: str, decimation_factor: int) -> str:
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

    column_rename_mappings = {f'{fn}_{agg}_{agg}': f'{fn}_{agg}' for agg in measurement_aggregations for fn in
                              measurement_field_names} | {
                                 'rcvtime_mean': 'rcvtime'
                             }

    return AggregationRunConfig(aggregation_funcs, type_conversion_mappings, column_rename_mappings)


def apply_decimation_stage(run_config: AggregationRunConfig, dataset_root_path: str, src_absolute_ratio: int,
                           step_factor: int):
    input_decimation_partition_path = get_decimation_subpartition_path(dataset_root_path, src_absolute_ratio)

    output_absolute_ratio = src_absolute_ratio * step_factor  # output decimation level as ratio of full-resolution
    output_decimation_partition_path = get_decimation_subpartition_path(dataset_root_path, output_absolute_ratio)

    target_files = enumerate_files_in_dir_tree(input_decimation_partition_path, '.*\.parquet$',
                                               match_filename_only=True)
    for input_filepath in target_files:
        output_filepath = input_filepath.replace(input_decimation_partition_path, output_decimation_partition_path)
        output_filepath_parent_dir = os.path.split(output_filepath)[0]
        os.makedirs(output_filepath_parent_dir, exist_ok=True)

        decimate_file_by_constant_factor(step_factor, run_config, input_filepath, output_filepath)


def temporally_repartition(dataset_root_path: str, output_absolute_ratio: int, output_hours_per_partition: int, partition_epoch_offset_hours: int):
    """

    Parameters
    ----------
    dataset_root_path -
    output_absolute_ratio
    output_hours_per_partition
    partition_epoch_offset_hours

    Returns
    -------

    """
    partition_prefix_str = f'{PARQUET_TEMPORAL_PARTITION_KEY}='
    output_subpartition_path = get_decimation_subpartition_path(dataset_root_path, output_absolute_ratio)
    for filepath in enumerate_files_in_dir_tree(
            output_subpartition_path):  # TODO: Solve race condition potential of lazy iterator
        parent_dirpath, filename = os.path.split(filepath)
        pre_temporal_path, temporal_path_chunk = os.path.split(parent_dirpath)

        if not (temporal_path_chunk.startswith(partition_prefix_str)):
            log.error(
                f"Filepath {filepath} does not have parent dir specifying partition with key {PARQUET_TEMPORAL_PARTITION_KEY}")
            continue

        current_temporal_partition_value = int(temporal_path_chunk.replace(partition_prefix_str, ''))
        new_temporal_partition_value = get_partition_id(current_temporal_partition_value, output_hours_per_partition,
                                                        partition_epoch_offset_hours)
        new_temporal_path_chunk = f'{PARQUET_TEMPORAL_PARTITION_KEY}={new_temporal_partition_value}'
        new_filepath_parent_dir = os.path.join(pre_temporal_path, new_temporal_path_chunk)
        new_filepath = os.path.join(new_filepath_parent_dir, filename)

        os.makedirs(new_filepath_parent_dir, exist_ok=True)
        shutil.move(filepath, new_filepath)

        parent_dirpath_remaining_file_count = len(os.listdir(parent_dirpath))
        if parent_dirpath_remaining_file_count == 0:
            log.debug(f'Cleaning up empty dir "{parent_dirpath}"')
            os.rmdir(parent_dirpath)

        epoch_dt = datetime(2000, 1, 1, 12)
        old_partition_dt = epoch_dt + timedelta(microseconds=current_temporal_partition_value)
        new_partition_dt = epoch_dt + timedelta(microseconds=new_temporal_partition_value)
        log.debug(f'repartitioning from {old_partition_dt.isoformat()} to {new_partition_dt.isoformat()}')

if __name__ == '__main__':

    #### EXTRACT TO DATASET-SPECIFIC CLASS/OBJECT ####
    dataset_root_path = '/nomount/masschange/data_volume_mount/gracefo_1a/'
    base_hours_per_partition = 24
    decimation_step_factors = [20, 20, 20, 3]  # factor to decimate by in each step. yields results in 1:20, 1:400, 1:8000, 1:24000
    partition_epoch_offset_hours = 12
    #### END EXTRACT TO DATASET-SPECIFIC CLASS/OBJECT ####

    src_absolute_ratio = 1  # input decimation level as ratio of full-resolution
    for step_factor in decimation_step_factors:
        run_config = get_initial_runconfig() if src_absolute_ratio == 1 else get_subsequent_runconfig()
        output_absolute_ratio = src_absolute_ratio * step_factor
        output_hours_per_partition = base_hours_per_partition * output_absolute_ratio

        log.info(f'Decimating from 1:{src_absolute_ratio} to 1:{output_absolute_ratio}')
        execution_start = datetime.now()
        apply_decimation_stage(run_config, dataset_root_path, src_absolute_ratio, step_factor)
        log.info(
            f"decimation stage for one week's data completed in {get_human_readable_elapsed_since(execution_start)}")

        log.info(f'Repartitioning {dataset_root_path} to {output_hours_per_partition}hrs/partition')
        execution_start = datetime.now()
        temporally_repartition(dataset_root_path, output_absolute_ratio, output_hours_per_partition, partition_epoch_offset_hours)
        log.info(
            f"Repartitioning completed in {get_human_readable_elapsed_since(execution_start)}")


        src_absolute_ratio = output_absolute_ratio
