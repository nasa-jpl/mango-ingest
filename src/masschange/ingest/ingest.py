import argparse
import logging
import os
import shutil
import tarfile
import tempfile
from datetime import datetime, timedelta
from io import StringIO
from typing import Iterable

import pandas
import pandas as pd
import psycopg2

from masschange.datasets.gracefo.acc1a import GraceFOAcc1ADataset
from masschange.datasets.gracefo.act1a import GraceFOAct1ADataset
from masschange.datasets.gracefo.ihk1a import GraceFOIhk1ADataset
from masschange.datasets.gracefo.imu1a import GraceFOImu1ADataset
from masschange.datasets.gracefo.mag1a import GraceFOMag1ADataset
from masschange.datasets.gracefo.pci1a import GraceFOPci1ADataset
from masschange.datasets.gracefo.sca1a import GraceFOSca1ADataset
from masschange.datasets.gracefo.thr1a import GraceFOThr1ADataset

from masschange.datasets.gracefo.act1b import GraceFOAct1BDataset

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree, order_filepaths_by_filename
from masschange.utils.logging import configure_root_logger
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


def run(dataset: TimeSeriesDataset, src: str, data_is_zipped: bool = True):
    """

    Parameters
    ----------
    src - the directory containing input files, identified by ACC1A_{YYYY-MM-DD}_{satellite_id}_04.txt
    dest - the destination parquet root directory

    Returns
    -------

    """

    log.info(f'ingesting {dataset.get_full_id()} data from {src}')
    log.info(f'targeting {"zipped" if data_is_zipped else "non-zipped"} data')
    reader = dataset.get_reader()
    zipped_regex = reader.get_zipped_input_file_default_regex()
    unzipped_regex = reader.get_input_file_default_regex()
    target_filepaths = get_zipped_input_iterable(src, zipped_regex, unzipped_regex) if data_is_zipped \
        else order_filepaths_by_filename(enumerate_files_in_dir_tree(src, unzipped_regex, match_filename_only=True))
    for fp in target_filepaths:
        ingest_file_to_db(dataset, fp)


def get_zipped_input_iterable(root_dir: str,
                              enclosing_filename_match_regex: str,
                              filename_match_regex: str) -> Iterable[str]:
    """
    Given a root_dir containing data tarballs, provide a transparently-iterable collection of data files matching
    filename_match_regex

    N.B. THIS APPROACH MINIMIZES ADDITIONAL DISK USE BUT CANNOT BE USED WITH CONCURRENCY

    Parameters
    ----------
    root_dir
    enclosing_filename_match_regex
    filename_match_regex

    Returns
    -------

    """

    for tar_fp in order_filepaths_by_filename(
            enumerate_files_in_dir_tree(root_dir, enclosing_filename_match_regex, match_filename_only=True)):
        temp_dir = tempfile.mkdtemp(prefix='masschange-gracefo-ingest-')
        log.debug(f'extracting contents of {tar_fp} to {temp_dir}')
        with tarfile.open(tar_fp) as tf:
            tf.extractall(temp_dir)

        for fp in order_filepaths_by_filename(
                enumerate_files_in_dir_tree(temp_dir, filename_match_regex, match_filename_only=True)):
            yield fp

        log.debug(f'cleaning up {temp_dir}')
        shutil.rmtree(temp_dir)


def ensure_database_exists(db_name: str) -> None:
    conn = get_db_connection(without_db=True)
    conn.autocommit = True
    with conn.cursor() as cur:
        try:
            cur.execute(f'CREATE DATABASE {db_name}')
            log.info(f'Created missing database: "{db_name}"')
        except psycopg2.errors.DuplicateDatabase:
            pass
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')
    conn.close()


def ensure_table_exists(dataset: TimeSeriesDataset, stream_id: str) -> None:
    """
    Ensure that the table for this dataset and stream_id's data exists, creating the table and all necessary views if
    the table doesn't exist.  Does not check for or fix partial existence (i.e. table exists but views do not).
    """
    table_name = dataset.get_table_name(stream_id)
    log.info(f'Ensuring table_name exists: "{table_name}"')

    timestamp_column_name = dataset.TIMESTAMP_COLUMN_NAME
    aggregation_statements = [get_continuous_aggregate_create_statements(dataset, stream_id, agg_level) for agg_level in
                              dataset.get_available_aggregation_levels()]
    aggregation_statements_block = '\n'.join(aggregation_statements)
    with get_db_connection() as conn, conn.cursor() as cur:
        try:
            sql = f"""
            {dataset.get_sql_table_create_statement(stream_id)}
            
            select create_hypertable('{table_name}','{timestamp_column_name}');
            select set_chunk_time_interval('{table_name}', interval '24 hours');
            {aggregation_statements_block}
            """
            cur.execute(sql)
            conn.commit()
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass


def get_continuous_aggregate_create_statements(
        dataset: TimeSeriesDataset,
        stream_id: str,
        aggregation_level: int) -> str:
    aggregation_interval_seconds = dataset.get_nominal_data_interval(aggregation_level).total_seconds()
    source_name = dataset.get_table_or_view_name(stream_id, aggregation_level - 1)
    new_view_name = dataset.get_table_or_view_name(stream_id, aggregation_level)

    agg_column_exprs = []
    aggregable_fields = [field for field in dataset.get_available_fields() if field.has_aggregations]
    for field in aggregable_fields:
        for agg in field.aggregations:
            dest_column = f'{field.name}_{agg.lower()}'
            src_column = dest_column if aggregation_level > 1 else field.name
            column_expr = f'{agg}({src_column}) as {dest_column}'
            agg_column_exprs.append(column_expr)

    bucket_expr = f"time_bucket(INTERVAL '{aggregation_interval_seconds} SECOND', src.{dataset.TIMESTAMP_COLUMN_NAME})"
    agg_columns_block = ',\n'.join(agg_column_exprs)

    return f"""
         -- create materialized view without data
        CREATE MATERIALIZED VIEW {new_view_name}
        WITH (timescaledb.continuous) AS
        SELECT {bucket_expr} AS {dataset.TIMESTAMP_COLUMN_NAME},
        {agg_columns_block}
        FROM {source_name} as src
        GROUP BY {bucket_expr}
        WITH NO DATA;
        
         ---- disable realtime aggregation
         -- RTA is prohibitively expensive, so data availability will be determined by the values used in the continuous
         -- aggregation refresh policy
        ALTER MATERIALIZED VIEW {new_view_name} set (timescaledb.materialized_only = true);
        
         ------ create continuous aggregation maintenance policy
         ---- https://docs.timescale.com/use-timescale/latest/continuous-aggregates/refresh-policies/
         ---- values are hardcoded for now but will eventually depend on datasets, as different datasets will have 
         ---- different operational needs viz data availablility lack
        --SELECT add_continuous_aggregate_policy('{new_view_name}',
        --start_offset => NULL,
        --end_offset => INTERVAL '1 hour',
        --schedule_interval => INTERVAL '1 hour');
    """


def delete_overlapping_data(dataset: TimeSeriesDataset, stream_id: str, data_temporal_span: TimeSpan):
    table_name = dataset.get_table_name(stream_id)
    with get_db_connection() as conn, conn.cursor() as cur:
        sql = f"""
            DELETE 
            FROM {table_name}
                WHERE   {dataset.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                    AND {dataset.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                """
        cur.execute(sql, {'from_dt': data_temporal_span.begin, 'to_dt': data_temporal_span.end})
        conn.commit()
        log.debug(f'purged data from {table_name} for span {data_temporal_span}')


def ingest_df(df: pandas.DataFrame, table_name: str) -> None:
    """
    see: https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
    """
    log.info(f'writing data to table {table_name}')

    with get_db_connection() as conn:
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)
        with conn.cursor() as cursor:
            try:
                cursor.copy_from(file=buffer, table=table_name, sep=",", null="")
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)


def refresh_continuous_aggregates(dataset: TimeSeriesDataset, stream_id: str, data_temporal_span: TimeSpan):
    log.info(f'refreshing continuous aggregates for {dataset.get_table_name(stream_id)}')
    for aggregation_level in dataset.get_available_aggregation_levels():
        materialized_view_name = dataset.get_table_or_view_name(stream_id, aggregation_level)
        bucket_interval = dataset.get_nominal_data_interval(aggregation_level)
        # Refresh span calculation soft-disabled as initial tests indicate that refreshing continuous aggregates for
        #   which no relevant data change has taken place is essentially free
        # refresh_span = get_refresh_span(materialized_view_name, bucket_interval, data_temporal_span)
        refresh_span = TimeSpan(begin=datetime.min, end=datetime.max)
        conn = get_db_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
            sql = f"CALL refresh_continuous_aggregate('{materialized_view_name}', %(from_dt)s, %(to_dt)s);"
            cur.execute(sql, {'from_dt': refresh_span.begin, 'to_dt': refresh_span.end})
            log.debug(f'refreshed cont. agg. {materialized_view_name} for buckets spanning {refresh_span}')
        conn.close()


def get_refresh_span(view_name: str, bucket_interval: timedelta, data_span: TimeSpan) -> TimeSpan:
    """
    Get a dataspan enclosing all extant buckets which overlap a given data_span.  If no data exists in the materialized
    view yet, instead return a safe value which will ensure timescaledb does not complain about too-small a window.

    Parameters
    ----------
    view_name - the name of the materialized view
    bucket_interval - the interval/size of this view's buckets
    data_span - the span of data for which to resolve a refresh span

    Returns
    -------
    an inclusive bucket span over which to refresh the continuous aggregate/materialized view

    """

    sql = f"""
    select min(bucket), max(bucket)
    from {view_name}
    where bucket >= ('{data_span.begin.isoformat()}'::timestamp - INTERVAL '{bucket_interval.total_seconds()} SECONDS')
      and bucket <= ('{data_span.end.isoformat()}'::timestamp + INTERVAL '{bucket_interval.total_seconds()} SECONDS');
      """

    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        results = cur.fetchone()
        if None not in results:
            return TimeSpan(begin=results[0], end=results[1] + bucket_interval)
        else:
            return TimeSpan(begin=datetime.min, end=datetime.max)


def ingest_file_to_db(dataset: TimeSeriesDataset, src_filepath: str):
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')

    reader = dataset.get_reader()
    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath)
    data_temporal_span = TimeSpan(begin=min(pd_df[dataset.TIMESTAMP_COLUMN_NAME]),
                                  end=max(pd_df[dataset.TIMESTAMP_COLUMN_NAME]))

    stream_id = reader.extract_stream_id(src_filepath)

    ensure_table_exists(dataset, stream_id)

    table_name = dataset.get_table_name(stream_id)
    delete_overlapping_data(dataset, stream_id, data_temporal_span)
    ingest_df(pd_df, table_name)
    refresh_continuous_aggregates(dataset, stream_id, data_temporal_span)

    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingested file: {src_filepath}')
    else:
        log.info(f'ingested file: {os.path.split(src_filepath)[-1]}')


def resolve_dataset(dataset_id: str) -> TimeSeriesDataset:
    #     hardcode these for now, figure out how to generate them later
    mappings = {
        'GRACEFO_ACC1A': GraceFOAcc1ADataset,
        'GRACEFO_ACT1A': GraceFOAct1ADataset,
        'GRACEFO_IHK1A': GraceFOIhk1ADataset,
        'GRACEFO_IMU1A': GraceFOImu1ADataset,
        'GRACEFO_MAG1A': GraceFOMag1ADataset,
        'GRACEFO_PCI1A': GraceFOPci1ADataset,
        'GRACEFO_SCA1A': GraceFOSca1ADataset,
        'GRACEFO_THR1A': GraceFOThr1ADataset,

        'GRACEFO_ACT1B': GraceFOAct1BDataset

    }

    cls = mappings.get(dataset_id)()
    if cls is not None:
        return cls
    else:
        err_msg = f"Failed to resolve provided dataset_id (got '{dataset_id}', expected one of {sorted(mappings.keys())})"
        log.error(err_msg)
        raise ValueError(err_msg)


def get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog='MassChange Data Ingester',
        description='Given product data in a local directory, process that data and store it in database'
    )
    ap.add_argument('--dataset', required=True, dest='dataset', type=resolve_dataset,
                    help='the id of the dataset to ingest <TO-DO: print out enumerated list of available ids>')

    ap.add_argument('--src', required=True, dest='src', help='the root directory containing input data files')

    ap.add_argument('--zipped', '-z', dest='target_zipped_data', action='store_true',
                    help='look in tarballs for source data')

    return ap.parse_args()


if __name__ == '__main__':
    args = get_args()

    logs_root = os.environ.get('MASSCHANGE_INGEST_LOGS_ROOT') or tempfile.mkdtemp()
    log_filepath = os.path.join(logs_root, f'ingest_{datetime.now().isoformat()}.log')
    configure_root_logger(log_filepath=log_filepath)

    database_name = os.environ['TSDB_DATABASE']
    ensure_database_exists(database_name)

    start = datetime.now()
    log.info(f'starting ingest of {args.dataset.get_full_id()} from {args.src} begin')
    run(args.dataset, args.src, data_is_zipped=args.target_zipped_data)
    log.info(
        f'ingest of {args.dataset.get_full_id()} from {args.src} completed in {get_human_readable_elapsed_since(start)}')

    exit(0)
