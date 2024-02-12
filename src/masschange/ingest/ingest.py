import argparse
import logging
import os
import shutil
import tarfile
import tempfile
from datetime import datetime
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

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree
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
        else enumerate_input_filepaths(src, unzipped_regex)
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

    for tar_fp in enumerate_input_filepaths(root_dir, enclosing_filename_match_regex):
        temp_dir = tempfile.mkdtemp(prefix='masschange-gracefo-ingest-')
        log.debug(f'extracting contents of {tar_fp} to {temp_dir}')
        with tarfile.open(tar_fp) as tf:
            tf.extractall(temp_dir)

        for fp in enumerate_input_filepaths(temp_dir, filename_match_regex):
            yield fp

        log.debug(f'cleaning up {temp_dir}')
        shutil.rmtree(temp_dir)


def enumerate_input_filepaths(root_dir: str, filename_match_regex: str) -> Iterable[str]:
    # TODO: replace all calls to enumerate_input_filepaths() with calls to enumerate_files_in_dir_tree()
    return enumerate_files_in_dir_tree(root_dir, filename_match_regex, match_filename_only=True)


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
    table_name = dataset.get_table_name(stream_id)
    log.info(f'Ensuring table_name exists: "{table_name}"')

    timestamp_column_name = dataset.TIMESTAMP_COLUMN_NAME

    with get_db_connection() as conn, conn.cursor() as cur:
        try:
            sql = f"""
            {dataset.get_sql_table_create_statement(stream_id)}
            
            select create_hypertable('{table_name}','{timestamp_column_name}');
            """
            cur.execute(sql)
            conn.commit()
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass


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
        'GRACEFO_PCI1A': GraceFOPci1ADataset
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
    log.info(f'ingest of {args.dataset.get_full_id()} from {args.src} completed in {get_human_readable_elapsed_since(start)}')

    exit(0)
