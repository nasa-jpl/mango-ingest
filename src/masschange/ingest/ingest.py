import argparse
import logging
import os
import shutil
import tarfile
import tempfile
from datetime import datetime
from io import StringIO
from typing import Iterable, Type

import pandas
import pandas as pd
import psycopg2

from masschange.datasets.gracefo.acc1a import GraceFOAcc1ADataset
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree
from masschange.utils.logging import configure_root_logger

log = logging.getLogger()


def run(dataset_cls: Type[TimeSeriesDataset], src: str, data_is_zipped: bool = True):
    """

    Parameters
    ----------
    src - the directory containing input files, identified by ACC1A_{YYYY-MM-DD}_{satellite_id}_04.txt
    dest - the destination parquet root directory

    Returns
    -------

    """

    log.info(f'ingesting {dataset_cls.get_full_id()} data from {src}')
    log.info(f'targeting {"zipped" if data_is_zipped else "non-zipped"} data')
    reader = dataset_cls.get_reader()
    zipped_regex = reader.get_zipped_input_file_default_regex()
    unzipped_regex = reader.get_input_file_default_regex()
    target_filepaths = get_zipped_input_iterable(src, zipped_regex, unzipped_regex) if data_is_zipped \
        else enumerate_input_filepaths(src, unzipped_regex)
    for fp in target_filepaths:
        ingest_file_to_db(dataset_cls, fp)


def get_zipped_input_iterable(root_dir: str,
        enclosing_filename_match_regex: str,
        filename_match_regex: str ) -> Iterable[str]:
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


def ensure_table_exists(dataset_cls: Type[TimeSeriesDataset], stream_id: str) -> None:
    table_name = dataset_cls.get_table_name(stream_id)
    log.info(f'Ensuring table_name exists: "{table_name}"')

    timestamp_column_name = dataset_cls.TIMESTAMP_COLUMN_NAME

    with get_db_connection() as conn, conn.cursor() as cur:
        try:
            sql = f"""
            {dataset_cls._get_sql_table_create_statement(stream_id)}
            
            select create_hypertable('{table_name}','{timestamp_column_name}');
            """
            cur.execute(sql)
            conn.commit()
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass


def ingest_df(df: pandas.DataFrame, table_name: str) -> None:
    """
    see: https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
    """

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


def ingest_file_to_db(dataset_cls: Type[TimeSeriesDataset], src_filepath: str):
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')

    reader = dataset_cls.get_reader()
    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath)

    stream_id = reader.extract_stream_id(src_filepath)
    table_name = dataset_cls.get_table_name(stream_id)

    ensure_table_exists(dataset_cls, stream_id)
    log.info(f'writing data to table {table_name}')
    ingest_df(pd_df, table_name)

    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingested file: {src_filepath}')
    else:
        log.info(f'ingested file: {os.path.split(src_filepath)[-1]}')


def resolve_dataset_class(dataset_id: str) -> Type[TimeSeriesDataset]:
    #     hardcode these for now, figure out how to generate them later
    mappings = {
        'gracefo_acc1a': GraceFOAcc1ADataset
    }

    cls = mappings.get(dataset_id)
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
    ap.add_argument('--dataset', required=True, dest='dataset_id', type=resolve_dataset_class,
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

    start = datetime.now()
    log.info(f'starting ingest of {args.dataset_id} from {args.src} begin')
    run(args.dataset_id, args.src, data_is_zipped=args.target_zipped_data)
    log.info(f'ingest of {args.dataset_id} from {args.src} completed in {get_human_readable_elapsed_since(start)}')

    exit(0)
