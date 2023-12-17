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
from psycopg2._psycopg import AsIs

from masschange.ingest.datasets.gracefo_1a.reader import extract_satellite_id_char
from masschange.ingest.utils import get_configured_logger
from masschange.ingest.datasets.gracefo_1a import reader
from masschange.ingest.datasets.gracefo_1a.constants import INPUT_FILE_DEFAULT_REGEX, \
    ZIPPED_INPUT_FILE_DEFAULT_REGEX
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree

log = get_configured_logger()

def get_connection():
    return psycopg2.connect(database='masschange', user='postgres', password='password', host='localhost', port=5433)

def run(src: str, dest: str, data_is_zipped: bool = True):
    """

    Parameters
    ----------
    src - the directory containing input files, identified by ACC1A_{YYYY-MM-DD}_{satellite_id}_04.txt
    dest - the destination parquet root directory

    Returns
    -------

    """

    collection_prefix = 'ACC1A'

    log.info(f'ingesting data from {src}')
    log.info(f'targeting {"zipped" if data_is_zipped else "non-zipped"} data')
    log.info(f'writing output to parquet root: {dest}')
    target_filepaths = get_zipped_input_iterable(src) if data_is_zipped else enumerate_input_filepaths(src)
    for fp in target_filepaths:
        ingest_file_to_db(collection_prefix, fp)


def get_zipped_input_iterable(
        root_dir: str,
        enclosing_filename_match_regex: str = ZIPPED_INPUT_FILE_DEFAULT_REGEX,
        filename_match_regex: str = INPUT_FILE_DEFAULT_REGEX) -> Iterable[str]:
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


def enumerate_input_filepaths(root_dir: str, filename_match_regex: str = INPUT_FILE_DEFAULT_REGEX) -> Iterable[str]:
    # TODO: replace all calls to enumerate_input_filepaths() with calls to enumerate_files_in_dir_tree()
    return enumerate_files_in_dir_tree(root_dir, filename_match_regex, match_filename_only=True)


def ensure_table_exists(table_name: str) -> None:
    log.info(f'Ensuring table_name exists: "{table_name}"')

    with get_connection() as conn:
        try:
            sql = """
            create table public.%(table_name)s
            (
                lin_accl_x double precision not null,
                lin_accl_y double precision not null,
                lin_accl_z double precision not null,
    
                ang_accl_x double precision not null,
                ang_accl_y double precision not null,
                ang_accl_z double precision not null,
    
                rcvtime bigint not null,
                timestamp timestamptz not null
            );
            
            select create_hypertable('%(table_name)s', 'timestamp');
            """
            cur = conn.cursor()
            cur.execute(sql, {'table_name': AsIs(table_name)})
            conn.commit()
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass


def ingest_df(df: pandas.DataFrame, table_name: str) -> None:
    """
    see: https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
    """

    with get_connection() as conn:
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False, float_format='%f')  # TODO: sort out float format for correct precision - currently only 6dec
        buffer.seek(0)
        with conn.cursor() as cursor:
            try:
                cursor.copy_from(file=buffer, table=table_name, sep=",", null="")
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)


def ingest_file_to_db(collection_prefix: str, src_filepath: str):
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')

    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath)

    # TODO: Implement DB write
    satellite_id_char = extract_satellite_id_char(src_filepath)
    table_name = f'{collection_prefix}_{satellite_id_char}'.lower()

    ensure_table_exists(table_name)
    log.info(f'writing data to table {table_name}')
    ingest_df(pd_df, table_name)

    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingested file: {src_filepath}')
    else:
        log.info(f'ingested file: {os.path.split(src_filepath)[-1]}')


def get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog='MassChange GRACE-FO Data Ingester',
        description='Given raw GRACE-FO ASCII data in a local directory, process that data and store it at a given path'
                    'in parquet format'
    )

    ap.add_argument('src', help='the root directory containing input data files')
    ap.add_argument('dest', help='the parquet root path under which to store the ingested data')

    ap.add_argument('--zipped', '-z', dest='target_zipped_data', action='store_true',
                    help='look in tarballs for source data')

    return ap.parse_args()


if __name__ == '__main__':
    args = get_args()

    start = datetime.now()
    log.info('ingest begin')
    run(args.src, args.dest, data_is_zipped=args.target_zipped_data)
    log.info(f'ingest completed in {get_human_readable_elapsed_since(start)}')

    exit(0)
