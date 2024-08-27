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

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.utils import resolve_dataset
from masschange.dataproducts.db.utils import get_db_connection
from masschange.ingest.executor.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.executor.utils.caggs import refresh_continuous_aggregates
from masschange.ingest.executor.utils.ensure import ensure_table_exists, ensure_continuous_aggregates, ensure_database_exists, ensure_metadata_tables_exist
from masschange.ingest.executor.utils.enumeration import enumerate_files_in_dir_tree, order_filepaths_by_filename
from masschange.ingest.executor.utils.metadata import update_metadata
from masschange.utils.logging import configure_root_logger
from masschange.utils.timespan import TimeSpan
from masschange.ingest.executor.errors import EmptyProductException

log = logging.getLogger()


def run(product: TimeSeriesDataProduct, src: str, data_is_zipped: bool = True):
    """

    Parameters
    ----------
    src - the directory containing input files, identified by ACC1A_{YYYY-MM-DD}_{satellite_id}_04.txt
    dest - the destination parquet root directory

    Returns
    -------

    """

    log.info(f'ingesting {product.get_full_id()} data from {src}')
    log.info(f'targeting {"zipped" if data_is_zipped else "non-zipped"} data')
    reader = product.get_reader()
    zipped_regex = reader.get_zipped_input_file_default_regex()
    unzipped_regex = reader.get_input_file_default_regex()
    target_filepaths = get_zipped_input_iterable(src, zipped_regex, unzipped_regex) if data_is_zipped \
        else order_filepaths_by_filename(enumerate_files_in_dir_tree(src, unzipped_regex, match_filename_only=True))
    for fp in target_filepaths:
        try:
            ingest_file_to_db(product, fp)
        except EmptyProductException as e:
            log.warning(f'{e} Skipping ingestion of the file...')

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


def delete_overlapping_data(dataset: TimeSeriesDataset, data_temporal_span: TimeSpan):
    table_name = dataset.get_table_name()
    with get_db_connection() as conn, conn.cursor() as cur:
        sql = f"""
            DELETE 
            FROM {table_name}
                WHERE   {dataset.product.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                    AND {dataset.product.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
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


def ingest_file_to_db(product: TimeSeriesDataProduct, src_filepath: str):
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')


    reader = product.get_reader()
    dataset = TimeSeriesDataset(product, reader.extract_dataset_version(src_filepath), reader.extract_instrument_id(src_filepath))

    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath)
    data_temporal_span = TimeSpan(begin=min(pd_df[product.TIMESTAMP_COLUMN_NAME]),
                                  end=max(pd_df[product.TIMESTAMP_COLUMN_NAME]))

    ensure_table_exists(dataset)
    ensure_continuous_aggregates(dataset)

    table_name = dataset.get_table_name()
    delete_overlapping_data(dataset, data_temporal_span)
    ingest_df(pd_df, table_name)
    refresh_continuous_aggregates(dataset)
    update_metadata(dataset, data_temporal_span)

    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingested file: {src_filepath}')
    else:
        log.info(f'ingested file: {os.path.split(src_filepath)[-1]}')


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
    ensure_metadata_tables_exist(database_name)

    start = datetime.now()
    log.info(f'starting ingest of {args.dataset.get_full_id()} from {args.src} begin')
    run(args.dataset, args.src, data_is_zipped=args.target_zipped_data)
    log.info(
        f'ingest of {args.dataset.get_full_id()} from {args.src} completed in {get_human_readable_elapsed_since(start)}')

    exit(0)
