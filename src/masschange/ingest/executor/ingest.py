import argparse
import logging
import os
import shutil
import tarfile
import zipfile
import tempfile
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Iterable, Union, List

import pandas
import pandas as pd
import psycopg2

from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.dataproducts.utils import resolve_dataset
from masschange.db.conn import get_db_cursor, get_db_connection
from masschange.ingest.executor.filter import EqualsFilter, DataFilter
from masschange.ingest.overwritebehaviours import ReaderOverwriteBehavior
from masschange.utils.misc import get_human_readable_elapsed_since
from masschange.db.data.caggs import refresh_continuous_aggregates
from masschange.db.ensure import ensure_database_exists
from masschange.db.data.ensure import ensure_dataset_table_exists, ensure_dataset_caggs_exist
from masschange.db.metadata.ensure import ensure_metadata_tables_exist
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree, order_filepaths_by_filename
from masschange.db.metadata.update import update_metadata
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
        if tar_fp.endswith('.zip'):
            with zipfile.ZipFile(tar_fp, 'r') as zf:
                zf.extractall(temp_dir)
        else:
            with tarfile.open(tar_fp) as tf:
                tf.extractall(temp_dir)

        for fp in order_filepaths_by_filename(
                enumerate_files_in_dir_tree(temp_dir, filename_match_regex, match_filename_only=True)):
            yield fp

        log.debug(f'cleaning up {temp_dir}')
        shutil.rmtree(temp_dir)

def delete_overlapping_data(dataset: Dataset, data_temporal_span: TimeSpan, src_filepath:str =None):

    if dataset.product.get_reader().OVERWRITE_BEHAVIOR ==  ReaderOverwriteBehavior.OVERWRITE_ROWS_WITH_MATCHING_SRC_FNAME:
        delete_overlapping_data_by_source_fname(dataset, os.path.basename(src_filepath))
    else:
        delete_overlapping_data_by_temporal_bounds(dataset, data_temporal_span)

def delete_overlapping_data_by_temporal_bounds(dataset: Dataset, data_temporal_span: TimeSpan):
    table_name = dataset.get_table_name()
    with get_db_cursor() as cur:
        sql = f"""
            DELETE 
            FROM {table_name}
                WHERE   {dataset.product.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                    AND {dataset.product.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                """
        cur.execute(sql, {'from_dt': data_temporal_span.begin, 'to_dt': data_temporal_span.end})
        log.debug(f'purged data from {table_name} for span {data_temporal_span}')

def delete_overlapping_data_by_source_fname(dataset: Dataset, source_file_name: str):
    '''
    This function is designed for inputs where data of the same product type are distributed across multiple files,
    all covering approximately the same time range. In such scenarios, it is not possible to rely on data span
    for removing duplicated entries, so the name of the source file is used instead to prevent ingesting
    the same file multiple times.

    This method is particularly useful for sources like OFFRED data, which includes entries of the same product type
    across overlapping files.
    '''

    # sanity check: make sure that a valid source file column name is defined in the reader
    source_file_column_name = dataset.product.get_reader().SOURCE_FILE_COLUMN_NAME
    if ((source_file_column_name is None) or
            (not source_file_column_name in [f.name for f in dataset.product.get_available_fields()])):
        raise RuntimeError(f" {dataset.product.get_reader().__class__.__name__} "
                           f" should set SOURCE_FILE_COLUMN_NAME to a valid column name for the source files...")

    table_name = dataset.get_table_name()

    with get_db_cursor() as cur:
        sql = f"""
            DELETE 
            FROM {table_name}
                WHERE   {source_file_column_name} = '{source_file_name}'
                """
        cur.execute(sql)
        log.debug(f'purged data from {table_name} for source file name {source_file_name}')

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

def get_data_filters(dataset: Dataset ) -> Union[List[DataFilter], None]:
    """
    Create a list of data filters that the reader will apply to the raw data.
    Filters are different for different datasets

    Parameters
    ----------
    dataset : Dataset.

    Returns
    -------
    List of data filters that implements DataFilter interface, or None
    """
    filters = None
    # So far, only one filter was requested by sci team:
    # for Level 1B products, remove rows where 'time_ref' is not equals 'G'
    if dataset.product.processing_level:
        if dataset.product.processing_level.upper() == '1B':
            filters = [EqualsFilter('time_ref', 'G')]
    return filters


def ingest_file_to_db(product: DataProduct, src_filepath: Union[str, Path]):
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')

    src_filepath = str(src_filepath)

    reader = product.get_reader()

    dataset = DatasetFactory.create(product, reader.extract_dataset_version(src_filepath),
                                    reader.extract_instrument_id(src_filepath))

    filters = get_data_filters(dataset)

    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath, filters=filters)
    data_temporal_span = TimeSpan(begin=min(pd_df[product.TIMESTAMP_COLUMN_NAME]).replace(tzinfo=timezone.utc),
                                  end=max(pd_df[product.TIMESTAMP_COLUMN_NAME]).replace(tzinfo=timezone.utc))
    channel_ids = {f: set(pd_df[f.name]) for f in dataset.product.get_available_fields() if f.is_channel_id_column}

    ensure_dataset_table_exists(dataset)
    ensure_dataset_caggs_exist(dataset)

    table_name = dataset.get_table_name()
    delete_overlapping_data(dataset, data_temporal_span, os.path.basename(src_filepath))

    ingest_df(pd_df, table_name)
    refresh_continuous_aggregates(dataset, data_temporal_span)
    update_metadata(dataset, data_span=data_temporal_span, channel_ids=channel_ids)

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

    configure_root_logger(log_filepath=None)

    database_name = os.environ['TSDB_DATABASE']
    ensure_database_exists(database_name)
    ensure_metadata_tables_exist()

    start = datetime.now()
    log.info(f'starting ingest of {args.dataset.get_full_id()} from {args.src} begin')
    run(args.dataset, args.src, data_is_zipped=args.target_zipped_data)
    log.info(
        f'ingest of {args.dataset.get_full_id()} from {args.src} completed in {get_human_readable_elapsed_since(start)}')

    exit(0)
