import argparse
import logging
import os
import shutil
import tarfile
import zipfile
import tempfile
from datetime import datetime, timezone, timedelta
from io import StringIO
from pathlib import Path
from typing import Iterable, Union, List, Generator

import pandas
import pandas as pd
import psycopg2

import time

from collections.abc import Generator

from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.dataproducts.utils import resolve_dataset
from masschange.db.conn import get_db_cursor, get_db_connection
from masschange.ingest.executor.datafilereaders.filter import EqualsFilter, DataFilter
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

logging.root.setLevel(logging.DEBUG)
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

    # Special case for OFFRED
    if "OFFRED" in product.get_full_id():
        ingest_offred(product, src)
    else:
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

def ingest_offred(product: DataProduct, src: Union[str, Path]):
    reader = product.get_reader()
    #zipped_regex = reader.get_zipped_input_file_default_regex()
    zipped_regex = reader.get_zipped_input_file_default_regex() # default is a zipped file
    unzipped_regex = reader.get_input_file_default_regex()
    ref_epoch = reader.get_reference_epoch()
    for fp, do_agg, zip_start_time_sec, zip_end_time_sec in get_zipped_input_iterable_for_offred(src, zipped_regex,
                                                                                                 unzipped_regex):
        log.debug(f'Now processing unzipped file {fp}')
        temp_span = TimeSpan(begin=(ref_epoch + timedelta(seconds=zip_start_time_sec)).replace(tzinfo=timezone.utc),
                             end=(ref_epoch + timedelta(seconds=zip_end_time_sec)).replace(tzinfo=timezone.utc))
        try:
            ingest_offred_to_db(product, fp, do_aggregate=do_agg, data_temporal_span=temp_span)
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

def get_zipped_input_iterable_for_offred(root_dir: str,
                              enclosing_filename_match_regex: str,
                              filename_match_regex: str) -> Generator[
    tuple[str, bool, str | None | int, str | None | int], None, None]:
    """
    Given a root_dir containing data tarballs, provide a transparently-iterable collection of data files matching
    filename_match_regex.
    For OFFRED, iterator returns a tuple: path to unzipped file, flag that indicates if this file is a last file
    in the zip file, earlies and latest time for the data in the zip file.
    We wnt to treat the last file differently and run aggregation on this file.
    The earliest and latest times will be used as a dataset time range for the aggregation.
    TODO: add dataset time span!!!!

    N.B. THIS APPROACH MINIMIZES ADDITIONAL DISK USE BUT CANNOT BE USED WITH CONCURRENCY

    Parameters
    ----------
    root_dir
    enclosing_filename_match_regex
    filename_match_regex

    Returns
    -------

    """
    log.info(f'Entering get_zipped_input_iterable_for_offred, root dir: {root_dir}')
    log.debug(f'enclosing_filename_match_regex: {enclosing_filename_match_regex}')
    log.debug(f'filename_match_regex: {filename_match_regex}')

    # List everything, then keep only the files
    files = [f for f in os.listdir(root_dir) if os.path.isfile(os.path.join(root_dir, f))]
    log.debug(f'files in root dir: {files}')

    for tar_fp in order_filepaths_by_filename(
            enumerate_files_in_dir_tree(root_dir, enclosing_filename_match_regex, match_filename_only=True)):
        # Extract files to a temp directory located in the same directory as zip file.
        # In case of dockerized ingest, the temp directory will be created in the mounted staging area,
        # in the same directory as zip file, and will be cleaned out after ingestion
        # TODO: This assumes that the root directory is writable, which is the case for dockerized ingest,
        # but not necessary for using ingest.py directly.
        # Add a flag to switch between default location of tmp dir and root_dir location?
        with tempfile.TemporaryDirectory(dir = root_dir) as temp_dir:
            # create the temp dir in context, so it will be cleaned out even on failure.
            # The original zip file will still remain

            log.debug(f'extracting contents of {tar_fp} to {temp_dir}')

            with zipfile.ZipFile(tar_fp, 'r') as zf:
                zf.extractall(temp_dir)

            # Evaluate the inner iterator into a list
            extracted_files = list(order_filepaths_by_filename(
                enumerate_files_in_dir_tree(temp_dir, filename_match_regex, match_filename_only=True)))

            total_files = len(extracted_files)

            # Iterate through the list and flag the last item
            zip_start_time = 0
            zip_end_time = 0
            for i, fp in enumerate(extracted_files):
                is_last = (i == total_files - 1)

                start_time = time.perf_counter()

                file_start_time, file_end_time = _get_start_end_tai_sec(fp)
                end_time = time.perf_counter()
                print(f"Execution time for '_get_start_end_tai_sec': {end_time - start_time:.4f} seconds")
                if  zip_start_time == 0 or file_start_time < zip_start_time:
                    zip_start_time = file_start_time
                if zip_end_time == 0 or file_end_time > zip_end_time:
                    zip_end_time = file_end_time

                # add one second padding to start/end time because we don't read the fractional time
                yield fp, is_last, int(zip_start_time) - 1, int(zip_end_time) + 1   # Yield as a tuple

def _get_start_end_tai_sec(file_path):
    # TODO: consider use os.SEEK_END to search last line from the end if
    # this implementation takes too long.
    # It takes 0.02 sec to process 4MB file

    first_line = None
    last_line = None

    # Open the file efficiently using a context manager
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            # Check if the line starts with '20'
            if line.startswith("20"):
                if first_line is None:
                    first_line = line.strip().split()[1]
                last_line = line.strip().split()[1]  # Continuously updates to the latest match

    return first_line, last_line

def delete_overlapping_data(dataset: Dataset, data_temporal_span: TimeSpan, src_filepath:str =None):

    if dataset.product.get_reader().OVERWRITE_BEHAVIOR ==  ReaderOverwriteBehavior.OVERWRITE_ROWS_WITH_MATCHING_SRC_FNAME:
        # TODO: generalise this, instead of hardcoding for OFFRED
        maximum_expected_file_temporal_duration = timedelta(hours=4)
        # the beginning and end of the data span should be padded equally to produce a deletion span of at least
        # maximum_expected_file_temporal_duration
        end_padding = max(maximum_expected_file_temporal_duration - data_temporal_span.duration, timedelta(0)) / 2
        deletion_constraint_span = TimeSpan(begin=data_temporal_span.begin - end_padding, end=data_temporal_span.end + end_padding)
        delete_overlapping_data_by_source_fname(dataset, os.path.basename(src_filepath), limit_to_temporal_span=deletion_constraint_span)
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


def delete_overlapping_data_by_source_fname(dataset: Dataset, source_file_name: str, limit_to_temporal_span: TimeSpan):
    '''
    This function is designed for inputs where data of the same product type are distributed across multiple files,
    all covering approximately the same time range. In such scenarios, it is not possible to rely on data span
    for removing duplicated entries, so the name of the source file is used instead to prevent ingesting
    the same file multiple times.

    limit_to_temporal_span is necessary to avoid scanning irrelevant chunks, which causes linear-time blowout

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
    # TODO: this is hardcoded for OFFRED case. Make it generic!
    source_file_name_id = dataset.product.get_reader().get_source_file_id(source_file_name)

    with get_db_cursor() as cur:
        sql = f"""
            DELETE 
            FROM {table_name}
                WHERE   {source_file_column_name} = '{source_file_name_id}'
                    AND {dataset.product.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                    AND {dataset.product.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                """
        cur.execute(sql, {'from_dt': limit_to_temporal_span.begin, 'to_dt': limit_to_temporal_span.end})
        log.debug(f'purged data from {table_name} for source file name {source_file_name}')

def ingest_df(df: pandas.DataFrame, table_name: str) -> None:
    """
    see: https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
    """
    log.info(f'writing data to table {table_name}')
    start_time = time.perf_counter()
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
    end_time = time.perf_counter()
    print(f"Execution time 'ingest_df()': {end_time - start_time:.4f} seconds")

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
    ingest_start_time = time.time()
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
    ingest_end_time = time.time()
    ingest_elapsed_time = ingest_end_time - ingest_start_time
    log.info(f"Ingest time: {ingest_elapsed_time} seconds")


def ingest_offred_to_db(product: DataProduct, src_filepath: Union[str, Path], do_aggregate, data_temporal_span) -> None:
    ingest_start_time = time.time()
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')

    src_filepath = str(src_filepath)

    reader = product.get_reader()

    dataset = DatasetFactory.create(product, reader.extract_dataset_version(src_filepath),
                                    reader.extract_instrument_id(src_filepath))

    filters = get_data_filters(dataset)
    start_time = time.perf_counter()
    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath, filters=filters)
    end_time = time.perf_counter()
    print(f"Execution time 'load_data_from_file()': {end_time - start_time:.4f} seconds")

    channel_ids = {f: set(pd_df[f.name]) for f in dataset.product.get_available_fields() if f.is_channel_id_column}

    ensure_dataset_table_exists(dataset)

    if do_aggregate:
        start_time = time.perf_counter()
        ensure_dataset_caggs_exist(dataset)
        end_time = time.perf_counter()
        print(f"Execution time for ensure_dataset_caggs_exist: {end_time - start_time:.4f} seconds")

    table_name = dataset.get_table_name()
    delete_overlapping_data(dataset, data_temporal_span, os.path.basename(src_filepath))

    ingest_df(pd_df, table_name)

    if do_aggregate:
        start_time = time.perf_counter()
        refresh_continuous_aggregates(dataset, data_temporal_span)
        end_time = time.perf_counter()
        print(f"Execution time for 'refresh_continuous_aggregates': {end_time - start_time:.4f} seconds")

    start_time = time.perf_counter()
    update_metadata(dataset, data_span=data_temporal_span, channel_ids=channel_ids)
    end_time = time.perf_counter()
    print(f"Execution time for 'update_metadata': {end_time - start_time:.4f} seconds")

    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingested file: {src_filepath}')
    else:
        log.info(f'ingested file: {os.path.split(src_filepath)[-1]}')
    ingest_end_time = time.time()
    ingest_elapsed_time = ingest_end_time - ingest_start_time
    log.info(f"Ingest time: {ingest_elapsed_time} seconds")


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
