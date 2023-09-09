import argparse
import logging
import os
import re
import shutil
import tarfile
import tempfile
from datetime import datetime
from typing import Iterable

import pandas as pd
import pyspark.sql
from pyspark.sql import SparkSession

from masschange.ingest.utils import get_configured_logger
from masschange.datasets.interface import get_spark_session
from masschange.ingest.datasets.gracefo import reader
from masschange.ingest.datasets.gracefo.constants import INPUT_FILE_DEFAULT_REGEX, \
    ZIPPED_INPUT_FILE_DEFAULT_REGEX
from masschange.ingest.datasets.constants import PARQUET_TEMPORAL_PARTITION_KEY
from masschange.ingest.utils.benchmarking import get_human_readable_elapsed_since
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree

log = get_configured_logger()


def run(src: str, dest: str, data_is_zipped: bool = True):
    """

    Parameters
    ----------
    src - the directory containing input files, identified by ACC1A_{YYYY-MM-DD}_{satellite_id}_04.txt
    dest - the destination parquet root directory

    Returns
    -------

    """

    spark = get_spark_session()

    log.info(f'ingesting data from {src}')
    log.info(f'targeting {"zipped" if data_is_zipped else "non-zipped"} data')
    log.info(f'writing output to parquet root: {dest}')
    target_filepaths = get_zipped_input_iterable(src) if data_is_zipped else enumerate_input_filepaths(src)
    for fp in target_filepaths:
        ingest_file_to_parquet(spark, fp, dest)


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


def ingest_file_to_parquet(spark: SparkSession, src_filepath: str, dest_parquet_root: str):
    """

    Parameters
    ----------
    spark_session
    src_filepath
    dest_parquet_root

    Returns
    -------

    """
    if log.isEnabledFor(logging.DEBUG):
        log.debug(f'ingesting file: {src_filepath}')
    else:
        log.info(f'ingesting file: {os.path.split(src_filepath)[-1]}')

    dataset_label = 'gracefo_1a'
    full_resolution_parquet_root = os.path.join(dest_parquet_root, dataset_label)

    pd_df: pd.DataFrame = reader.load_data_from_file(src_filepath)
    spark_df: pyspark.sql.DataFrame = spark.createDataFrame(pd_df)
    spark_df.write \
        .format('parquet') \
        .partitionBy(PARQUET_TEMPORAL_PARTITION_KEY) \
        .bucketBy(1, 'rcvtime') \
        .sortBy('rcvtime') \
        .option('path', full_resolution_parquet_root) \
        .mode('append') \
        .saveAsTable(dataset_label)

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
