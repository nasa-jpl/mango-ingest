import logging
import os

import psycopg2

from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.utils import get_time_series_dataproduct_classes
from masschange.db.conn import get_db_connection
from masschange.db.data.ensure import ensure_dataset_table_exists, ensure_dataset_caggs_exist
from masschange.db.ingestmanagement.ensure import ensure_ingest_manager_tables_exist
from masschange.db.metadata.ensure import ensure_metadata_tables_exist

from masschange.db.metadata.update import update_metadata
from masschange.utils.logging import configure_root_logger

log = logging.getLogger()


def ensure_database_exists(db_name: str) -> None:
    conn = get_db_connection(without_db=True)
    conn.autocommit = True
    with conn.cursor() as cur:
        try:
            cur.execute(f'CREATE DATABASE {db_name}')
            log.info(f'Created missing database: "{db_name}"')
        except psycopg2.errors.DuplicateDatabase:
            pass
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS postgis')
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')
    conn.close()


def ensure_dataset(dataset: Dataset) -> None:
    ensure_dataset_table_exists(dataset)
    ensure_dataset_caggs_exist(dataset)


def initialize_dataset(dataset, populate_dataproducts_versions):
    log.info(f'Ensuring table for {dataset.get_table_name()}')
    ensure_dataset_table_exists(dataset)
    if dataset.is_time_series_dataset():
        log.info(f'Ensuring caggs for {dataset.get_table_name()}')
    ensure_dataset_caggs_exist(dataset)
    log.info(f'Updating metadata for {dataset.get_table_name()}')
    data_span = dataset.get_data_span()
    update_metadata(
        dataset,
        data_span=data_span,
        populate_versions=populate_dataproducts_versions,
        accumulate_data_span=False
    )


def ensure_all_db_state(database_name: str, populate_dataproducts_versions = False, is_database_init: bool = False):
    """
    Ensure that database is consistent and up-to-date (within limits)
    :param database_name:
    :param populate_dataproducts_versions: Not yet implemented
    :param is_database_init: Set True to skip superfluous db calls for when there is no data.
    :return:
    """
    ensure_database_exists(database_name)
    ensure_metadata_tables_exist(database_name)

    ensure_ingest_manager_tables_exist()

    if not is_database_init:
        for product_cls in get_time_series_dataproduct_classes():
            product = product_cls()
            for version in product_cls.get_available_versions():
                for instrument_id in product_cls.instrument_ids:
                    dataset = TimeSeriesDataset(product, version, instrument_id)
                    initialize_dataset(dataset, populate_dataproducts_versions)


if __name__ == '__main__':
    configure_root_logger()

    database_name = os.environ['TSDB_DATABASE']
    logging.info(f'Ensuring all database state for db "{database_name}"')
    ensure_all_db_state(database_name)
