import logging
import os

import psycopg2

from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.dataproducts.utils import get_dataproducts
from masschange.db.conn import get_db_connection, get_db_cursor
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

    conn.close()

    with get_db_cursor(autocommit=True) as cur:
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS postgis')
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')


def ensure_dataset(dataset: Dataset) -> None:
    ensure_dataset_table_exists(dataset)
    if dataset.is_time_series_dataset():
        ensure_dataset_caggs_exist(dataset)


def initialize_dataset(dataset: Dataset):
    log.info(f'Ensuring table for {dataset.get_table_name()}')
    ensure_dataset_table_exists(dataset)
    if dataset.is_time_series_dataset():
        log.info(f'Ensuring caggs for {dataset.get_table_name()}')
        ensure_dataset_caggs_exist(dataset)


def regenerate_dataset_metadata(dataset: Dataset, populate_dataproducts_versions: bool = False):
    log.info(f'Updating metadata for {dataset.get_table_name()}')
    data_span = dataset.get_data_span()
    channel_ids = dataset._enumerate_channel_id_values()
    update_metadata(dataset, data_span=data_span, channel_ids=channel_ids,
                    populate_versions=populate_dataproducts_versions,
                    accumulate_data_span=False)


def ensure_prototype_json_store():
    with get_db_cursor() as cur:
        sql = '''
              create table if not exists public._jsonstore
              (
                  id      varchar(64) primary key not null,
                  content jsonb
              );
              comment on table public._jsonstore is 'storage for arbitrary non-sensitive JSON objects by the frontend'; \
              '''

        cur.execute(sql)
        log.info(f'Ensured presence of prototype jsonstore table')


def ensure_all_db_state(database_name: str, is_database_init: bool = False):
    """
    Ensure that database is consistent and up-to-date (within limits)
    :param database_name:
    :param is_database_init: Set True to skip superfluous db calls for when there is no data.
    :return:
    """
    ensure_database_exists(database_name)
    ensure_metadata_tables_exist()
    ensure_prototype_json_store()

    ensure_ingest_manager_tables_exist()

    if not is_database_init:
        for product in get_dataproducts():
            product.ensure()
            for version in product.get_available_versions():
                for instrument_id in product.instrument_ids:
                    dataset = DatasetFactory.create(product, version, instrument_id)
                    initialize_dataset(dataset)


if __name__ == '__main__':
    configure_root_logger(log_filepath=None)

    database_name = os.environ['TSDB_DATABASE']
    logging.info(f'Ensuring all database state for db "{database_name}"')
    ensure_all_db_state(database_name)
