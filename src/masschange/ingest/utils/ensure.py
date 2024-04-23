import logging
import os

import psycopg2

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.datasets.utils import get_time_series_dataset_classes
from masschange.db import get_db_connection
from masschange.ingest.utils.caggs import get_extant_continuous_aggregates, delete_caggs, \
    get_continuous_aggregate_create_statements, refresh_continuous_aggregates
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
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS postgis')

    conn.close()


def ensure_table_exists(dataset: TimeSeriesDataset, stream_id: str) -> None:
    """
    Ensure that the table for this dataset exists, creating and configuring the table if it does not.
    """
    table_name = dataset.get_table_name(stream_id)
    log.info(f'Ensuring table_name exists: "{table_name}"')

    timestamp_column_name = dataset.TIMESTAMP_COLUMN_NAME
    with get_db_connection() as conn, conn.cursor() as cur:
        try:
            sql = f"""
            {dataset.get_sql_table_create_statement(stream_id)}
            
            select create_hypertable('{table_name}','{timestamp_column_name}');
            select set_chunk_time_interval('{table_name}', interval '24 hours');
            """
            cur.execute(sql)

            # TODO: eventually, all datasets will need a geometry column
            # For now, add it for GraceFOGnv1ADataset only
            if dataset.id_suffix == 'GNV1A':
                geo_sql = f"""
                select AddGeometryColumn('{table_name}', 'location', 4326, 'POINT', 2 );    
                """
                cur.execute(geo_sql)

            conn.commit()
            log.info(f'Created new table: "{table_name}"')
        except psycopg2.errors.DuplicateTable:
            pass


def ensure_continuous_aggregates(dataset: TimeSeriesDataset, stream_id: str) -> None:
    """
    Ensure that the table for this dataset and stream_id's data exists, creating the table and all necessary views if
    the table doesn't exist.  Does not check for or fix partial existence (i.e. table exists but views do not).
    """
    log.info(f'Ensuring expected continuous aggregates exist for dataset "{dataset.get_full_id()}"')

    expected_dataset_caggs = {dataset.get_table_or_view_name(stream_id, level) for level in
                              dataset.get_available_aggregation_levels()}
    extant_dataset_caggs = get_extant_continuous_aggregates(dataset, stream_id)

    if expected_dataset_caggs != extant_dataset_caggs:
        if expected_dataset_caggs.issubset(extant_dataset_caggs):
            extraneous_caggs = extant_dataset_caggs.difference(expected_dataset_caggs)
            log.info(f'Extraneous caggs found - deleting {sorted(extraneous_caggs)}')
            delete_caggs(extraneous_caggs)
        else:
            log.info(
                f'Regenerating all dataset caggs due to mismatch between expected/extant caggs (expected {sorted(expected_dataset_caggs)}, '
                f'got {sorted(extant_dataset_caggs)})')

            delete_caggs(extant_dataset_caggs)

            cagg_create_statements = [get_continuous_aggregate_create_statements(dataset, stream_id, agg_level) for
                                      agg_level in
                                      dataset.get_available_aggregation_levels()]
            with get_db_connection() as conn, conn.cursor() as cur:
                sql = '\n'.join(cagg_create_statements)
                cur.execute(sql)
                conn.commit()
                log.info(f'Created continous aggregates for dataset "{dataset.get_full_id()}"')

            refresh_continuous_aggregates(dataset, stream_id)


def ensure_all_db_state(database_name: str):
    ensure_database_exists(database_name)

    for cls in get_time_series_dataset_classes():
        ds = cls()
        for stream_id in ds.stream_ids:
            ensure_table_exists(ds, stream_id)
            ensure_continuous_aggregates(ds, stream_id)


if __name__ == '__main__':
    configure_root_logger()

    database_name = os.environ['TSDB_DATABASE']
    logging.info(f'Ensuring all database state for db "{database_name}"')
    ensure_all_db_state(database_name)
