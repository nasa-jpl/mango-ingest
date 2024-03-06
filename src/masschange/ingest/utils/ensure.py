import logging

import psycopg2

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.utils.caggs import get_extant_continuous_aggregates, delete_caggs, \
    get_continuous_aggregate_create_statements, refresh_continuous_aggregates

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
        log.info(f'Mismatch between expected/extant caggs (expected {sorted(expected_dataset_caggs)}, '
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
