import logging
import os

import psycopg2.errors


from masschange.db.conn import get_db_connection, get_db_cursor
from masschange.db.ensure import ensure_all_db_state

log = logging.getLogger()

TEST_DATABASE_NAME = 'masschange_functional_tests'


def initDb(database_name: str):
    # Ensure test database is used
    assert database_name == TEST_DATABASE_NAME
    os.environ['TSDB_DATABASE'] = database_name

    log.info(f'Instantiating test database "{database_name}"')
    conn = get_db_connection(without_db=True)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS {database_name} WITH (FORCE);')
        cur.execute(f'CREATE DATABASE {database_name}')
    conn.close()

    with get_db_cursor(autocommit=True) as cur:
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS postgis')
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')

    ensure_all_db_state(database_name, is_database_init=True)


def destroyDb(database_name: str):
    # Ensure only test databases can be torn down
    assert database_name == TEST_DATABASE_NAME

    conn = get_db_connection(without_db=True)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE {database_name} WITH (FORCE);')
    except (psycopg2.errors.ObjectInUse, psycopg2.errors.InvalidCatalogName):
        pass
    conn.close()


def _truncate_data_tables(prefix: str):
    sql = """
        DO $do$
        DECLARE
            tbl RECORD;
        BEGIN
            FOR tbl IN
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE starts_with(tablename, %(prefix)s)
                  AND schemaname = 'public'
            LOOP
                EXECUTE format('TRUNCATE TABLE %%I.%%I CASCADE', tbl.schemaname, tbl.tablename);
            END LOOP;
        END
        $do$;
    """

    with get_db_cursor(autocommit=True) as cur:
        cur.execute(sql, {"prefix": prefix})


def truncate_all_data_tables():
    _truncate_data_tables(prefix='_meta_')
    _truncate_data_tables(prefix='gracefo_')
