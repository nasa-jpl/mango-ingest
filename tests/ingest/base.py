import logging
import os
import unittest

import psycopg2.errors

from masschange.db.conn import get_db_cursor, get_db_connection
from masschange.db.ensure import ensure_all_db_state

log = logging.getLogger()

testing_database = 'masschange_functional_tests'
test_database_names = {testing_database}


def initDb(database_name: str):
    # Ensure test database is used
    assert database_name in test_database_names
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
    assert database_name in test_database_names

    conn = get_db_connection(without_db=True)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE {database_name} WITH (FORCE);')
    except (psycopg2.errors.ObjectInUse, psycopg2.errors.InvalidCatalogName):
        pass
    conn.close()


#### FRESH DATABASE INITIALIZATION BEGIN ####
destroyDb(testing_database)
initDb(testing_database)
#### FRESH DATABASE INITIALIZATION END ####

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

def _truncate_all_data_tables():
    _truncate_data_tables(prefix='_meta_')
    _truncate_data_tables(prefix='gracefo_')


class IngestTestCaseBase(unittest.TestCase):
    """
    Defines a base class for test-cases of general ingestion behaviour, which may mutate db data as a side effect.
    This should be used for all ingestion-related test-cases except for the single test-case which is defined for each
    DataFileReader to ensure they parse the test input files correctly
    """

    target_database = testing_database

    def setUp(self) -> None:
        super().setUp()
        _truncate_all_data_tables()

    @classmethod
    def tearDownClass(cls) -> None:
        _truncate_all_data_tables()
        super().tearDownClass()


class ReaderTestCaseBase(unittest.TestCase):
    """
    Defines a base class for reader implementation test-cases which are guaranteed not to interfere with one another.
    This exists to prevent the need to initialize/ingest the test dataset once for every reader class, which is
    time-consuming and not necessary for this specific type of test-case.
    """

    target_database = testing_database

