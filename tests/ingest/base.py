import logging
import os
import unittest

import psycopg2.errors

from masschange.db.conn import get_db_cursor, get_db_connection
from masschange.db.ensure import ensure_all_db_state

log = logging.getLogger()

# Two databases are used - one for reader tests which are guaranteed not to interact with one another, and another for
# tests which require an empty database to function correctly
reader_tests_target_database = 'masschange_reader_functional_tests'
isolated_tests_target_database = 'masschange_isolated_functional_tests'
test_database_names = {reader_tests_target_database, isolated_tests_target_database}


def setUp(database_name: str):
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


def tearDown(database_name: str):
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
for database_name in test_database_names:
    tearDown(database_name)
    setUp(database_name)
#### FRESH DATABASE INITIALIZATION END ####


class IngestTestCaseBase(unittest.TestCase):
    """
    Defines a base class for test-cases of general ingestion behaviour, which may mutate db data as a side effect.
    This should be used for all ingestion-related test-cases except for the single test-case which is defined for each
    DataFileReader to ensure they parse the test input files correctly
    """

    target_database = isolated_tests_target_database

    @classmethod
    def setUpClass(cls) -> None:
        setUp(cls.target_database)

    @classmethod
    def tearDownClass(cls) -> None:
        tearDown(cls.target_database)


class ReaderTestCaseBase(unittest.TestCase):
    """
    Defines a base class for reader implementation test-cases which are guaranteed not to interfere with one another.
    This exists to prevent the need to initialize/ingest the test dataset once for every reader class, which is
    time-consuming and not necessary for this specific type of test-case.
    """

    target_database = reader_tests_target_database

