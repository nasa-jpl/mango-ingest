import logging
import os
import unittest

import psycopg2.errors

from masschange.db.conn import get_db_cursor, get_db_connection
from masschange.db.ensure import ensure_all_db_state

log = logging.getLogger()


class IngestTestCaseBase(unittest.TestCase):
    """
    Defines a base class for test cases which interact with the database - handles test db setup/teardown.
    """

    target_database = 'masschange_functional_tests'

    @classmethod
    def setUpClass(cls):
        # Ensure test database is used
        os.environ['TSDB_DATABASE'] = cls.target_database

        log.info(f'Instantiating test database "{cls.target_database}"')
        conn = get_db_connection(without_db=True)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS {cls.target_database} WITH (FORCE);')
            cur.execute(f'CREATE DATABASE {cls.target_database}')
        conn.close()

        with get_db_cursor(autocommit=True) as cur:
            cur.execute(f'CREATE EXTENSION IF NOT EXISTS postgis')
            cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')

        ensure_all_db_state(cls.target_database)

    @classmethod
    def tearDownClass(cls):
        conn = get_db_connection(without_db=True)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE {cls.target_database} WITH (FORCE);')
        except psycopg2.errors.ObjectInUse:
            pass
        conn.close()
