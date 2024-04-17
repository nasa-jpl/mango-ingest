import logging
import os
import unittest

import psycopg2.errors

from masschange.db import get_db_connection
from masschange.ingest.utils.ensure import ensure_all_db_state

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
        conn.close()

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
