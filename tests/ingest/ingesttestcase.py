import logging
import os
import unittest
from typing import Sequence, Type, Tuple

from masschange.ingest import ingest
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection

log = logging.getLogger()


class IngestTestCase(unittest.TestCase):
    dataset_cls: Type[TimeSeriesDataset]
    expected_table_names: Sequence[str]
    expected_field_types: Sequence[Type]
    expected_table_row_counts: Sequence[int]
    expected_table_first_rows: Sequence[Tuple]

    target_database = 'masschange_functional_tests'

    @classmethod
    def setUpClass(cls):
        # Ensure test database is used
        os.environ['TSDB_DATABASE'] = cls.target_database

        if cls != IngestTestCase:

            log.info(f'Instantiating test database "{cls.target_database}"')
            conn = get_db_connection(without_db=True)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE IF EXISTS {cls.target_database} WITH (FORCE);')
                cur.execute(f'CREATE DATABASE {cls.target_database}')
                cur.execute(f'CREATE EXTENSION IF NOT EXISTS timescaledb')
            conn.close()

            ingest.run(dataset=cls.dataset_cls(), src=os.path.abspath('./tests/input_data'), data_is_zipped=True)

    @classmethod
    def tearDownClass(cls):
        if cls != IngestTestCase:
            conn = get_db_connection(without_db=True)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE {cls.target_database} WITH (FORCE);')
            conn.close()

    def test_has_expected_row_counts(self):
        if self.__class__ != IngestTestCase:
            with get_db_connection() as conn, conn.cursor() as cur:
                for i, table_name in enumerate(self.expected_table_names):
                    expected_row_count_for_table = self.expected_table_row_counts[i]
                    cur.execute(f'SELECT COUNT(*) from {table_name};')
                    table_row_count = cur.fetchone()[0]
                    self.assertEqual(expected_row_count_for_table, table_row_count)

    def test_has_expected_schema(self):
        if self.__class__ != IngestTestCase:
            with get_db_connection() as conn, conn.cursor() as cur:
                for i, table_name in enumerate(self.expected_table_names):
                    cur.execute(f'SELECT * from {table_name};')
                    table_first_row = cur.fetchone()

                    self.assertEqual(len(self.expected_field_types), len(table_first_row),
                                     'row has expected number of columns')
                    for column_i, expected_type in enumerate(self.expected_field_types):
                        value = table_first_row[column_i]
                        self.assertIsInstance(value, expected_type,
                                              f'column {column_i} has expected type {expected_type}')

    def test_has_expected_table_names(self):
        if self.__class__ != IngestTestCase:
            with get_db_connection() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE';
                """)

                table_names = set(row[0] for row in cur.fetchall())
                for table_name in self.expected_table_names:
                    self.assertIn(table_name, table_names)

    def test_has_expected_table_first_rows(self):
        if self.__class__ != IngestTestCase:
            with get_db_connection() as conn, conn.cursor() as cur:
                for i, table_name in enumerate(self.expected_table_names):
                    expected_first_row = self.expected_table_first_rows[i]
                    cur.execute(f'SELECT * from {table_name};')
                    table_first_row = cur.fetchone()

                    self.assertEqual(expected_first_row, table_first_row)
