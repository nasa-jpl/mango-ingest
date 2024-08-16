import logging
import os
import unittest
from typing import Sequence, Type, Tuple

from masschange.ingest import ingest
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.db.utils import get_db_connection
from tests.ingest.base import IngestTestCaseBase

log = logging.getLogger()


class DatasetIngestTestCaseBase(IngestTestCaseBase):
    """
    Exists to allow efficient definition of regression tests for specific TimeSeriesDataset subclasses by inheriting
    from this base class and assigning values for the member variables.
    """
    dataset_cls: Type[TimeSeriesDataProduct]
    expected_table_names: Sequence[str]
    expected_field_types: Sequence[Type]
    expected_table_row_counts: Sequence[int]
    expected_table_first_rows: Sequence[Tuple]

    target_database = 'masschange_functional_tests'

    def skip_if_abstract(self):
        """Prevent test case from running on instantiation of this abstract test class"""
        if self.__class__ == DatasetIngestTestCaseBase:
            self.skipTest('abstract test case class')


    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Prevent attempt to run ingest on abstract base class
        if cls is not DatasetIngestTestCaseBase:
            ingest.run(product=cls.dataset_cls(), src=os.path.abspath('./tests/input_data'), data_is_zipped=True)

    def test_has_expected_row_counts(self):
        self.skip_if_abstract()
        with get_db_connection() as conn, conn.cursor() as cur:
            for i, table_name in enumerate(self.expected_table_names):
                expected_row_count_for_table = self.expected_table_row_counts[i]
                cur.execute(f'SELECT COUNT(*) from {table_name};')
                table_row_count = cur.fetchone()[0]
                self.assertEqual(expected_row_count_for_table, table_row_count)

    def test_has_expected_schema(self):
        self.skip_if_abstract()
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
        self.skip_if_abstract()
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
        self.skip_if_abstract()
        with get_db_connection() as conn, conn.cursor() as cur:
                for i, table_name in enumerate(self.expected_table_names):
                    expected_first_row = self.expected_table_first_rows[i]
                    cur.execute(f'SELECT * from {table_name};')
                    table_first_row = cur.fetchone()

                    self.assertEqual(expected_first_row, table_first_row)

class DatasetIngestTestCaseBaseForUnzippedData(DatasetIngestTestCaseBase):
    """
    Base class for testing unzipped data located in /tests/input_data/test_unzipped directory
    """
    @classmethod
    def setUpClass(cls):
        super(DatasetIngestTestCaseBase, cls).setUpClass()
        ingest.run(product=cls.dataset_cls(), src=os.path.abspath('./tests/input_data/test_unzipped'),
                   data_is_zipped=False)