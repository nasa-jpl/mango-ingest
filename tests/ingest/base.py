import unittest

from tests.ingest.utils.db import initDb, destroyDb, truncate_all_data_tables, TEST_DATABASE_NAME

destroyDb(TEST_DATABASE_NAME)
initDb(TEST_DATABASE_NAME)


class IngestTestCaseBase(unittest.TestCase):
    """
    Defines a base class for test-cases of general ingestion behaviour, which may mutate db data as a side effect.
    This should be used for all ingestion-related test-cases except for the single test-case which is defined for each
    DataFileReader to ensure they parse the test input files correctly
    """

    target_database = TEST_DATABASE_NAME

    def setUp(self) -> None:
        super().setUp()
        truncate_all_data_tables()

    @classmethod
    def tearDownClass(cls) -> None:
        truncate_all_data_tables()
        super().tearDownClass()


class ReaderTestCaseBase(unittest.TestCase):
    """
    Defines a base class for reader implementation test-cases which are guaranteed not to interfere with one another.
    This exists to prevent the need to initialize/ingest the test dataset once for every reader class, which is
    time-consuming and not necessary for this specific type of test-case.
    """

    target_database = TEST_DATABASE_NAME
