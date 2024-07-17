import logging
import os
from masschange.ingest import ingest
from tests.ingest.base import IngestTestCaseBase
from masschange.ingest.ingest import ingest_file_to_db
from masschange.ingest.errors import EmptyProductException
from masschange.dataproducts.implementations.gracefo.primary.act1b import GraceFOAct1BDataProduct
from masschange.dataproducts.implementations.gracefo.primary.ahk1a import GraceFOAhk1ADataProduct
from masschange.ingest.datafilereaders.gracefo.primary.gracefoact1b import GraceFOAct1BDataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.gracefoahk1a import GraceFOAhk1ADataFileReader

from masschange.db import get_db_connection

log = logging.getLogger()

class IngestEmptyProductTestCase(IngestTestCaseBase):

    def table_exists(self, table_name):
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public';
            """)
            table_names = set(row[0] for row in cur.fetchall())
            if table_name in table_names:
                return True
            else:
                return False

    def test_ingest_empty_product(self):
        # Test for empty files with fixed schema (ACT1B) and with variable schema (AHK1A)
        input_dir = './tests/input_data/ingest/test_empty_product'

        input_filepaths = ['ACT1B_2024-04-01_D_04.txt', 'AHK1A_2023-06-01_C_04.txt']
        products = [GraceFOAct1BDataProduct(), GraceFOAhk1ADataProduct()]
        readers = [GraceFOAct1BDataFileReader(), GraceFOAhk1ADataFileReader()]
        table_names = ['gracefo_act1b_04_c', 'gracefo_ahk1a_04_c']
        for f, p, r, tn in zip(input_filepaths, products, readers, table_names):
            fp = os.path.join(os.path.abspath(input_dir), f)
            self.assertRaises(EmptyProductException, r.load_data_from_file, fp)
            self.assertRaises(EmptyProductException, ingest_file_to_db, p, fp)

            # check that ingest runs without exception
            ingest.run(product=p, src=os.path.abspath(input_dir), data_is_zipped=False)

            # check that the table was not created
            self.assertFalse(self.table_exists(tn))


