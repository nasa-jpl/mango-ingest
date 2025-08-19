import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.glv1b import GraceFOGlv1BDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOGlv1BDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOGlv1BDataProduct
    expected_table_names = ['gracefo_glv1b_04_c', 'gracefo_glv1b_04_d']
    expected_field_types = [int, str, str,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            str, str, str, bool, bool, bool, bool, bool, bool, bool, bool, datetime
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (741431100, 'C', 'E', 1766327.170161367, -4501220.723604153, 4877491.423820307,
         0.0009198433028575017, 0.001320906551201105, 0.001057326205498364, 1828.799486826192,
         -5101.815149725497, -5347.889635895815, 2.06388409305481e-06, 2.875120653425848e-06,
         3.660564223037133e-06,  '00000000', '0101000020E61000001D15E866C32451C0C8999833A1B64640', 'A',
          False, False, False, False, False, False, False, False,
         datetime(2023, 6, 30, 21, 5,  tzinfo=timezone.utc)),
         (741431100, 'D', 'E', 1717468.839451545, -4388184.432560776, 4995998.291909228,
          0.0009009556354270192, 0.001174235015068061, 0.001092019228755931, 1885.165291449159,
          -5220.688201006816, -5211.462253159052, 2.124758506155336e-06, 2.975143542125805e-06,
          3.786591691534089e-06,  '00000000', '0101000020E61000006B181534062851C0224B2DCB086D4740', 'A',
           False, False, False, False, False, False, False, False,
          datetime(2023, 6, 30, 21, 5, tzinfo=timezone.utc)
          )
    ]
if __name__ == '__main__':
    unittest.main()
