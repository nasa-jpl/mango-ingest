import unittest
from datetime import datetime, timezone
from typing import Union
from masschange.dataproducts.implementations.gracefo.offred.offred_num import GraceFOOffredNumDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOOffredNumDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/offred/'
    data_is_zipped = False
    maxDiff = None
    dataset_cls = GraceFOOffredNumDataProduct
    expected_table_names = ['gracefo_offred_num_4_gf1']
    expected_field_types = [str, int, int, str, str, Union[str, None], float, datetime]
    expected_table_row_counts = [72]
    # Diff between UTC and GPS is 18 sec on June 1st, 2022
    expected_table_first_rows = [
        ('2022.152.06.20.31.886', 1338099649,	886,	'GPS', 'TST67157.rn', None, 'ACQVLD',
         datetime(2022, 6, 1, 6, 20, 49, 886000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
