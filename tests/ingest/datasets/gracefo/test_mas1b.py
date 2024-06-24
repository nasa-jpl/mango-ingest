import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.mas1b import GraceFOMas1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOMas1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOMas1BDataProduct
    expected_table_names = ['gracefo_mas1b_04_c', 'gracefo_mas1b_04_d']

    expected_field_types = [int, int, str, str, str, datetime,
                            Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)]
                            ]
    expected_table_row_counts = [24, 24]
    expected_table_first_rows = [
        (738853199,  983274,  'C',  '00000000', '11000000', datetime(2023, 6, 1, 0, 59, 59, 983274, tzinfo=timezone.utc),
         None, None, None, None, None,None,
         13.43510675514573, 13.45650041507746),
        (738853199,  982687, 'D',  '00000000', '11000000', datetime(2023, 6, 1, 0, 59, 59, 982687, tzinfo=timezone.utc),
         None, None, None, None, None, None,
         13.66314067421178, 13.82399457459087)
    ]

if __name__ == '__main__':
    unittest.main()
