import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.hrt1b import GraceFOHrt1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOHrt1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOHrt1BDataProduct
    expected_table_names = ['gracefo_hrt1b_04_c', 'gracefo_hrt1b_04_d']
    expected_field_types = [int, int, str,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (632318424, 501580, 'C',
         5.542404174804688, 16.37571907043457, 5.716303825378418,
         12.76568984985352, 1.526301980018616, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         1.583472967147827, 22.3332405090332, 25.78310966491699,
         22.36527061462402, 25.8984203338623, 32.33049011230469,
         29.25219917297363, 32.23759841918945, 29.11125946044922,
         22.21792030334473, 25.89521980285645, 0, 13.01688003540039,
         12.25364971160889, 13.27128982543945, 12.16026020050049,
         '00000000', datetime(2020, 1, 15, 0, 0, 24, 501580, tzinfo=timezone.utc)),
        (632318416, 502161, 'D',
         1.469131946563721, 0.8933441042900085, 10.11532974243164,
         6.118850231170654, 6.20580005645752, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         6.16393518447876, 25.99131965637207, 21.86236953735352,
         25.81513977050781, 21.73423957824707, 32.08383941650391,
         26.62556076049805, 32.23440170288086, 26.38530921936035,
         25.8215503692627, 21.83674049377441, 0, 12.29551982879639,
         11.41958045959473, 12.01212024688721, 11.34228992462158,
         '00000000', datetime(2020, 1, 15, 0, 0, 16, 502161, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
