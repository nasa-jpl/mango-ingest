import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.hrt1b import GraceFOHrt1BDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOHrt1BDatasetReaderTestCase(DatasetReaderTestCaseBase):
    """
    This test also tests data filters.
    The HRT1B_2020-01-15_C_04.txt is modified to replace 'G' for 'time_ref' with other letter in the first row
    and 3 other rows.
    So, the 4 rows including the first row get filtered out, the record in the table starts with the second row,
    and the number of rows in the table is 96 instead of 100
    """
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
                            str, bool, bool, bool, bool, bool, bool, bool, bool, datetime]
    expected_table_row_counts = [96, 100]
    expected_table_first_rows = [
        (632318456, 501580, 'C',
        5.54562520980835, 16.39182090759277, 5.709863185882568,
        12.76247024536133, 1.530385971069336, 0, 0, 0, 0, 0, 0 ,0, 0, 0 ,
        1.583472967147827, 22.33643913269043, 25.78952026367188 ,
        22.3588695526123, 25.89521980285645 ,32.31447982788086 ,
        29.1464900970459, 32.23440170288086, 28.9991397857666 ,
        22.21792030334473, 25.89521980285645, 0, 13.01688003540039,
        12.2472095489502, 13.27451038360596, 12.16026020050049,
         '00000000', False, False, False, False, False, False, False, False,
         datetime(2020, 1, 15, 0, 0, 56, 501580, tzinfo=timezone.utc)),
        (632318416, 502161, 'D',
         1.469131946563721, 0.8933441042900085, 10.11532974243164,
         6.118850231170654, 6.20580005645752, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         6.16393518447876, 25.99131965637207, 21.86236953735352,
         25.81513977050781, 21.73423957824707, 32.08383941650391,
         26.62556076049805, 32.23440170288086, 26.38530921936035,
         25.8215503692627, 21.83674049377441, 0, 12.29551982879639,
         11.41958045959473, 12.01212024688721, 11.34228992462158,
         '00000000', False, False, False, False, False, False, False, False,
         datetime(2020, 1, 15, 0, 0, 16, 502161, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
