import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.hrt1a import GraceFOHrt1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOHrt1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOHrt1ADataProduct
    expected_table_names = ['gracefo_hrt1a_04_c', 'gracefo_hrt1a_04_d']
    expected_field_types = [int, int, str,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849609, 506000, 'C',
         5.535964012145996, 16.70420074462891, 5.568167209625244,
         13.43875026702881, 1.297621011734009, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 1.34254002571106, 22.33964920043945, 25.79911994934082,
         22.36527061462402, 25.90802955627441, 32.2311897277832,
         28.32966995239258, 32.18954849243164, 28.15028953552246,
         22.22113037109375, 25.90483093261719, 0, 13.4033203125,
         12.22789001464844, 13.65773010253906, 12.13127994537354,
         '00000000', datetime(2023, 6, 1, 0, 0, 9, 506000, tzinfo=timezone.utc)),
        (738849623, 506000, 'D',
         1.367041945457458, 1.162861943244934, 10.80770969390869,
         5.909525871276855, 6.840211868286133, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 6.798347949981689, 26.24757957458496, 22.0930004119873,
         26.05537986755371, 21.97768020629883, 32.14149856567383,
         26.45257949829102, 32.30487060546875, 26.18992042541504,
         26.06819915771484, 22.07698059082031, 0, 12.32127952575684,
         12.76247024536133, 12.03466987609863, 12.66586017608643,
         '00000000', datetime(2023, 6, 1, 0, 0, 23, 506000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
