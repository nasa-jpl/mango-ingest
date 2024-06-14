import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.gnv1b import GraceFOGnv1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOGnv1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOGnv1BDataProduct
    expected_table_names = ['gracefo_gnv1b_04_c', 'gracefo_gnv1b_04_d']
    expected_field_types = [int, str, str,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            str, str, str, datetime,
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 'C', 'E',
         562516.6928322315, -2051148.109060925, -6546817.760876593,
         0.0005730634910580956, 0.0007019597040754808, 0.0009904123183782341,
         -1636.58495865621, 7025.557923232562, -2348.304620900529,
         1.355885561557259e-06, 1.963835415859967e-06, 2.021675127198291e-06,
         '00000000', '0101000020E61000005A172BB87EAA52C0602B996BD20652C0',
         'A', datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
         (738849600, 'D', 'E',
          595944.1677232814, -2209863.655682023, -6491827.337483045,
          0.0006038281162352046, 0.0007449456765001425, 0.001004714080891457,
          -1632.457007090984, 6964.636302676829, -2527.646764562352,
          1.334523249336289e-06, 1.998181731481052e-06, 2.083411888826681e-06,
          '00000000', '0101000020E610000031DEE96C19BA52C00CF49F942BAC51C0',
          'A', datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
          )
    ]
if __name__ == '__main__':
    unittest.main()
