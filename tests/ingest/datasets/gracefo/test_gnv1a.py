import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.gnv1a import GraceFOGnv1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOGnv1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOGnv1ADataset
    expected_table_names = ['gracefo_gnv1a_04_c', 'gracefo_gnv1a_04_d']
    expected_field_types = [int, int, str,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, str,
                            datetime,
                            str]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 10, 'C',
         5.107140064239502, 1.880059957504272, 0,
         562544.3952235891, -2051265.134819092, -6546778.197176656,
         0.5296178460121155, 0.587357759475708, 1.49042022228241,
         -1636.570070981475, 7025.480440801754, -2348.529043447772,
         0.005298092495650053, 0.005876647308468819, 0.01492065656930208,
         0.01667117358522485, 2.754388273018549e-09, 1.572264764754594e-08,
         2.756986826335517e-11,   '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
         '0101000020E61000008351499D80AA52C0499D8026C20652C0'),
         (738849600, 11, 'D',
         5.683535099029541, 1.428586006164551, 0,
         595972.4408544349, - 2209983.177703416, - 6491786.117647586,
         0.5123481750488281, 0.5861619114875793, 1.044809579849243,
         - 1632.445225803575, 6964.551557743021, - 2527.930929881756,
         0.005124685820192099, 0.005863454192876816, 0.01045407168567181,
         0.01725577728485153, 1.950082539892151e-09, 1.638214866571998e-08,
         1.95077305004121e-11,   '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
         '0101000020E610000075029A081BBA52C01CEBE2361AAC51C0'
          )
    ]

    if __name__ == '__main__':
        unittest.main()
