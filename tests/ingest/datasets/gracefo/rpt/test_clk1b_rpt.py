import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.clk1b_rpt import GraceFOClk1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOClk1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOClk1BRptDataProduct
    expected_table_names = ['gracefo_clk1b_rpt_04_c', 'gracefo_clk1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, float, float, float, float, float, int,
                            float, float, float, float, float, float, int,
                            int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('CLK1B_2023-06-01_C_04.rpt', 738849600, 739543541, 738849300, 738936300,
         8703, 9.997701677775224, 0.1515847616441534, 0, 10,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         0.06203582158200148, 0.05265892710629507, 2.05366837265406e-06,
         6.332785182727499e-06, 0.06373215805538925, 0.01184754237596673, 1441,
         0.0764951048359834, 0.05265892710629507, -2.479959314316899e-06,
         6.332785182727499e-06, 0.07808642143859937, 0.01181376821992483,1441,
         0, datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc)),
        ('CLK1B_2023-06-01_D_04.rpt', 738849600, 739543703, 738849300, 738936300,
         8703, 9.997701677775224, 0.1515847616441534, 0, 10,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         0.02195637093256983, 0.05265892710629507, 1.07097746439461e-06,
         6.332785182727499e-06, 0.02435891425261411, 0.009561742354342165, 1441,
         0.1582941189720475, 0.05265892710629507, -1.503824784608493e-06,
         6.332785182727499e-06, 0.1589425385291189, 0.01290618640750732, 1441,
         0, datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
