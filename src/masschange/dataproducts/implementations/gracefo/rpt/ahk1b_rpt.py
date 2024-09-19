from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.ahk1b_rpt import GraceFOAhk1BRptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOAhk1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAhk1BRptDataFileReader()

    mission = GraceFO
    id_suffix = 'AHK1B_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'


