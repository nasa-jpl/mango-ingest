from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.ihk1b_rpt import GraceFOIhk1BRptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOIhk1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOIhk1BRptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'IHK1B_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

