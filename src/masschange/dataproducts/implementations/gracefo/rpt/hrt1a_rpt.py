from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.hrt1a_rpt import GraceFOHrt1ARptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOHrt1ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOHrt1ARptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'HRT1A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

