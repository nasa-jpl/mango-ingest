from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.llt1a_rpt import GraceFOLlt1ARptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOLlt1ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLlt1ARptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'LLT1A_RPT'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

