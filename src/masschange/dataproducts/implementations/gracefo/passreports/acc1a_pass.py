from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.passreports.acc1a_pass import GraceFOAcc1APassDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOAcc1APassDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1APassDataFileReader()

    mission = GraceFO
    id_suffix = 'ACC1A_PASS'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=3)
    processing_level = '1A'
