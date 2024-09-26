from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.passreports.ihk1a_pass import GraceFOIhk1APassDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOIhk1APassDataProduct(TimeSeriesRptDataProduct):
    # TODO: sample files has extra columns that are not listed at the GMAT website
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOIhk1APassDataFileReader()

    mission = GraceFO
    id_suffix = 'IHK1A_PASS'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=3)
    processing_level = '1A'

