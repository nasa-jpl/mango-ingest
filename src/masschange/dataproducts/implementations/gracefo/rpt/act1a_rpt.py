from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.act1a_rpt import GraceFOAct1ARptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesRptDataProduct


class GraceFOAct1ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAct1ARptDataFileReader()

    mission = GraceFO
    id_suffix = 'ACT1A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

    @classmethod
    def get_custom_rpt_sql_schema_columns(cls):
        return f"""
            noutliers int not null,
            outlier_max_span double precision not null,
            
            timestamp timestamptz not null,
        """
