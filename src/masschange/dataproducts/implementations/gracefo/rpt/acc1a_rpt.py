from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.acc1a_rpt import GraceFOAcc1ARptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAcc1ARptDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1ARptDataFileReader()

    mission = GraceFO
    id_suffix = 'ACC1A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        def get_sql_table_schema(cls) -> str:
            print("QQQQQQQQQQQ ", f"""
                {cls.RPT_COMMON_SCHEMA_SQL}

                timestamp timestamptz not null
            """)
            return f"""
                {cls.RPT_COMMON_SCHEMA_SQL}

                timestamp timestamptz not null
            """
