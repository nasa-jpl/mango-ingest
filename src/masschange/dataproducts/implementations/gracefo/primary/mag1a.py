from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.mag1a import GraceFOMag1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOMag1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOMag1ADataFileReader()

    mission = GraceFO
    id_suffix = 'MAG1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(milliseconds=500)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            
            GRACEFO_id CHAR not null,
            MfvX_RAW double precision not null,
            MfvY_RAW double precision not null,
            MfvZ_RAW double precision not null,
            torque1A double precision not null,
            torque2A double precision not null,
            torque3A double precision not null,
            torque1B double precision not null,
            torque2B double precision not null,
            torque3B double precision not null,
            MF_BCalX double precision not null,
            MF_BCalY double precision not null,
            MF_BCalZ double precision not null,
            torque_cal double precision not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null
        """
