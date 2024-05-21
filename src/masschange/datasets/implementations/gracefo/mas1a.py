from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefomas1a import GraceFOMas1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOMas1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOMas1ADataFileReader()

    mission = GraceFO
    id_suffix = 'MAS1A'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=1)
    processing_level = '1A'

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            prod_flag VARCHAR(8) not null,
            
            timestamp timestamptz not null,
            
            mass_thr double precision,
            mass_thr_err double precision,
            mass_tnk double precision,
            mass_tnk_err double precision,
            gas_mass_thr1 double precision,
            gas_mass_thr2 double precision,
            gas_mass_tnk1 double precision,
            gas_mass_tnk2 double precision
        """
