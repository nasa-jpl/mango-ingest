from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefotnk1a import GraceFOTnk1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOTnk1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOTnk1ADataFileReader()

    mission = GraceFO
    id_suffix = 'TNK1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1/2)  # one measurement (but sometimes two) per tank per second.  Two tanks
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            
            GRACEFO_id CHAR not null,
            tank_id int not null,
            qualflg VARCHAR(8) not null,
            prod_flag VARCHAR(32) not null,
            
            timestamp timestamptz not null,
            
            tank_pres double precision,
            reg_pres double precision,
            skin_temp double precision,
            skin_temp_r double precision,
            adap_temp double precision,
            boss_fixed double precision,
            boss_sliding double precision
        """
