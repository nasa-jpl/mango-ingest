from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.sca1a import GraceFOSca1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOSca1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOSca1ADataFileReader()

    mission = GraceFO
    id_suffix = 'SCA1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(milliseconds=500)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            GRACEFO_id CHAR not null,
            
            sca_id smallint not null,
            sca_desig CHAR not null,
            quatangle double precision not null,
            quaticoeff double precision not null,
            quatjcoeff double precision not null,
            quatkcoeff double precision not null,
            nlocks smallint not null,
            nstars smallint not null,
            sca_confid smallint not null,
            sca_mode VARCHAR(8) not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null
        """
