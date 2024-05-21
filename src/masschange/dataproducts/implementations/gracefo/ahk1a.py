from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoahk1a import GraceFOAhk1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAhk1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAhk1ADataFileReader()

    mission = GraceFO
    id_suffix = 'AHK1A'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1A'

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            prod_flag VARCHAR(32) not null,
            
            timestamp timestamptz not null,
            
            TFEEU_IF double precision,
            TFEEU_REF double precision,
            TFEEU_X double precision,
            TFEEU_YZ double precision,
            analog_GND double precision,
            plus_3_dot_3V double precision,
            Vp double precision,
            MES_Vd double precision,
            MES_DET_X1 double precision,
            MES_DET_X2 double precision,
            MES_DET_X3 double precision,
            MES_DET_Y1 double precision,
            MES_DET_Y2 double precision,
            MES_DET_Z1 double precision,
            TSU_Y_plus double precision,
            TICUN double precision,
            TSU_Y_minus double precision,
            TSU_Z_plus double precision,
            TSU_Z_minus double precision,
            plus_5V double precision,
            TICUR double precision,
            plus_15V double precision,
            minus_15V double precision,
            plus_48V double precision,
            minus_48V double precision,
            icu_blk_nr int, 
            PPS_source int ,
            Sync_Qual int ,
            statusflag VARCHAR(32)
        """
