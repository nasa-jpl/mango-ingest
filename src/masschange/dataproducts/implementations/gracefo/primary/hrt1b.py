from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.hrt1b import GraceFOHrt1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOHrt1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOHrt1BDataFileReader()

    mission = GraceFO
    id_suffix = 'HRT1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=32)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
        
                time_intg bigint not null,
                time_frac int not null,
                GRACEFO_id CHAR not null,

                TEMP_MEP_neg_y double precision not null,
                TEMP_MEP_pos_y double precision not null,
                TEMP_MEPm double precision not null,
                TEMP_ICU double precision not null,
                TEMP_ICU_red double precision not null,
                TEMP_ACC_neg_z double precision not null,
                TEMP_ACC_pos_z double precision not null,
                TEMP_CFRP_pos_x double precision not null,
                TEMP_CFRP_pos_x_red double precision not null,
                TEMP_CFRP_neg_x double precision not null,
                TEMP_CFRP_neg_x_red double precision not null,
                TEMP_CFRP_neg_y double precision not null,
                TEMP_CFRP_neg_y_red double precision not null,
                TEMP_ACCSen double precision not null,
                TEMP_ICU_spec double precision not null,
                TEMP_MWA_neg_y double precision not null,
                TEMP_MWA_neg_yoff double precision not null,
                TEMP_MWA_pos_y double precision not null,
                TEMP_MWA_pos_yoff double precision not null,
                TEMP_Horn_pos_x double precision not null,
                TEMP_Horn_pos_x_red double precision not null,
                TEMP_HornPl double precision not null,
                TEMP_HornPl_red double precision not null,
                TEMP_HMWA_neg_y double precision not null,
                TEMP_HMWA_pos_y double precision not null,
                TEMP_RFSamp double precision not null,
                TEMP_USO_neg_y double precision not null,
                TEMP_USO_neg_y_red double precision not null,
                TEMP_USO_pos_y double precision not null,
                TEMP_USO_pos_y_red double precision not null,
                
                qualflg VARCHAR(8) not null, 
            
                timestamp timestamptz not null
        """
