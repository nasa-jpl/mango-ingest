from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.sci1a_rpt import GraceFOSci1ARptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOSci1ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOSci1ARptDataFileReader()

    mission = GraceFO
    id_suffix = 'SCI1A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            nr_obs_sc1 double precision not null,                    
            nr_obs_sc2 double precision not null,                      
            nr_obs_sc3 double precision not null,                      
            nr_obs_imu double precision not null,                       
            nr_obs_acc double precision not null,                      
            rms_postfit_sc1_x double precision not null,                       
            rms_postfit_sc1_y double precision not null,                       
            rms_postfit_sc1_z double precision not null,                       
            rms_postfit_sc2_x double precision not null,                      
            rms_postfit_sc2_y double precision not null,                    
            rms_postfit_sc2_z double precision not null,                     
            rms_postfit_sc3_x double precision not null,                     
            rms_postfit_sc3_y double precision not null,                      
            rms_postfit_sc3_z double precision not null,
            rms_postfit_imu_x double precision not null,                      
            rms_postfit_imu_y double precision not null,                      
            rms_postfit_imu_z double precision not null,
            rms_postfit_acc_x double precision not null,                      
            rms_postfit_acc_y double precision not null,                      
            rms_postfit_acc_z double precision not null,
            '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
