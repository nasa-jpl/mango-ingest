from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.gps1b_rpt import GraceFOGps1BRptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOGps1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGps1BRptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'GPS1B_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            crms_ca double precision not null,
            ca_nobs int not null,
             
            crms_l1 double precision not null, 
            l1_nobs int not null,
             
            crms_l2 double precision not null,
            l2_nobs int not null,
            
            breaks int not null,
            
            low_l1_snr int not null,
            low_l2_snr int not null,
            
            ca_mis_lock int not null,
            discards int not null,
            nobs_in int not null,
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
