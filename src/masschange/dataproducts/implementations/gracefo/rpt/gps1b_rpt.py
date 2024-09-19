from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.gps1b_rpt import GraceFOGps1BRptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOGps1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGps1BRptDataFileReader()

    mission = GraceFO
    id_suffix = 'GPS1B_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            crms_CA double precision not null,
            CA_nobs int not null,
             
            crms_L1 double precision not null, 
            L1_nobs int not null,
             
            crms_L2 double precision not null,
            L2_nobs int not null,
            
            breaks int not null,
            
            lowL1_snr int not null,
            lowL2_snr int not null,
            
            CAmisLock int not null,
            discards int not null,
            nobs_in int not null,
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
