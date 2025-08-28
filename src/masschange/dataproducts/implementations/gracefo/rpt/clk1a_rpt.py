from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.clk1a_rpt import GraceFOClk1ARptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOClk1ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOClk1ARptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'CLK1A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            overlap_bias_start double precision not null,
            overlap_bias_sigma_start double precision not null, 
            overlap_slope_start	double precision not null, 
            overlap_slope_sigma_start double precision not null, 
            overlap_rms_zero_start double precision not null, 
            overlap_rms_fit_start double precision not null, 
            overlap_npoints_start int not null,
            
            overlap_bias_end double precision not null, 
            overlap_bias_sigma_end	double precision not null, 
            overlap_slope_end double precision not null, 
            overlap_slope_sigma_end	double precision not null, 
            overlap_rms_zero_end double precision not null, 
            overlap_rms_fit_end	double precision not null, 
            overlap_npoints_end	int not null,
            nobs_formal_edit int not null,
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
