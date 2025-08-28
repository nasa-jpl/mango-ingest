from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.lri1b_rpt import GraceFOLri1BRptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOLri1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLri1BRptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'LRI1B_RPT'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            crms_twr double precision not null,
            arc_length double precision not null,
            resid_nobs int not null,
            resid_rms double precision not null,
            resid_min double precision not null,
            resid_max double precision not null,
            number_of_arcs int not null,
            est_scale_1 double precision not null,
            est_scale_sigma_1 double precision not null,
            est_scale_2 double precision not null,
            est_scale_sigma_2 double precision not null,  
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
