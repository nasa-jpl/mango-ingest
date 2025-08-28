from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.kbr1b_rpt import GraceFOKbr1BRptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOKbr1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOKbr1BRptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'KBR1B_RPT'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            crms_dowr double precision not null,
            crms_ion double precision not null,
            arc_length double precision not null,
            resid_nobs int not null,
            resid_rms double precision not null,
            resid_min double precision not null,
            resid_max double precision not null,
            number_of_arcs int not null,
            clkdd_nobs int not null,
            clkdd_mean double precision not null,
            clkdd_sigma double precision not null,
            clkdd_min double precision not null,
            clkdd_max double precision not null,
            clkdd_rms double precision not null,
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
