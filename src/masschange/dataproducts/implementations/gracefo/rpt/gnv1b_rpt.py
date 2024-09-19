from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.gnv1b_rpt import GraceFOGnv1BRptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOGnv1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGnv1BRptDataFileReader()

    mission = GraceFO
    id_suffix = 'GNV1B_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            npoints_start int not null,
            hpos_rms_start double precision not null,
            cpos_rms_start double precision not null,
            lpos_rms_start double precision not null,
            hvel_rms_start double precision not null,
            cvel_rms_start double precision not null,
            lvel_rms_start double precision not null,
            npoints_end  int not null,
            hpos_rms_end double precision not null,
            cpos_rms_end double precision not null,
            lpos_rms_end double precision not null,
            hvel_rms_end double precision not null,
            cvel_rms_end double precision not null,
            lvel_rms_end double precision not null,
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
