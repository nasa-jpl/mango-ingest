from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.poe1a_rpt import GraceFOPoe1ARptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOPoe1ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOPoe1ARptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'POE1A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls):
        # Special case for POE1A:
        # For all other PRT, the file_tag and process_ttag are defined as follows:
        #     file_tag : Date in name of data product, expressed as the time of midnight of that date, in seconds
        #     past January 1, 2000 12:00 UTC
        #     process_ttag: Report creation time, expressed in seconds past January 1, 2000 12:00 UTC
        # so the data types were set to int for these columns
        # For POE1A, sample files have these data as floats
        return f"""
                    file_name VARCHAR(40) not null,
                    file_tag double precision not null,
                    process_ttag double precision not null,
                    first_data_point_t_tag double precision not null,
                    last_data_point_t_tag  double precision not null,
                    n_recs int not null,
                    time_gap_avg double precision not null,
                    time_gap_var double precision not null,
                    time_gap_min double precision not null,
                    time_gap_max double precision not null,
                    n_qual_bits int not null,
                    bit_count_0 int not null,
                    bit_count_1 int not null,
                    bit_count_2 int not null,
                    bit_count_3 int not null,
                    bit_count_4 int not null,
                    bit_count_5 int not null,
                    bit_count_6 int not null,
                    bit_count_7 int not null,  
                    
                    rms_phase double precision not null,
                    n_phase int not null,
                    n_phase_rej int not null,
                    per_phase_rej double precision not null,
                    rms_range double precision not null,
                    n_range int not null,
                    n_range_rej int not null,
                    per_range_rej double precision not null,
                    
                    timestamp timestamptz not null
                """
