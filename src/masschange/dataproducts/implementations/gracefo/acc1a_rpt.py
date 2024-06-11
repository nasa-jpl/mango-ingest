from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoacc1a_rpt import GraceFOAcc1ARptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAcc1ARptDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1ARptDataFileReader()

    mission = GraceFO
    id_suffix = 'ACC1A_RPT'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            file_name VARCHAR(32) not null,
            file_tag bigint not null,
            process_ttag bigint not null,
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
        
            timestamp timestamptz not null
        """
