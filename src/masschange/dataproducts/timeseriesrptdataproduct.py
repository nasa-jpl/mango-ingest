from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct

class TimeSeriesRptDataProduct(TimeSeriesDataProduct):
    """
    Abstract base class for report file readers
    """

    @classmethod
    def get_sql_table_schema(cls) -> str:
        """All report data products have a set of common columns.
           If a class has additional columns, they are inserted
           to the SQL statement through cls.get_custom_rpt_sql_schema_columns()
           method that the class overwrites
        """
        return f"""
            file_name VARCHAR(40) not null,
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

    @classmethod
    def insert_additional_columns_schema(cls, common_columns_sql: str, additional_columns_sql: str) -> str:
        """
        Insert additional columns into default report table schema before the 'timestamp' clause
        """
        idx = common_columns_sql.index('timestamp')
        return f""" {common_columns_sql[:idx]}  {additional_columns_sql}  {common_columns_sql[idx:]}"""
