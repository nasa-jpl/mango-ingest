from abc import ABC

from pyspark.sql import DataFrame, SparkSession

from masschange.datasets.interface import crude_sql_injection_guard


class Dataset(ABC):
    """
    A parquet-backed pyspark dataset.
    Attributes:
        - dataset_id: a dataset type identifier for this dataset type
        - parquet_path: a local-filesystem parquet src_filepath (or basepath, if partitioned)
    """

    spark: SparkSession
    parquet_path: str

    def __init__(self, spark_session: SparkSession, parquet_path: str):
        self.spark = spark_session
        self.parquet_path = parquet_path

    def raw_query(self, parquet_filepath: str, sql_query: str, sql_temp_view_name) -> DataFrame:
        """
        Performs the given raw sql query against the specified parquet file.  sql_temp_view_name must match the table name used in the query
        """

        crude_sql_injection_guard(sql_query)

        df = self.spark.read.parquet(parquet_filepath)
        df.createOrReplaceTempView(sql_temp_view_name)
        return self.spark.sql(sql_query)
