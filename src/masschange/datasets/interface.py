from datetime import datetime
from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession.builder \
        .master("local") \
        .config('spark.driver.host', 'localhost') \
        .appName('masschange') \
        .getOrCreate()

    spark.conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    return spark


def get_temporary_view_name():
    return datetime.now().isoformat()


def crude_sql_injection_guard(raw_str: str) -> None:
    if any([not (x.isalnum() or x in '-_.') for x in raw_str]):
        raise ValueError(f'Value {raw_str} failed dumb_sql_injection_guard')
