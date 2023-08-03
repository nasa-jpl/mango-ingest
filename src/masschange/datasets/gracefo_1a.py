import os
from datetime import datetime
from typing import Iterable, List, Dict, Set

import pyspark

from masschange.datasets.interface import get_spark_session
from masschange.ingest.datasets.gracefo.constants import reference_epoch as timestamp_epoch, \
    PARQUET_TEMPORAL_PARTITION_KEY
from masschange.datasets.dataset import Dataset


class GraceFO1AFullResolutionDataset(Dataset):
    # TODO: The parquet_path attribute will get messy when decimation is incorporated, refactoring will be necessary
    parquet_path = os.path.join(os.environ['PARQUET_ROOT'], 'full-resolution')

    DEFAULT_FIELDS = {'ang_accl_x', 'ang_accl_y', 'ang_accl_z', 'lin_accl_x', 'lin_accl_y', 'lin_accl_z',
                      'rcv_timestamp', 'rcvtime_frac', 'rcvtime_intg', 'satellite_id', 'temporal_partition_key'}

    @classmethod
    def get_available_fields(cls) -> Set[str]:
        spark = get_spark_session()
        schema = spark.read.parquet(cls.parquet_path).schema
        return set(schema.fieldNames())

    @classmethod
    def select(cls, fields: Iterable[str], from_dt: datetime, to_dt: datetime) -> List[pyspark.Row]:
        table_name = 'GRACEFO_1A_full_resolution'

        spark = get_spark_session()
        table_df = spark.read.parquet(cls.parquet_path)
        table_df.createOrReplaceTempView(table_name)

        fields_str = ', '.join(fields)
        from_rcvtime_intg = cls.dt_to_rcvtime_intg(max(from_dt, timestamp_epoch))
        to_rcvtime_intg = cls.dt_to_rcvtime_intg(to_dt)

        query = f"""
            select {fields_str}, rcv_timestamp
            from {table_name}
            where
                {PARQUET_TEMPORAL_PARTITION_KEY} >= '{from_dt.date().isoformat()}'
                and {PARQUET_TEMPORAL_PARTITION_KEY} <= '{to_dt.date().isoformat()}'
                and rcvtime_intg >= {from_rcvtime_intg}
                and rcvtime_intg <= {to_rcvtime_intg}
        """

        records_df = spark.sql(query)
        return records_df.collect()

    @staticmethod
    def dt_to_rcvtime_intg(dt: datetime) -> int:
        """Given a python datetime, return its equivalent rcvtime_intg value.  This is useful for faster querying"""
        if dt < timestamp_epoch:
            raise ValueError(
                f'Cannot convert datetime "{dt}" to rcvtime_intg (dt is earlier than epoch "{timestamp_epoch}")')

        return int((dt - timestamp_epoch).total_seconds())
