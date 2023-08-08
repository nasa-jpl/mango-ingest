import os
from datetime import datetime
from typing import List, Dict, Set

from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.ingest.datasets.gracefo.constants import reference_epoch as timestamp_epoch, \
    PARQUET_TEMPORAL_PARTITION_KEY
from masschange.datasets.dataset import Dataset


class GraceFO1AFullResolutionDataset(Dataset):
    # TODO: The parquet_path attribute will get messy when decimation is incorporated, refactoring will be necessary
    parquet_path = os.path.join(os.environ['PARQUET_ROOT'], 'full-resolution')

    INTERNAL_USE_COLUMNS = {'rcvtime_intg', 'rcvtime_frac', 'temporal_partition_key'}

    @classmethod
    def get_available_fields(cls) -> Set[str]:
        schema = pq.ParquetDataset(cls.parquet_path).schema
        return set(schema.names).difference(cls.INTERNAL_USE_COLUMNS)

    @classmethod
    def select(cls, from_dt: datetime, to_dt: datetime) -> List[Dict]:
        from_rcvtime_intg = cls.dt_to_rcvtime_intg(max(from_dt, timestamp_epoch))
        to_rcvtime_intg = cls.dt_to_rcvtime_intg(to_dt)

        coarse_gte_expr = from_dt.date().isoformat() <= pc.field(PARQUET_TEMPORAL_PARTITION_KEY)
        coarse_lte_expr = pc.field(PARQUET_TEMPORAL_PARTITION_KEY) <= to_dt.date().isoformat()
        fine_gte_expr = from_rcvtime_intg <= pc.field('rcvtime_intg')
        fine_lte_expr = pc.field('rcvtime_intg') <= to_rcvtime_intg
        expr = coarse_gte_expr & coarse_lte_expr & fine_gte_expr & fine_lte_expr

        # todo: play around with metadata_nthreads and other options (actually, not that one, it's not supported yet)
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
        dataset = pq.ParquetDataset(cls.parquet_path, filters=expr)
        results = dataset.read().drop_columns(list(cls.INTERNAL_USE_COLUMNS)).to_pylist()

        return results

    @staticmethod
    def dt_to_rcvtime_intg(dt: datetime) -> int:
        """Given a python datetime, return its equivalent rcvtime_intg value.  This is useful for faster querying"""
        if dt < timestamp_epoch:
            raise ValueError(
                f'Cannot convert datetime "{dt}" to rcvtime_intg (dt is earlier than epoch "{timestamp_epoch}")')

        return int((dt - timestamp_epoch).total_seconds())
