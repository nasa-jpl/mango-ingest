import os
from datetime import datetime, timedelta
from typing import List, Dict, Set

from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.ingest.datasets.gracefo.constants import reference_epoch as timestamp_epoch
from masschange.ingest.datasets.constants import PARQUET_TEMPORAL_PARTITION_KEY
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFO1AFullResolutionDataset(TimeSeriesDataset):
    # TODO: The root_parquet_path attribute will get messy when decimation is incorporated, refactoring will be necessary
    root_parquet_path = os.path.join(os.environ['PARQUET_ROOT'], 'gracefo_1a')

    INTERNAL_USE_COLUMNS = {'temporal_partition_key'}

    @classmethod
    def get_available_fields(cls) -> Set[str]:
        schema = pq.ParquetDataset(cls.root_parquet_path).schema
        return set(schema.names).difference(cls.INTERNAL_USE_COLUMNS)

    @classmethod
    def _select(cls, parquet_path: str, from_dt: datetime, to_dt: datetime) -> List[Dict]:
        # TODO: Determine if this implementation needs to account for additional filters - probably not since this will
        #  always be handled by the pseudo-index optimization

        from_rcvtime = cls.dt_to_rcvtime(max(from_dt, timestamp_epoch))
        to_rcvtime = cls.dt_to_rcvtime(to_dt)

        # TODO: Determine if this coarse/fine approach is necessary any longer, post-decimation
        coarse_gte_expr = from_rcvtime <= pc.field(PARQUET_TEMPORAL_PARTITION_KEY)
        coarse_lte_expr = pc.field(PARQUET_TEMPORAL_PARTITION_KEY) <= to_rcvtime
        fine_gte_expr = from_rcvtime <= pc.field('rcvtime')
        fine_lte_expr = pc.field('rcvtime') <= to_rcvtime
        expr = coarse_gte_expr & coarse_lte_expr & fine_gte_expr & fine_lte_expr

        # todo: play around with metadata_nthreads and other options (actually, not that one, it's not supported yet)
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
        dataset = pq.ParquetDataset(parquet_path, filters=expr)
        results = dataset.read().drop_columns(list(cls.INTERNAL_USE_COLUMNS)).to_pylist()

        return results

    @staticmethod
    def dt_to_rcvtime(dt: datetime) -> int:
        """Given a python datetime, return its equivalent rcvtime.  This is useful for faster querying"""
        if dt < timestamp_epoch:
            raise ValueError(
                f'Cannot convert datetime "{dt}" to rcvtime_intg (dt is earlier than epoch "{timestamp_epoch}")')

        return int((dt - timestamp_epoch).total_seconds() * 10**6)

    @classmethod
    def enumerate_temporal_partition_values(cls, from_dt: datetime, to_dt: datetime):
        keys = []
        date_iter = from_dt.date()
        one_day = timedelta(days=1)
        end_date = to_dt.date()
        while (date_iter <= end_date):
            keys.append(date_iter.isoformat())
            date_iter += one_day

        return keys
