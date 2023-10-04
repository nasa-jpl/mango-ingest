import os
from datetime import datetime, timedelta, time
from typing import List, Dict, Set

from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.datasets.timeseriesdatasetconfig import TimeSeriesDatasetConfig
from masschange.ingest.datasets.constants import PARQUET_TEMPORAL_PARTITION_KEY
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.ingest.utils.decimation import get_partition_id


class GraceFO1ADataset(TimeSeriesDataset):
    # TODO: The root_parquet_path attribute will get messy when decimation is incorporated, refactoring will be necessary
    root_parquet_path = os.path.join(os.environ['PARQUET_ROOT'], 'gracefo_1a')

    INTERNAL_USE_COLUMNS = {'temporal_partition_key'}

    @classmethod
    def get_config(cls) -> TimeSeriesDatasetConfig:
        return TimeSeriesDatasetConfig(
            stream_ids={1, 2},
            base_hours_per_partition=24,
            decimation_step_factors=[20, 20, 20, 3],
            reference_epoch=datetime(2000, 1, 1, 12)
        )

    @classmethod
    def get_available_fields(cls) -> Set[str]:
        schema = pq.ParquetDataset(cls.root_parquet_path).schema
        return set(schema.names).difference(cls.INTERNAL_USE_COLUMNS)

    @classmethod
    def _select(cls, parquet_path: str, from_dt: datetime, to_dt: datetime) -> List[Dict]:
        # TODO: Determine if this implementation needs to account for additional filters - probably not since this will
        #  always be handled by the pseudo-index optimization

        from_rcvtime = cls.dt_to_rcvtime(max(from_dt, cls.get_config().reference_epoch))
        to_rcvtime = cls.dt_to_rcvtime(to_dt)

        # TODO: Test performance after excising coarse filter, and commit if performance is satisfactory
        # coarse_gte_expr = str(from_rcvtime) <= pc.field(PARQUET_TEMPORAL_PARTITION_KEY)
        # coarse_lte_expr = pc.field(PARQUET_TEMPORAL_PARTITION_KEY) <= str(to_rcvtime)
        fine_gte_expr = from_rcvtime <= pc.field('rcvtime')
        fine_lte_expr = pc.field('rcvtime') <= to_rcvtime
        # expr = coarse_gte_expr & coarse_lte_expr & fine_gte_expr & fine_lte_expr
        expr = fine_gte_expr & fine_lte_expr

        # todo: play around with metadata_nthreads and other options (actually, not that one, it's not supported yet)
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
        dataset = pq.ParquetDataset(parquet_path, filters=expr)
        results = dataset.read().drop_columns(list(cls.INTERNAL_USE_COLUMNS)).to_pylist()

        return results

    @staticmethod
    def dt_to_rcvtime(dt: datetime) -> int:
        """Given a python datetime, return its equivalent rcvtime.  This is useful for faster querying"""
        timestamp_epoch = GraceFO1ADataset.get_config().reference_epoch
        if dt < timestamp_epoch:
            raise ValueError(
                f'Cannot convert datetime "{dt}" to rcvtime_intg (dt is earlier than epoch "{timestamp_epoch}")')

        return int((dt - timestamp_epoch).total_seconds() * 10**6)

    @classmethod
    def enumerate_temporal_partition_values(cls, decimation_factor: int, from_dt: datetime, to_dt: datetime):
        # TODO: Check performance, and optimize this to generate only keys which can be expected to exist given the
        #  relevant decimation ratio.  Currently it generates values for every day, as this is trivial to implement.
        #  WAIT NO ACTUALLY THIS WON'T WORK - you need to include the partition value to the left and right of the
        #  requested span - just doing the date will be insufficient.  At an absolute minimum you need to generate dates
        #  from (from_dt - decimation factor) AND ensure that the generated values line up with the index steps
        #  (see: 12hr epoch offset).  Really this needs to be done properly through a utility.
        #  The simplest interim approach would be to generate all dates including one date left/right, then map those to
        #  partition values with the utility and throw them into a set to deduplicate.
        config = cls.get_config()
        hours_per_partition = config.get_hours_per_partition(decimation_factor)
        values = set()
        iter = datetime.combine(from_dt.date(), time(0,0))
        increment = timedelta(days=1)  # Assumes partition granularity no finer than 24hrs
        iter_end = datetime.combine(to_dt, time(23, 59, 59, 999999))
        while iter <= iter_end:
            value = cls.dt_to_rcvtime(iter)
            partition_value = get_partition_id(value, hours_per_partition, config.partition_epoch_offset_hours)
            values.add(partition_value)
            iter += increment

        return values
