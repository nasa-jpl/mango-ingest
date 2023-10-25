import os
from datetime import datetime, timedelta, time
from typing import List, Dict, Set

from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.datasets.timeseriesdatasetconfig import TimeSeriesDatasetConfig
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.partitioning import get_partition_id


class GraceFO1ADataset(TimeSeriesDataset):
    # TODO: The root_parquet_path attribute will get messy when decimation is incorporated, refactoring will be necessary
    root_parquet_path = os.path.join(os.environ['MASSCHANGE_DATA_ROOT'], 'gracefo_1a')

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
        return set(schema.names).difference(cls.INTERNAL_USE_COLUMNS) | {'timestamp'}  # bandaid for runtime-generated timestamp field

    @classmethod
    def _select(cls, parquet_path: str, from_dt: datetime, to_dt: datetime) -> List[Dict]:
        from_rcvtime = cls.dt_to_rcvtime(max(from_dt, cls.get_config().reference_epoch))
        to_rcvtime = cls.dt_to_rcvtime(to_dt)

        gte_filter_expr = from_rcvtime <= pc.field('rcvtime')
        lte_filter_expr = pc.field('rcvtime') <= to_rcvtime
        filter_expr = gte_filter_expr & lte_filter_expr

        # todo: play around with metadata_nthreads and other options (actually, not that one, it's not supported yet)
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
        dataset = pq.ParquetDataset(parquet_path, filters=filter_expr)

        #TODO: extract common elements of this to TimeSeriesDataset - it may be the whole line.
        results = dataset.read().sort_by('rcvtime').drop_columns(list(cls.INTERNAL_USE_COLUMNS)).to_pylist()

        # TODO: see todo in rcvtime_to_dt()
        # populate ISO timestamp dynamically
        for result in results:
            result['timestamp'] = cls.rcvtime_to_dt(result['rcvtime'])

        return results

    @staticmethod
    def rcvtime_to_dt(rcvtime: int) -> datetime:
        # TODO: This is a temporary bandaid due to inadvertant disable of populate_timestamp during ingest().
        #  Performance hit is ~25%, so we'll roll with it for the time being.
        return GraceFO1ADataset.get_config().reference_epoch + timedelta(microseconds=rcvtime)

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
