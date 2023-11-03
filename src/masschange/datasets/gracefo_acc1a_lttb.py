import os
from datetime import datetime, timedelta
from typing import List, Dict

from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.datasets.gracefo_1a import GraceFO1ADataset


class GraceFOACC1ALTTBDataset(GraceFO1ADataset):
    # TODO: The root_parquet_path attribute will get messy when decimation is incorporated, refactoring will be necessary
    root_parquet_path = os.path.join(os.environ['MASSCHANGE_DATA_ROOT'], 'gracefo_acc1a_lttb')

    max_safe_select_temporal_span_at_full_resolution = timedelta(days=366)  # remove the guard-rail for benchmarking

    @classmethod
    def _select(cls, parquet_path: str, from_dt: datetime, to_dt: datetime) -> List[Dict]:
        # IMPLEMENTATION OVERRIDDEN TO AVOID OVERHEAD OF DYNAMIC TIMESTAMP COMPUTATION
        from_rcvtime = cls.dt_to_rcvtime(max(from_dt, cls.get_config().reference_epoch))
        to_rcvtime = cls.dt_to_rcvtime(to_dt)

        gte_filter_expr = from_rcvtime <= pc.field('rcvtime')
        lte_filter_expr = pc.field('rcvtime') <= to_rcvtime
        filter_expr = gte_filter_expr & lte_filter_expr

        # todo: play around with metadata_nthreads and other options (actually, not that one, it's not supported yet)
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
        dataset = pq.ParquetDataset(parquet_path, filters=filter_expr)

        #TODO: extract common elements of this to TimeSeriesDataset - it may be the whole line.
        results_table = dataset.read(columns=['rcvtime', 'lin_accl_x']).sort_by('rcvtime')
        results = results_table.to_pylist()

        # TODO: see todo in rcvtime_to_dt()
        # populate ISO timestamp dynamically
        for result in results:
            result['timestamp'] = cls.rcvtime_to_dt(result['rcvtime'])

        return results