import functools
import multiprocessing
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Iterable

import lttb
import lttbc
import numpy as np
import pandas as pd
from pyarrow import parquet as pq
from pyarrow import compute as pc

from masschange.datasets.gracefo_1a import GraceFO1ADataset
from masschange.ingest.utils import get_configured_logger


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

        benchmark_begin = datetime.now()

        start = datetime.now()
        # todo: play around with metadata_nthreads and other options (actually, not that one, it's not supported yet)
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
        dataset = pq.ParquetDataset(parquet_path, filters=filter_expr)
        elapsed = datetime.now() - start
        print(f'Filtered in {int(elapsed.total_seconds()*1000)}ms')

        start = datetime.now()
        #TODO: extract common elements of this to TimeSeriesDataset - it may be the whole line.
        results_df = dataset.read(columns=['rcvtime', 'lin_accl_x']).sort_by('rcvtime').to_pandas()
        elapsed = datetime.now() - start
        print(f'Read dataset in {int(elapsed.total_seconds()*1000)}ms')

        start = datetime.now()
        downsampled_results = cls.downsample(5000, results_df)
        elapsed = datetime.now() - start
        print(f'Downsampled in {int(elapsed.total_seconds()*1000)}ms')

        benchmark_query = datetime.now()
        query_elapsed_ms = int((benchmark_query - benchmark_begin).total_seconds() * 1000)
        print(f'Completed query/lttb in {query_elapsed_ms}ms')

        # TODO: see todo in rcvtime_to_dt()
        # populate ISO timestamp dynamically
        results = [{'timestamp': cls.rcvtime_to_dt(rcvtime), 'lin_accl_x': val} for rcvtime, val in downsampled_results]

        return results

    @classmethod
    def downsample(cls, output_points: int, df: pd.DataFrame) -> Iterable[Tuple[str, float]]:

        cores = 1
        chunks = np.array_split(df, cores)
        downsampling_f = functools.partial(cls._downsample, output_points // cores)  # approximate the target number of output points for now

        start = datetime.now()
        with multiprocessing.Pool(cores) as mp_pool:
            downsampled_chunks = mp_pool.map(downsampling_f, chunks)
        elapsed = datetime.now() - start
        print(f'downsampled all chunks in {int(elapsed.total_seconds()*1000)}ms')

        start = datetime.now()
        result = pd.concat(downsampled_chunks)
        elapsed = datetime.now() - start
        print(f'concatenated all chunks in {int(elapsed.total_seconds()*1000)}ms')
        return result

    @classmethod
    def _downsample(cls, output_points: int, df: pd.DataFrame) -> Iterable[Tuple[str, float]]:
        # Comment to switch between lttb and lttbc implementations
        start = datetime.now()
        results = lttb.downsample(df.to_numpy(), output_points).T
        # results = lttbc.downsample(df['rcvtime'].to_numpy(), df['lin_accl_x'].to_numpy(), output_points)
        elapsed = datetime.now() - start
        print(f'LTTBd chunk in {int(elapsed.total_seconds()*1000)}ms')

        start = datetime.now()
        data_count = len(results[0])
        results_series = pd.Series([(results[0][i], results[1][i]) for i in range(0, data_count)])
        elapsed = datetime.now() - start
        print(f'zipped chunk in {int(elapsed.total_seconds()*1000)}ms')

        return results_series