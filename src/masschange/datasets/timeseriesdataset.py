import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict

from masschange.datasets.timeseriesdatasetconfig import TimeSeriesDatasetConfig
from masschange.datasets.utils.performance import get_prepruned_parquet_path_multilevel, safely_remove_multilevel_temporary_index


class TimeSeriesDataset(ABC):
    """
    A parquet-backed time-series dataset.

    A TimeSeriesDataset contains [1,n] "streams", each having string identifier.  This will typically be a satellite_id,
    and it will be up to subclasses to implement overrides of get_stream_p

    Attributes:
        - dataset_id: a dataset type identifier for this dataset type
        - root_parquet_path: a local-filesystem parquet src_filepath (or basepath, if partitioned)
    """

    # TODO: Move these to TimeSeriesDataSetConfig, as well as other values like partition_key_hierarchy and the like
    root_parquet_path: str
    max_safe_select_temporal_span_at_full_resolution = timedelta(days=1)  # safety guard to prevent server crash
    max_select_results_count = 100000  # equivalent to a bit over one hour of full-resolution data
    # END MOVE

    def __init__(self, root_parquet_path: str):
        self.root_parquet_path = root_parquet_path

    @classmethod
    @abstractmethod
    def get_config(cls) -> TimeSeriesDatasetConfig:
        pass

    @classmethod
    def select(cls, stream_id: str | int, from_dt: datetime, to_dt: datetime, requested_decimation_factor: int = 1, use_preprune_optimisation: bool = True) -> List[Dict]:
        max_safe_select_temporal_span = cls.max_safe_select_temporal_span_at_full_resolution * requested_decimation_factor
        requested_data_temporal_span = to_dt - from_dt
        if requested_data_temporal_span > max_safe_select_temporal_span:
            raise TooMuchDataRequestedError(
                f'Requested temporal span {requested_data_temporal_span} exceeds maximum allowed by server ({max_safe_select_temporal_span}) at 1:{requested_decimation_factor} decimation')

        if use_preprune_optimisation:
            temporal_partition_values = cls.enumerate_temporal_partition_values(requested_decimation_factor, from_dt, to_dt)

            # TODO: Split this hardcoded structure out to subclass methods, as not all datasets will use these structures
            partition_key_hierarchy = ['satellite_id', 'decimation_factor', 'temporal_partition_key']
            partition_value_filters = {
                'satellite_id': [stream_id],
                'decimation_factor': [requested_decimation_factor],
                'temporal_partition_key': list(temporal_partition_values)}
            # end to do

            parquet_path = get_prepruned_parquet_path_multilevel(cls.root_parquet_path, partition_key_hierarchy, partition_value_filters)
        else:
            parquet_path = (cls.root_parquet_path)

        if len(list(os.scandir(parquet_path))) > 0:
            results = cls._select(parquet_path, from_dt, to_dt)
        else:
            logging.error(f'Failed to resolve any data between {from_dt} and {to_dt}')
            results = []

        if use_preprune_optimisation:
            safely_remove_multilevel_temporary_index(parquet_path)

        if len(results) > cls.max_select_results_count:
            raise TooMuchDataRequestedError(
                f'Requested data quantity exceeds allowed maximum of {cls.max_select_results_count} records')

        return results

    @classmethod
    @abstractmethod
    def _select(cls, parquet_path: str, from_dt: datetime, to_dt: datetime) -> List[Dict]:
        """
        Subclass-specific implementation for cls.select()
        Parameters
        ----------
        parquet_path
        from_dt
        to_dt

        Returns
        -------

        """
        pass

    @classmethod
    @abstractmethod
    def enumerate_temporal_partition_values(cls, decimation_factor: int, from_dt: datetime, to_dt: datetime):
        pass


class TooMuchDataRequestedError(ValueError):
    pass
