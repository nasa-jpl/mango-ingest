import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict

from masschange.datasets.utils.performance import get_prepruned_parquet_path, safely_remove_temporary_index


class TimeSeriesDataset(ABC):
    """
    A parquet-backed time-series dataset.
    Attributes:
        - dataset_id: a dataset type identifier for this dataset type
        - root_parquet_path: a local-filesystem parquet src_filepath (or basepath, if partitioned)
    """

    root_parquet_path: str

    def __init__(self, root_parquet_path: str):
        self.root_parquet_path = root_parquet_path

    @classmethod
    def select(cls, from_dt: datetime, to_dt: datetime, use_preprune_optimisation: bool = True) -> List[Dict]:
        if use_preprune_optimisation:
            partition_values = cls.enumerate_temporal_partition_values(from_dt, to_dt)
            parquet_path = get_prepruned_parquet_path(partition_values, cls.root_parquet_path)
        else:
            parquet_path = (cls.root_parquet_path)

        if len(list(os.scandir(parquet_path))) > 0:
            results = cls._select(parquet_path, from_dt, to_dt)
        else:
            logging.error(f'Failed to resolve any data between {from_dt} and {to_dt}')
            results = []

        if use_preprune_optimisation:
            safely_remove_temporary_index(parquet_path)

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
    def enumerate_temporal_partition_values(cls, from_dt: datetime, to_dt: datetime):
        pass
