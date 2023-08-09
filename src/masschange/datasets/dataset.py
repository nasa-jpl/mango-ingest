from abc import ABC, abstractmethod
from datetime import datetime


class Dataset(ABC):
    """
    A parquet-backed time-series dataset.
    Attributes:
        - dataset_id: a dataset type identifier for this dataset type
        - parquet_path: a local-filesystem parquet src_filepath (or basepath, if partitioned)
    """

    parquet_path: str

    def __init__(self, parquet_path: str):
        self.parquet_path = parquet_path

    @classmethod
    @abstractmethod
    def enumerate_temporal_partition_values(cls, from_dt: datetime, to_dt: datetime):
        pass
