from abc import ABC


class Dataset(ABC):
    """
    A parquet-backed dataset.
    Attributes:
        - dataset_id: a dataset type identifier for this dataset type
        - parquet_path: a local-filesystem parquet src_filepath (or basepath, if partitioned)
    """

    parquet_path: str

    def __init__(self, parquet_path: str):
        self.parquet_path = parquet_path
