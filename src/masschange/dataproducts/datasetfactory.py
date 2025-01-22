
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion


class DatasetFactory:
    @classmethod
    def create(cls, product: DataProduct, version: TimeSeriesDatasetVersion, instrument_id: str):
        if product.is_time_series_dataproduct():
            return TimeSeriesDataset(product, version, instrument_id)
        else:
            return Dataset(product, version, instrument_id)
