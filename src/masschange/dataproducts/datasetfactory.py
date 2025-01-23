
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion


class DatasetFactory:
    @classmethod
    def create(cls, product: DataProduct, version: TimeSeriesDatasetVersion, instrument_id: str):
        # TODO: check whether using local imports this way to avoid circular imports is an anti-pattern
        #  if so, revert the choice to make them local and extract Dataset.attach_lat_lon() to a separate
        #  location-attaching class, avoiding the Dataset<->DatasetFactory circular import issue
        #  Probably that's the better way to do it anyway, but maybe this is acceptable for a factory method?
        from masschange.dataproducts.dataset import Dataset
        from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset

        if product.is_time_series_dataproduct():
            return TimeSeriesDataset(product, version, instrument_id)
        else:
            return Dataset(product, version, instrument_id)
