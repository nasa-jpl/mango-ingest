from typing import Union, Iterable

from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.utils import get_dataproduct_classes
from masschange.dataproducts.dataset import Dataset


def permute_all_datasets() -> Iterable[TimeSeriesDataset]:
    for product_cls in get_dataproduct_classes():
        dataset_cls = TimeSeriesDataset if product_cls.is_time_series_dataproduct() else Dataset
        for version in product_cls.get_available_versions():
            for instrument_id in product_cls.instrument_ids:
                yield dataset_cls(product_cls(), version, instrument_id)


def is_nearly_equal(expected: Union[int, float], actual: Union[int, float], allowed_deviation_percent=5) -> bool:
    if expected == 0:
        return actual == 0
    return (abs(expected - actual) / expected) <= allowed_deviation_percent / 100
