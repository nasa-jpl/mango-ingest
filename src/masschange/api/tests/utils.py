from typing import Union, Iterable

from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.utils import get_time_series_dataproduct_classes


def permute_all_datasets() -> Iterable[TimeSeriesDataset]:
    for product in get_time_series_dataproduct_classes():
        for version in product.get_available_versions():
            for stream_id in product.instrument_ids:
                yield TimeSeriesDataset(product(), version, stream_id)


def is_nearly_equal(expected: Union[int, float], actual: Union[int, float], allowed_deviation_percent=5) -> bool:
    if expected == 0:
        return actual == 0
    return (abs(expected - actual) / expected) < allowed_deviation_percent / 100
