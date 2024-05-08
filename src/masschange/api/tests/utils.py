from typing import Union, Iterable, Tuple

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.datasets.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.datasets.utils import get_time_series_dataset_classes


def permute_all_dataset_instances() -> Iterable[Tuple[TimeSeriesDataset, TimeSeriesDatasetVersion, str]]:
    for ds in get_time_series_dataset_classes():
        for version in ds.get_available_versions():
            for stream_id in ds.stream_ids:
                yield (ds, version, stream_id)


def is_nearly_equal(expected: Union[int, float], actual: Union[int, float], allowed_deviation_percent=5) -> bool:
    if expected == 0:
        return actual == 0
    return (abs(expected - actual) / expected) < allowed_deviation_percent / 100
