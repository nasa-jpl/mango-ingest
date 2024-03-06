import logging
from collections.abc import Collection
from typing import Type

from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.utils.packaging import import_submodules
from masschange.datasets import implementations as datasetimplementations

log = logging.getLogger()


def get_time_series_dataset_classes() -> Collection[Type[TimeSeriesDataset]]:
    import_submodules(datasetimplementations)
    return TimeSeriesDataset.__subclasses__()


def resolve_dataset(dataset_id: str) -> TimeSeriesDataset:
    datasets_by_name = {ds().get_full_id(): ds() for ds in get_time_series_dataset_classes()}
    dataset = datasets_by_name.get(dataset_id)
    if dataset is not None:
        return dataset
    else:
        err_msg = f"Failed to resolve provided dataset_id (got '{dataset_id}', expected one of {sorted(datasets_by_name.keys())})"
        log.error(err_msg)
        raise ValueError(err_msg)
