import logging
from collections.abc import Collection
from typing import Type
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.utils.packaging import import_submodules
from masschange.dataproducts import implementations as datasetimplementations

log = logging.getLogger()


def get_time_series_dataproduct_classes() -> Collection[Type[TimeSeriesDataProduct]]:
    import_submodules(datasetimplementations)
    return TimeSeriesDataProduct.__subclasses__()


def resolve_dataset(dataset_id: str) -> TimeSeriesDataProduct:
    datasets_by_name = {ds().get_full_id(): ds() for ds in get_time_series_dataproduct_classes()}
    dataset = datasets_by_name.get(dataset_id)
    if dataset is not None:
        return dataset
    else:
        err_msg = f"Failed to resolve provided dataset_id (got '{dataset_id}', expected one of {sorted(datasets_by_name.keys())})"
        log.error(err_msg)
        raise ValueError(err_msg)
