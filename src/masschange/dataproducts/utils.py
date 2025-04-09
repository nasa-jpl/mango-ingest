import logging
from collections.abc import Collection
from typing import Type
from masschange.dataproducts.dataproduct import DataProduct
from masschange.utils.packaging import import_submodules
from masschange.dataproducts import implementations as datasetimplementations

import inspect

log = logging.getLogger()


def get_all_subclasses(cls: Type) -> Collection[Type]:
    """
    Get all subclasses of a class recursively
    """
    all_subclasses = []

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses


def get_dataproduct_classes() -> Collection[Type[DataProduct]]:
    """
    Get all concrete subclasses of DataProduct
    """
    import_submodules(datasetimplementations)
    return [subclass for subclass in get_all_subclasses(DataProduct) if not inspect.isabstract(subclass)]  # TODO: fix inspection warning


def get_time_series_dataproduct_classes() -> Collection[Type[DataProduct]]:
    return [cls for cls in get_dataproduct_classes() if cls.is_time_series_dataproduct()]


def get_dataproducts() -> Collection[DataProduct]:
    return [cls() for cls in get_dataproduct_classes()]


def get_time_series_dataproducts() -> Collection[DataProduct]:
    return [cls() for cls in get_time_series_dataproduct_classes()]


def resolve_dataset(dataset_id: str) -> DataProduct:
    datasets_by_name = {ds().get_full_id(): ds() for ds in get_dataproduct_classes()}
    dataset = datasets_by_name.get(dataset_id)
    if dataset is not None:
        return dataset
    else:
        err_msg = f"Failed to resolve provided dataset_id (got '{dataset_id}', expected one of {sorted(datasets_by_name.keys())})"
        log.error(err_msg)
        raise ValueError(err_msg)


def get_schema_updates_for_flag_fields(prefix: str, num_of_fields: int) -> str:
    """
    Convenience method that returns a string that could be inserted to DataProduct's
    table schema to add fields for boolean quality flags.
    Quality flag filed name format is <prefix>_<n>

    Parameters
    ----------
    prefix: str prefix for the name of the field
    num_of_fields: int number of columns

    Returns
    -------

    """
    return "".join( [f'{prefix}_{str(i)} boolean not null, \n' for i in range(num_of_fields)])
