from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Type, Set

from masschange.api.utils.db.queries import fetch_dataset_bulk_metadata, fetch_bulk_channel_id_enums
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.dataproductfield import DataProductField
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.utils import get_dataproducts


class CachedDataProductMetadata:
    product: DataProduct
    channel_enum_values: Dict[DataProductField, Set[str]]

    def __init__(self, product: DataProduct, channel_enum_values: Dict[DataProductField, Set[str]]):
        self.product = product
        self.channel_enum_values = {f: set() for f in product.get_available_fields() if f.is_channel_id_column}
        self.channel_enum_values.update(channel_enum_values)



@dataclass
class CachedDatasetMetadata:
    # dataset: Dataset   # use strings for now so as not to overcomplicate initial implementation
    dataset_id: str
    product_id: str
    version_id: str
    instrument_id: str
    data_begin: datetime = None
    data_end: datetime = None
    last_updated: datetime = None

    @property
    def product(self) -> DataProduct:
        """Exists to provide backwards-compatibility"""
        return next(p for p in get_dataproducts() if p.get_full_id() == self.product_id)

    @property
    def version(self) -> DatasetVersion:
        """Exists to provide backwards-compatibility"""
        return DatasetVersion(self.version_id)

    @property
    def instrument(self) -> None:
        # TODO: implement class for instruments
        """Exists to provide backwards-compatibility"""
        raise NotImplementedError('Instruments have not been implemented as classes yet, and are still identified by string id')


class BulkMetadataCache:
    dataproducts: Collection[CachedDataProductMetadata]
    datasets: Collection[CachedDatasetMetadata]

    def __init__(self):
        self.dataproducts = [CachedDataProductMetadata(product=product, channel_enum_values=channel_id_enums) for
                             product, channel_id_enums in fetch_bulk_channel_id_enums().items()]
        self.datasets = [CachedDatasetMetadata(dataset_id=k, **record) for k, record in
                         fetch_dataset_bulk_metadata().items()]
