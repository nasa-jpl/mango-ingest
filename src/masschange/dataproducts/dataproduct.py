import logging

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Set, Type, List, Dict, Collection
from masschange.missions import Mission
from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField
from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion

log = logging.getLogger()


class DataProduct(ABC):
    TIMESTAMP_COLUMN_NAME = 'timestamp'  # must be considered reserved

    @classmethod
    def get_full_id(cls) -> str:
        return f'{cls.mission.id}_{cls.id_suffix}'

    @classmethod
    def get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()
