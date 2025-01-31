import logging

from datetime import datetime
from typing import Dict, Union, Iterable

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import cursor as Cursor

from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.db.conn import get_db_cursor
from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.implementations.gracefo.primary.gnv1a import GraceFOGnv1ADataProduct

log = logging.getLogger()


class TimeSeriesDataset(Dataset):

    product: TimeSeriesDataProduct


    def get_table_name(self) -> str:
        """Return the name of the SQL table storing the data for this dataset for a given instruments"""
        return self.get_table_or_view_name(aggregation_depth=0)

    def get_table_or_view_name(self, aggregation_depth: int) -> str:
        """
        Return the name of the SQL table or view providing access to data for this dataset for a given instruments at a given
        aggregation level
        """
        if self.instrument_id not in self.product.instrument_ids:
            raise ValueError(
                f'instruments id "{self.instrument_id}" not recognized (expected one of {sorted(self.product.instrument_ids)})')

        aggregation_depth_pad_width = 2
        padded_aggregation_depth = str(aggregation_depth).rjust(aggregation_depth_pad_width, "0")
        if len(padded_aggregation_depth) > aggregation_depth_pad_width:
            raise ValueError(
                f'aggregation_depth "{aggregation_depth}" exceeds maximum accounted for ({aggregation_depth_pad_width} digits)')

        # f for factor, l for level - aids in view maintenance
        aggregation_suffix = f'f{self.product.aggregation_step_factor}l{padded_aggregation_depth}'

        # TODO: Remove legacy null-version support when fully implemented/migrated
        table_base_name = super().get_table_name()

        return (table_base_name if aggregation_depth == 0 else f'{table_base_name}_{aggregation_suffix}').lower()

    @classmethod
    def is_time_series_dataset(cls) -> bool:
        return True
