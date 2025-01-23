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

    """
    TODO: this is in the child class because it uses aggregations
    """
    def _enumerate_time_series_id_values(self, cur: Cursor) -> Dict:
        if not self.product.has_time_series_id_fields():
            return {}

        time_series_id_column_names = [f.name for f in self.product.get_available_fields() if
                                       f.is_time_series_id_column]
        # To avoid long queries, a view is used rather than the full-res dataset.  The level must be low enough that it
        # is safe to assume all possible values have been written to that materialized view. 5 is a good starting point.
        view_depth = min(([0, *self.product.get_available_aggregation_levels()])[-1], 5)
        sql = f"""
            SELECT DISTINCT {','.join(sorted(time_series_id_column_names))}
            FROM {self.get_table_or_view_name(view_depth)};
            """
        try:
            cur.execute(sql)
        except Exception as err:
            raise err.__class__(f'query failed with {err}: {sql}')

        metadata = {column: set() for column in time_series_id_column_names}
        for row in cur.fetchall():
            for column in time_series_id_column_names:
                metadata[column].add(row[column])

        for column in time_series_id_column_names:
            metadata[column] = sorted(metadata[column])

        return metadata

    """TODO: this method is overwritten in a child class because is 
    uses  _enumerate_time_series_id_values which uses aggregations"""
    def get_metadata_properties(self) -> Union[Dict, None]:
        """Get available values from the _meta_dataproducts_versions_instruments table for the corresponding row"""
        supported_properties = {'data_begin', 'data_end', 'last_updated'}

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                metadata = self._get_basic_metadata(cur, supported_properties)
                metadata['time_series_id_enums'] = self._enumerate_time_series_id_values(cur)
            except Exception as err:
                logging.warning(err)
                return None

        return metadata

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
