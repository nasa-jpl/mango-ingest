import logging

from collections.abc import Collection
from typing import Dict
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.utils.timespan import TimeSpan
from typing import Union
from datetime import datetime
from masschange.db.conn import get_db_cursor
from psycopg2.extensions import cursor as Cursor

log = logging.getLogger()


class Dataset:
    def __init__(self, product: DataProduct, version: TimeSeriesDatasetVersion, instrument_id: str):
        self.product = product
        self.version = version
        self.instrument_id = instrument_id

    def get_table_name(self) -> str:
        """Return the name of the SQL table storing the data for this dataset for a given instruments"""
        table_base_name = (f'{self.product.get_table_name_prefix()}_{self.instrument_id}'.lower()
                           if self.version.is_null
                           else f'{self.product.get_table_name_prefix()}_{str(self.version)}_{self.instrument_id}'.lower())
        return table_base_name

    def get_sql_table_create_statement(self) -> str:
        # TODO: Perhaps generate this from column definitions rather than hardcoding per-class?  Need to think about it.
        """Get an SQL statement to create a table for this dataset/instruments"""
        if self.instrument_id not in self.product.instrument_ids:
            raise ValueError(
                f'instrument_id {self.instrument_id} not in {self.product.__name__}.instrument_ids - expected one of {self.product.instrument_ids}')

        sql = f"""
            create table public.{self.get_table_name()}
            (
                {self.product.get_sql_table_schema()}
            );
        """
        return sql

    def get_data_span(self, use_cache: bool = False) -> Union[TimeSpan, None]:
        """
        Return the TimeSpan corresponding to the span of extant data, or None if no data exists
        :param use_cache: Use stateful metadata cache rather than deriving the value from the data itself, which is
                           expensive if the number of partitions is large.
                           (~1-2min for 30 years, chunked at 24hr intervals, at time of testing)
        :return:
        """
        if use_cache:
            with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                metadata = self._get_basic_metadata(cur, ['data_begin', 'data_end'])
            begin = metadata['data_begin']
            end = metadata['data_end']
        else:
            begin = self._get_data_begin()
            end = self._get_data_end()

        if begin is not None and end is not None:
            return TimeSpan(begin=begin, end=end)
        else:
            return None

    def _get_data_begin(self) -> Union[datetime, None]:
        return self._get_data_span_stat('min')

    def _get_data_end(self) -> Union[datetime, None]:
        return self._get_data_span_stat('max')

    def _get_data_span_stat(self, agg: str) -> Union[datetime, None]:
        """Get either the min or max timestamp for a given dataset, version and instruments"""
        if agg not in {'min', 'max'}:
            raise ValueError(f'"{agg}" is not a supported timespan stat')

        with get_db_cursor() as cur:
            table_name = self.get_table_name()

            try:
                sql = f"""
                       SELECT {agg}({self.product.TIMESTAMP_COLUMN_NAME})
                       FROM {table_name}
                       """
                cur.execute(sql)
                result = cur.fetchone()[0]
            except Exception as err:
                logging.warning(f'query failed with {err}: {sql}')
                return None

        return result