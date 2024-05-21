import logging
from collections.abc import Collection
from datetime import datetime
from typing import List, Dict, Union

import psycopg2
from psycopg2 import extras

from masschange.api.errors import TooMuchDataRequestedError
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.db import get_db_connection
from masschange.db.utils import list_table_columns as list_db_table_columns
from masschange.utils.misc import get_human_readable_timedelta
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


class TimeSeriesDataset:
    product: TimeSeriesDataProduct
    version: TimeSeriesDatasetVersion
    stream_id: str

    def __init__(self, product: TimeSeriesDataProduct, version: TimeSeriesDatasetVersion, stream_id: str):
        self.product = product
        self.version = version
        self.stream_id = stream_id

    def get_metadata_properties(self) -> Union[Dict, None]:
        """Get available values from the _meta_dataproducts_versions_instruments table for the corresponding row"""
        supported_properties = {'data_begin', 'data_end', 'last_updated'}

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                sql = f"""
                    SELECT {','.join(sorted(supported_properties))}
                    FROM _meta_dataproducts_versions_instruments as mdpvi
                    WHERE mdpvi._meta_dataproducts_versions_id in (
                        SELECT id 
                        FROM _meta_dataproducts_versions as mdpv
                        WHERE mdpv.name=%(version_name)s
                        AND mdpv._meta_dataproducts_id in (
                            SELECT id
                            FROM _meta_dataproducts as mdp
                            WHERE mdp.name=%(data_product_name)s
                        )
                    )
                    AND mdpvi._meta_instruments_id in (
                        SELECT id 
                        FROM _meta_instruments as mi
                        WHERE mi.name=%(instrument_name)s
                    );
                    """
                cur.execute(sql, {'data_product_name': self.product.get_full_id(), 'version_name': self.version.value,
                                  'instrument_name': self.stream_id})
                result = cur.fetchone()
            except Exception as err:
                logging.info(f'query failed with {err}: {sql}')
                return None

        return result

    def get_data_span(self) -> Union[TimeSpan, None]:
        begin = self._get_data_begin()
        end = self._get_data_end()

        if begin is not None and end is not None:
            return TimeSpan(begin=begin, end=end)
        else:
            return None

    def _get_data_span_stat(self, agg: str) -> Union[datetime, None]:
        """Get either the min or max timestamp for a given dataset, version and stream"""
        if agg not in {'min', 'max'}:
            raise ValueError(f'"{agg}" is not a supported timespan stat')

        with get_db_connection() as conn, conn.cursor() as cur:
            table_name = self.get_table_name()

            try:
                sql = f"""
                       SELECT {agg}({self.product.TIMESTAMP_COLUMN_NAME})
                       FROM {table_name}
                       """
                cur.execute(sql)
                result = cur.fetchone()[0]
            except Exception as err:
                logging.info(f'query failed with {err}: {sql}')
                return None

        return result

    def _get_data_begin(self) -> Union[datetime, None]:
        return self._get_data_span_stat('min')

    def _get_data_end(self) -> Union[datetime, None]:
        return self._get_data_span_stat('max')

    def select(self, from_dt: datetime, to_dt: datetime,
               fields: Collection[TimeSeriesDataProductField] = None, aggregation_level: int = 0,
               limit_data_span: bool = True) -> List[Dict]:
        fields = fields or [f for f in self.product.get_available_fields() if not f.is_constant]
        using_aggregations = aggregation_level > 0
        self.product.validate_requested_fields(fields, using_aggregations=using_aggregations)
        if not using_aggregations:
            column_names = {field.name for field in fields}
        else:
            column_names = set()
            for field in fields:
                if field.has_aggregations:
                    column_names.update(field.aggregation_db_column_names)
                else:
                    column_names.add(field.name)

        downsampling_factor = self.product.aggregation_step_factor ** aggregation_level
        max_query_temporal_span = self.product.query_result_limit * self.product.time_series_interval * downsampling_factor
        requested_temporal_span = to_dt - from_dt
        if limit_data_span and requested_temporal_span > max_query_temporal_span:
            raise TooMuchDataRequestedError(
                f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} at 1:{downsampling_factor} aggregation exceeds maximum allowed by server ({get_human_readable_timedelta(max_query_temporal_span)})')

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            table_name = self.get_table_or_view_name(aggregation_level)
            select_columns_clause = ','.join(column_names)

            try:
                sql = f"""
                    SELECT {select_columns_clause}
                    FROM {table_name}
                    WHERE   {self.product.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                        AND {self.product.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                    ORDER BY {self.product.TIMESTAMP_COLUMN_NAME}
                    """
                cur.execute(sql, {'from_dt': from_dt, 'to_dt': to_dt})
                results = cur.fetchall()
            except psycopg2.errors.UndefinedTable as err:
                logging.error(f'Query failed with {err}: {sql}')
                raise RuntimeError(
                    f'Table {table_name} is not present in db.  Files may not been ingested for this dataset.')
            except psycopg2.errors.UndefinedColumn as err:
                logging.error(f'Query failed due to mismatch between dataset definition and database schema: {err}')
                available_columns = list_db_table_columns(table_name)
                missing_columns = {f.name for f in self.product.get_available_fields() if
                                   f.name not in available_columns}
                raise ValueError(
                    f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
            except Exception as err:
                logging.error(f'query failed with {err}: {sql}')
                raise Exception

        return [self.product.structure_results(fields, using_aggregations, result) for result in results]

    def get_table_name(self) -> str:
        """Return the name of the SQL table storing the data for this dataset for a given stream"""
        return self.get_table_or_view_name(aggregation_depth=0)

    def get_table_or_view_name(self, aggregation_depth: int) -> str:
        """
        Return the name of the SQL table or view providing access to data for this dataset for a given stream at a given
        aggregation level
        """
        if self.stream_id not in self.product.stream_ids:
            raise ValueError(
                f'stream id "{self.stream_id}" not recognized (expected one of {sorted(self.product.stream_ids)})')

        aggregation_depth_pad_width = 2
        padded_aggregation_depth = str(aggregation_depth).rjust(aggregation_depth_pad_width, "0")
        if len(padded_aggregation_depth) > aggregation_depth_pad_width:
            raise ValueError(
                f'aggregation_depth "{aggregation_depth}" exceeds maximum accounted for ({aggregation_depth_pad_width} digits)')

        # f for factor, l for level - aids in view maintenance
        aggregation_suffix = f'f{self.product.aggregation_step_factor}l{padded_aggregation_depth}'

        # TODO: Remove legacy null-version support when fully implemented/migrated
        table_base_name = (f'{self.product.get_table_name_prefix()}_{self.stream_id}'.lower()
                           if self.version.is_null
                           else f'{self.product.get_table_name_prefix()}_{str(self.version)}_{self.stream_id}'.lower())

        return (table_base_name if aggregation_depth == 0 else f'{table_base_name}_{aggregation_suffix}').lower()

    def get_sql_table_create_statement(self) -> str:
        # TODO: Perhaps generate this from column definitions rather than hardcoding per-class?  Need to think about it.
        """Get an SQL statement to create a table for this dataset/stream"""
        if self.stream_id not in self.product.stream_ids:
            raise ValueError(
                f'stream_id {self.stream_id} not in {self.product.__name__}.stream_ids - expected one of {self.product.stream_ids}')

        sql = f"""
            create table public.{self.get_table_name()}
            (
                {self.product.get_sql_table_schema()}
            );
        """
        return sql
