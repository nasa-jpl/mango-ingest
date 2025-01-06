import logging
from datetime import timedelta
import psycopg2
from psycopg2 import extras
from psycopg2.extensions import cursor as Cursor
from typing import List
from psycopg2.sql import SQL
from collections.abc import Collection
from typing import Dict
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.utils.timespan import TimeSpan
from typing import Union
from datetime import datetime
from masschange.db.conn import get_db_cursor
from psycopg2.extensions import cursor as Cursor
from masschange.api.errors import TooMuchDataRequestedError
from masschange.utils.misc import get_human_readable_timedelta
from masschange.dataproducts.db.utils import list_table_columns as list_db_table_columns, \
    prepare_where_clause_conditions, prepare_where_clause_parameters
from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField, \
    TimeSeriesDataProductLocationLookupField
from masschange.api.utils.misc import KeyValueQueryParameter

log = logging.getLogger()


class Dataset:
    product: DataProduct
    version: TimeSeriesDatasetVersion
    instrument_id: str

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

    def get_metadata_properties(self) -> Union[Dict, None]:
        """Get available values from the _meta_dataproducts_versions_instruments table for the corresponding row"""
        supported_properties = {'data_begin', 'data_end', 'last_updated'}

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                metadata = self._get_basic_metadata(cur, supported_properties)
            except Exception as err:
                logging.warning(err)
                return None

        return metadata

    def _get_basic_metadata(self, cur: Cursor, supported_properties: Collection[str]) -> Dict:
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
        try:
            cur.execute(sql, {'data_product_name': self.product.get_full_id(), 'version_name': self.version.value,
                              'instrument_name': self.instrument_id})
        except Exception as err:
            raise err.__class__(f'query failed with {err}: {sql}')
        result = cur.fetchone()
        return result


    @staticmethod
    def _get_sql_select_columns_clause(column_names: Collection[str]):
        """
        Given a collection of column names, return a select clause to fetch those columns when querying SQL.
        Processes special cases (in this case, just location) where some transformation must be applied between SQL-land
        and Python-land.

        This type of behaviour may end up being necessary for fields other than location.  If this is necessary, this
        should be refactored, as this implementation is a stopgap approach.
        """
        column_names = list(set(column_names))  # deduplicate and store in indexable format
        clause = ''
        for idx, column_name in enumerate(column_names):
            if column_name == DataProduct.LOCATION_COLUMN_NAME:
                clause += f"st_x({DataProduct.LOCATION_COLUMN_NAME}) as longitude, st_y({DataProduct.LOCATION_COLUMN_NAME}) as latitude"
            else:
                clause += column_name

            if idx < len(column_names) - 1:
                clause += ", "

        return clause

    def select(self, from_dt: datetime, to_dt: datetime,
               fields: Collection[TimeSeriesDataProductField] = None, # aggregation_level: int = None, #TODO: how to deal with different signature of 'select' in the child?
               limit_data_span: bool = True, # resolve_location: bool = False,
               filters: List[KeyValueQueryParameter] = None) -> List[Dict]:

        filters = filters or []

        if fields is None:
            fields = {f for f in self.product.get_available_fields() \
                      if not f.is_constant \
                      and not f.is_lookup_field}

        non_lookup_fields = [f for f in fields if not f.is_lookup_field]

        self.product.validate_requested_fields(non_lookup_fields, using_aggregations=False)

        column_names = {field.name for field in non_lookup_fields}

        max_query_temporal_span = timedelta(days=31)  # TODO: set to 31 days for now

        requested_temporal_span = to_dt - from_dt
        if limit_data_span and requested_temporal_span > max_query_temporal_span:
            raise TooMuchDataRequestedError(
                f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} '
                f' exceeds maximum allowed by server ({get_human_readable_timedelta(max_query_temporal_span)})')

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            table_name = self.get_table_name()
            select_columns_clause = self._get_sql_select_columns_clause(column_names)

            parameters = prepare_where_clause_parameters(from_dt, to_dt, filters)
            conditions = prepare_where_clause_conditions(self.product.TIMESTAMP_COLUMN_NAME, filters)
            where_clause = SQL(' AND ').join(conditions).as_string(cur.connection)

            try:
                sql = f"""
                    SELECT {select_columns_clause}
                    FROM {table_name}
                    WHERE {where_clause}
                    ORDER BY {self.product.TIMESTAMP_COLUMN_NAME}
                    """
                cur.execute(sql, parameters)
                results = cur.fetchall()
            except psycopg2.errors.UndefinedTable as err:
                logging.warning(f'Query failed with {err}: {sql}')
                raise RuntimeError(
                    f'Table {table_name} is not present in db.  Files may not been ingested for this dataset.')
            except psycopg2.errors.UndefinedColumn as err:
                logging.error(f'Query failed due to mismatch between dataset definition and database schema: {err}')
                available_columns = list_db_table_columns(table_name)
                missing_columns = {f.name for f in self.product.get_available_fields() if
                                   f.name not in available_columns and not f.is_lookup_field}
                raise ValueError(
                    f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
            except Exception as err:
                logging.warning(f'query failed with {err}: {sql}')
                raise Exception

        return [self.product.structure_results(fields, False, result) for result in results]
