import logging
import math
from collections.abc import Collection
from datetime import datetime
from typing import List, Dict, Union, Iterable

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import cursor as Cursor
from psycopg2.sql import SQL, Identifier

from masschange.api.errors import TooMuchDataRequestedError
from masschange.api.utils.misc import KeyValueQueryParameter
from masschange.dataproducts.implementations.gracefo.primary.gnv1a import GraceFOGnv1ADataProduct
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField, \
    TimeSeriesDataProductLocationLookupField
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.dataproducts.db.utils import get_db_connection, list_table_columns as list_db_table_columns, \
    prepare_where_clause_conditions, prepare_where_clause_parameters
from masschange.utils.misc import get_human_readable_timedelta
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


class TimeSeriesDataset:
    product: TimeSeriesDataProduct
    version: TimeSeriesDatasetVersion
    instrument_id: str

    def __init__(self, product: TimeSeriesDataProduct, version: TimeSeriesDatasetVersion, instrument_id: str):
        self.product = product
        self.version = version
        self.instrument_id = instrument_id

    def get_metadata_properties(self) -> Union[Dict, None]:
        """Get available values from the _meta_dataproducts_versions_instruments table for the corresponding row"""
        supported_properties = {'data_begin', 'data_end', 'last_updated'}

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                metadata = self._get_basic_metadata(cur, supported_properties)
                metadata['time_series_id_enums'] = self._enumerate_time_series_id_values(cur)
            except Exception as err:
                logging.warning(err)
                return None

        return metadata

    def get_data_span(self) -> Union[TimeSpan, None]:
        begin = self._get_data_begin()
        end = self._get_data_end()

        if begin is not None and end is not None:
            return TimeSpan(begin=begin, end=end)
        else:
            return None

    def _get_data_span_stat(self, agg: str) -> Union[datetime, None]:
        """Get either the min or max timestamp for a given dataset, version and instruments"""
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
                logging.warning(f'query failed with {err}: {sql}')
                return None

        return result

    def _get_data_begin(self) -> Union[datetime, None]:
        return self._get_data_span_stat('min')

    def _get_data_end(self) -> Union[datetime, None]:
        return self._get_data_span_stat('max')

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

    def _enumerate_time_series_id_values(self, cur: Cursor) -> Dict:
        if not self.product.has_time_series_id_fields():
            return {}

        time_series_id_column_names = [f.name for f in self.product.get_available_fields() if f.is_time_series_id_column]
        # To avoid long queries, a view is used rather than the full-res dataset.  The level must be low enough that it
        # is safe to assume all possible values have been written to that materialized view. 5 is a good starting point.
        view_depth = min(([0] + self.product.get_available_aggregation_levels())[-1], 5)
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
            if column_name == TimeSeriesDataProduct.LOCATION_COLUMN_NAME:
                clause += f"st_x({TimeSeriesDataProduct.LOCATION_COLUMN_NAME}) as longitude, st_y({TimeSeriesDataProduct.LOCATION_COLUMN_NAME}) as latitude"
            else:
                clause += column_name

            if idx < len(column_names) - 1:
                clause += ", "

        return clause

    def select(self, from_dt: datetime, to_dt: datetime,
               fields: Collection[TimeSeriesDataProductField] = None, aggregation_level: int = None,
               limit_data_span: bool = True, resolve_location: bool = False, filters: List[KeyValueQueryParameter] = None) -> List[Dict]:
        filters = filters or []

        if aggregation_level is None:
            aggregation_level = self.get_minimum_aggregation_level(from_dt, to_dt)

        using_aggregations = aggregation_level > 0

        if fields is None:
            fields = {f for f in self.product.get_available_fields() \
                      if not f.is_constant \
                      and not f.is_lookup_field \
                      and (f.has_aggregations or not using_aggregations)}
            if resolve_location:
                try:
                    location_lookup_field = next(f for f in fields if isinstance(f, TimeSeriesDataProductLocationLookupField))
                    fields.add(location_lookup_field)
                except StopIteration:
                    pass

        non_lookup_fields = [f for f in fields if not f.is_lookup_field]

        self.product.validate_requested_fields(non_lookup_fields, using_aggregations=using_aggregations)

        if not using_aggregations:
            column_names = {field.name for field in non_lookup_fields}
        else:
            column_names = set()
            for field in non_lookup_fields:
                if field.has_aggregations:
                    aggregate_column_names =  {agg.get_aggregated_name(field.name) for agg in field.aggregations}
                    column_names.update(aggregate_column_names)
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
            select_columns_clause = self._get_sql_select_columns_clause(column_names)

            parameters = prepare_where_clause_parameters(from_dt, to_dt, filters)
            conditions = prepare_where_clause_conditions(self.product.TIMESTAMP_COLUMN_NAME, filters)
            where_clause = SQL(' AND ').join(conditions).as_string(conn)

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

        try:
            if resolve_location:
                self.attach_lat_lon(from_dt, to_dt, results)
        except psycopg2.Error as err:
            log.error(f'Failed to resolve location data for {self.get_table_name()} over span ({from_dt}, {to_dt}) '
                      f'due to {err}')

        return [self.product.structure_results(fields, using_aggregations, result) for result in results]

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
        table_base_name = (f'{self.product.get_table_name_prefix()}_{self.instrument_id}'.lower()
                           if self.version.is_null
                           else f'{self.product.get_table_name_prefix()}_{str(self.version)}_{self.instrument_id}'.lower())

        return (table_base_name if aggregation_depth == 0 else f'{table_base_name}_{aggregation_suffix}').lower()

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

    def attach_lat_lon(self, from_dt: datetime, to_dt: datetime, data: Iterable[Dict]) -> None:
        """
        Assign approximate locations to a set of results from TimeSeriesDataset.select(), using ingested GNV data to map
        datum timestamps to a lat/lon.  The format is 'location': {'latitude': $value, 'longitude': $value}
        The maximal error will be equal to +/- the satellite's movement in one second (i.e. half the temporal resolution
        of the GNV dataset).
        A value of None will be assigned to input data for which there is no GNV data available.
        """
        gnv_dataset = TimeSeriesDataset(GraceFOGnv1ADataProduct(), self.version, self.instrument_id)
        gnv_field_names = {gnv_dataset.product.TIMESTAMP_COLUMN_NAME, 'location'}
        gnv_fields = [f for f in gnv_dataset.product.get_available_fields() if f.name in gnv_field_names]

        # Need to ensure that the GNV data span fully encloses the input data span
        gnv_from_dt = from_dt - GraceFOGnv1ADataProduct.time_series_interval
        gnv_to_dt = to_dt + GraceFOGnv1ADataProduct.time_series_interval
        gnv_data = gnv_dataset.select(gnv_from_dt, gnv_to_dt, gnv_fields)

        try:
            data_iter = iter(data)
            geo_iter = iter(gnv_data)

            data_el = next(data_iter)
            gnv_pair_begin = None
            gnv_pair_end = next(geo_iter)

            # DEV WARNING: Here be dragons - the nested iteration is easy to mess up and unit tests don't exist yet.
            while True:  # iterate until a StopIteration
                gnv_pair_begin = gnv_pair_end
                gnv_pair_end = next(geo_iter)

                gnv_begin_ts = gnv_pair_begin[GraceFOGnv1ADataProduct.TIMESTAMP_COLUMN_NAME]
                gnv_end_ts = gnv_pair_end[GraceFOGnv1ADataProduct.TIMESTAMP_COLUMN_NAME]
                el_ts = data_el[self.product.TIMESTAMP_COLUMN_NAME]

                # If datum exists before start of the GNV pair, assign it a null value and move on
                # This should ONLY occur for the first GNV pair, and should loop through all data elements with
                # timestamps earlier than the available GNV data
                if (el_ts < gnv_begin_ts):
                    data_el[self.product.LOCATION_COLUMN_NAME] = None
                    data_el = next(data_iter)
                    continue

                # for each datum falling within the timespan bounded by the gnv pair, assign it the location of
                #  the closest bounding gnv record
                while (gnv_begin_ts <= el_ts <= gnv_end_ts):
                    if abs(el_ts - gnv_begin_ts) <= abs(el_ts - gnv_end_ts):
                        data_el[self.product.LOCATION_COLUMN_NAME] = gnv_pair_begin[
                            GraceFOGnv1ADataProduct.LOCATION_COLUMN_NAME]
                    else:
                        data_el[self.product.LOCATION_COLUMN_NAME] = gnv_pair_end[
                            GraceFOGnv1ADataProduct.LOCATION_COLUMN_NAME]

                    data_el = next(data_iter)
                    el_ts = data_el[self.product.TIMESTAMP_COLUMN_NAME]

        except StopIteration:
            pass

        # Assign null location to all data after end of available GNV data
        while (data_el := next(data_iter, None)) is not None:
            data_el[self.product.LOCATION_COLUMN_NAME] = None

    def get_minimum_aggregation_level(self, from_dt: datetime, to_dt: datetime, check_data_span: bool = False):
        """
        Given a query span, return the lowest aggregation level required to limit the result to the product's query
        result limit

        Parameters
        ----------
        from_dt: datetime
        to_dt: datetime
        check_data_span: bool - if true, will query the db for actual data span and trim the requested bounds
            accordingly. Gives absolute minimum aggregation level but is slower due to overhead

        """
        extant_data_span = self.get_data_span()
        if check_data_span:
            span_duration = max(to_dt, extant_data_span.begin) - min(from_dt, extant_data_span.end)
        else:
            span_duration = to_dt - from_dt
        full_res_data_count = span_duration / self.product.time_series_interval
        downsampling_factor_lower_bound = full_res_data_count / self.product.query_result_limit
        # return the lowest index for all factors which meet or exceed the lower bound
        return min(i for i, f in enumerate(self.product.get_available_downsampling_factors()) if f >= downsampling_factor_lower_bound)