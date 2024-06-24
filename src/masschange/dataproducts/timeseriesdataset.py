import logging
import math
from collections.abc import Collection
from datetime import datetime
from typing import List, Dict, Union, Iterable

import psycopg2
from psycopg2 import extras

from masschange.api.errors import TooMuchDataRequestedError
from masschange.dataproducts.implementations.gracefo.gnv1a import GraceFOGnv1ADataProduct
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField, \
    TimeSeriesDataProductLocationLookupField
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
               limit_data_span: bool = True, resolve_location: bool = False) -> List[Dict]:
        if aggregation_level is None:
            aggregation_level = self._get_minimum_aggregation_level(from_dt, to_dt)

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
                                   f.name not in available_columns and not f.is_lookup_field}
                raise ValueError(
                    f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
            except Exception as err:
                logging.error(f'query failed with {err}: {sql}')
                raise Exception

        try:
            if resolve_location:
                self.attach_lat_lon(from_dt, to_dt, results)
        except psycopg2.Error as err:
            log.error(f'Failed to resolve location data for {self.get_table_name()} over span ({from_dt}, {to_dt}) '
                      f'due to {err}')

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

    def attach_lat_lon(self, from_dt: datetime, to_dt: datetime, data: Iterable[Dict]) -> None:
        """
        Assign approximate locations to a set of results from TimeSeriesDataset.select(), using ingested GNV data to map
        datum timestamps to a lat/lon.  The format is 'location': {'latitude': $value, 'longitude': $value}
        The maximal error will be equal to +/- the satellite's movement in one second (i.e. half the temporal resolution
        of the GNV dataset).
        A value of None will be assigned to input data for which there is no GNV data available.
        """
        gnv_dataset = TimeSeriesDataset(GraceFOGnv1ADataProduct(), self.version, self.stream_id)
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

            while True:  # iterate until a StopIteration
                gnv_pair_begin = gnv_pair_end
                gnv_pair_end = next(geo_iter)

                gnv_begin_ts = gnv_pair_begin[GraceFOGnv1ADataProduct.TIMESTAMP_COLUMN_NAME]
                gnv_end_ts = gnv_pair_end[GraceFOGnv1ADataProduct.TIMESTAMP_COLUMN_NAME]
                el_ts = data_el[self.product.TIMESTAMP_COLUMN_NAME]

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

        while (data_el := next(data_iter, None)) is not None:
            data_el[self.product.LOCATION_COLUMN_NAME] = None

    def _get_minimum_aggregation_level(self, from_dt: datetime, to_dt: datetime, check_data_span: bool = False):
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
        min_downsampling_factor = full_res_data_count / self.product.query_result_limit
        downsampling_level = math.ceil(math.log(min_downsampling_factor, self.product.aggregation_step_factor))
        return max(downsampling_level, 0)