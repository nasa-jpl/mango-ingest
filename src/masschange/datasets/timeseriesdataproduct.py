import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import List, Dict, Set, Type, Union

import psycopg2
from psycopg2 import extras

from masschange.api.errors import TooMuchDataRequestedError
from masschange.datasets.timeseriesdataproductfield import TimeSeriesDataProductField, TimeSeriesDataProductTimestampField
from masschange.datasets.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.db import get_db_connection
from masschange.db.utils import list_table_columns as list_db_table_columns
from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.missions import Mission
from masschange.utils.misc import get_human_readable_timedelta
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


class TimeSeriesDataProduct(ABC):
    # TODO: Document this class properly
    description: str = ''
    mission: Type[Mission]
    id_suffix: str  # TODO: come up with a better name for this - it's used as a full id in the API so need to iron out the nomenclature
    stream_ids: Set[str]
    time_series_interval: timedelta
    processing_level: str

    aggregation_step_factor: int = 5
    max_data_span = timedelta(weeks=52 * 30)  # extent of full data span for determining aggregation steps
    query_result_limit = 36000

    TIMESTAMP_COLUMN_NAME = 'timestamp'

    @classmethod
    def get_full_id(cls) -> str:
        return f'{cls.mission.id}_{cls.id_suffix}'

    @classmethod
    def describe(cls, exclude_available_versions: bool = False) -> Dict:
        """
        Returns
        -------
        An object which describes this dataset's attributes/configuration to an end-user, providing details which are
        useful or necessary for querying it.
        """
        description = {
            'description': cls.description,
            'mission': cls.mission.id,
            'id': cls.id_suffix,
            'full_id': cls.get_full_id(),
            'processing_level': cls.processing_level,
            'streams': [{
                'id': id,
                # These are disabled for the time being, as they are slow
                # TODO: Store these in the metadata table that doesn't exist yet
                # 'data_begin': cls.get_data_begin(id),
                # 'data_end': cls.get_data_end(id)
            } for id in
                sorted(cls.stream_ids)],
            'available_fields': sorted([field.describe() for field in cls.get_available_fields()],
                                       key=lambda description: description['name']),
            'available_resolutions': [
                {
                    'downsampling_factor': factor,
                    'nominal_data_interval_seconds': cls.time_series_interval.total_seconds() * factor
                } for factor in cls.get_available_downsampling_factors()
            ],
            'timestamp_field': cls.TIMESTAMP_COLUMN_NAME,
            'query_result_limit': cls.query_result_limit
        }

        # This includes a db call, and may not always be useful
        if not exclude_available_versions:
            description.update({'available_versions': [str(version) for version in cls.get_available_versions()]})

        return description

    @classmethod
    def get_metadata_properties(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> \
            Dict:
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
                cur.execute(sql, {'data_product_name': cls.get_full_id(), 'version_name': dataset_version.value,
                                  'instrument_name': stream_id})
                result = cur.fetchone()
            except Exception as err:
                logging.info(f'query failed with {err}: {sql}')
                return None

        return result

    @classmethod
    def get_data_span(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> Union[TimeSpan, None]:
        begin = cls._get_data_begin(dataset_version, stream_id)
        end = cls._get_data_end(dataset_version, stream_id)

        if (begin is not None and end is not None):
            return TimeSpan(begin=begin, end=end)
        else:
            return None

    @classmethod
    def _get_data_span_stat(cls, agg: str, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> Union[
        datetime, None]:
        """Get either the min or max timestamp for a given dataset, version and stream"""
        if agg not in {'min', 'max'}:
            raise ValueError(f'"{agg}" is not a supported timespan stat')

        with get_db_connection() as conn, conn.cursor() as cur:
            table_name = cls.get_table_name(dataset_version, stream_id)

            try:
                sql = f"""
                       SELECT {agg}({cls.TIMESTAMP_COLUMN_NAME})
                       FROM {table_name}
                       """
                cur.execute(sql)
                result = cur.fetchone()[0]
            except Exception as err:
                logging.info(f'query failed with {err}: {sql}')
                return None

        return result

    @classmethod
    def _get_data_begin(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> Union[datetime, None]:
        return cls._get_data_span_stat('min', dataset_version, stream_id)

    @classmethod
    def _get_data_end(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> Union[datetime, None]:
        return cls._get_data_span_stat('max', dataset_version, stream_id)

    @classmethod
    def select(cls, dataset_version, stream_id: str, from_dt: datetime, to_dt: datetime,
               fields: Collection[TimeSeriesDataProductField] = None, aggregation_level: int = 0,
               limit_data_span: bool = True) -> List[Dict]:
        fields = fields or [f for f in cls.get_available_fields() if not f.is_constant]
        using_aggregations = aggregation_level > 0
        cls._validate_requested_fields(fields, using_aggregations=using_aggregations)
        if not using_aggregations:
            column_names = {field.name for field in fields}
        else:
            column_names = set()
            for field in fields:
                if field.has_aggregations:
                    column_names.update(field.aggregation_db_column_names)
                else:
                    column_names.add(field.name)

        downsampling_factor = cls.aggregation_step_factor ** aggregation_level
        max_query_temporal_span = cls.query_result_limit * cls.time_series_interval * downsampling_factor
        requested_temporal_span = to_dt - from_dt
        if limit_data_span and requested_temporal_span > max_query_temporal_span:
            raise TooMuchDataRequestedError(
                f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} at 1:{downsampling_factor} aggregation exceeds maximum allowed by server ({get_human_readable_timedelta(max_query_temporal_span)})')

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            table_name = cls.get_table_or_view_name(dataset_version, stream_id, aggregation_level)
            select_columns_clause = ','.join(column_names)

            try:
                sql = f"""
                    SELECT {select_columns_clause}
                    FROM {table_name}
                    WHERE   {cls.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                        AND {cls.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                    ORDER BY {cls.TIMESTAMP_COLUMN_NAME}
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
                missing_columns = {f.name for f in cls.get_available_fields() if f.name not in available_columns}
                raise ValueError(
                    f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
            except Exception as err:
                logging.error(f'query failed with {err}: {sql}')
                raise Exception

        return [cls._structure_results(fields, using_aggregations, result) for result in results]

    @classmethod
    def _get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()

    @classmethod
    def get_table_name(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> str:
        """Return the name of the SQL table storing the data for this dataset for a given stream"""
        return cls.get_table_or_view_name(dataset_version, stream_id, aggregation_depth=0)

    @classmethod
    def get_table_or_view_name(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str,
                               aggregation_depth: int) -> str:
        """
        Return the name of the SQL table or view providing access to data for this dataset for a given stream at a given
        aggregation level
        """
        if stream_id not in cls.stream_ids:
            raise ValueError(f'stream id "{stream_id}" not recognized (expected one of {sorted(cls.stream_ids)})')

        aggregation_depth_pad_width = 2
        padded_aggregation_depth = str(aggregation_depth).rjust(aggregation_depth_pad_width, "0")
        if len(padded_aggregation_depth) > aggregation_depth_pad_width:
            raise ValueError(
                f'aggregation_depth "{aggregation_depth}" exceeds maximum accounted for ({aggregation_depth_pad_width} digits)')

        # f for factor, l for level - aids in view maintenance
        aggregation_suffix = f'f{cls.aggregation_step_factor}l{padded_aggregation_depth}'

        # TODO: Remove legacy null-version support when fully implemented/migrated
        table_base_name = (f'{cls._get_table_name_prefix()}_{stream_id}'.lower()
                           if dataset_version.is_null
                           else f'{cls._get_table_name_prefix()}_{str(dataset_version)}_{stream_id}'.lower())

        return (table_base_name if aggregation_depth == 0 else f'{table_base_name}_{aggregation_suffix}').lower()

    @classmethod
    def _validate_requested_fields(cls, requested_fields: Collection[TimeSeriesDataProductField],
                                   using_aggregations: bool) -> None:
        requested_fields = set(requested_fields)
        available_fields = {f for f in cls.get_available_fields() \
                            if (
                                    f.has_aggregations or f.name == cls.TIMESTAMP_COLUMN_NAME or not using_aggregations) and not f.is_constant}
        if not all([f in available_fields for f in requested_fields]):
            available_field_names = [f.name for f in available_fields]
            # requested fields which aren't available for selection
            unavailable_fields = {f for f in requested_fields.difference(available_fields)}
            unavailable_field_names = {f.name for f in unavailable_fields}
            # requested fields which aren't available for selection due to lack of defined aggregations
            unavailable_aggregate_field_names = {f.name for f in unavailable_fields if
                                                 not f.has_aggregations} if using_aggregations else set()

            msg = f'Some requested fields {sorted(unavailable_field_names)} not present in available fields ({sorted(available_field_names)}).'

            if len(unavailable_aggregate_field_names) > 0:
                msg += f' The following fields are unavailable due to lack of defined aggregations: {sorted(unavailable_aggregate_field_names)}'

            raise ValueError(msg)

    @classmethod
    def _structure_results(cls, requested_fields: Collection[TimeSeriesDataProductField], using_aggregations: bool,
                           result: Dict) -> Dict:
        structured_result = {}
        for field in requested_fields:
            if field.name == cls.TIMESTAMP_COLUMN_NAME:
                structured_result[field.name] = result[field.name]

            elif using_aggregations and field.has_aggregations:
                for column_name in field.aggregation_db_column_names:
                    agg_name = column_name.replace(f'{field.name}_', '', 1)
                    if field.name not in structured_result:
                        structured_result[field.name] = {}
                    structured_result[field.name][agg_name] = result[column_name]

            else:
                if field.name not in structured_result:
                    structured_result[field.name] = {}
                structured_result[field.name]['value'] = result[field.name]

        return structured_result

    @classmethod
    def get_sql_table_create_statement(cls, dataset_version: TimeSeriesDatasetVersion, stream_id: str) -> str:
        # TODO: Perhaps generate this from column definitions rather than hardcoding per-class?  Need to think about it.
        """Get an SQL statement to create a table for this dataset/stream"""
        if stream_id not in cls.stream_ids:
            raise ValueError(
                f'stream_id {stream_id} not in {cls.__name__}.stream_ids - expected one of {cls.stream_ids}')

        sql = f"""
            create table public.{cls.get_table_name(dataset_version, stream_id)}
            (
                {cls._get_sql_table_schema()}
            );
        """
        return sql

    @classmethod
    @abstractmethod
    def _get_sql_table_schema(cls) -> str:
        """
        Get the column definitions used in the SQL create table statement.
        Must be defined in every subclass.
        """
        pass

    @classmethod
    @abstractmethod
    def get_reader(cls) -> DataFileReader:
        """Return the DataFileReader used to ingest this dataset"""
        pass

    @classmethod
    def get_available_fields(cls) -> Set[TimeSeriesDataProductField]:
        timestamp_field: TimeSeriesDataProductField = TimeSeriesDataProductTimestampField(cls.TIMESTAMP_COLUMN_NAME, 'n/a')
        return {timestamp_field}.union(cls.get_reader().get_fields())

    @classmethod
    def get_available_versions(cls) -> Set[TimeSeriesDatasetVersion]:
        with get_db_connection() as conn, conn.cursor() as cur:
            data_product_name = cls.get_full_id()

            sql = f"""
                SELECT v.name
                FROM _meta_dataproducts_versions as v 
                WHERE v._meta_dataproducts_id in (
                    SELECT dp.id
                    FROM _meta_dataproducts as dp
                    WHERE dp.name=%(data_product_name)s
                );
                """
            cur.execute(sql, {'data_product_name': data_product_name})
            results = [row[0] for row in cur.fetchall()]

        return {TimeSeriesDatasetVersion(version_name) for version_name in results}

    @classmethod
    def get_required_aggregation_depth(cls) -> int:
        if not any(field.has_aggregations for field in cls.get_available_fields()):
            return 0

        approximate_pixel_count = 5000
        full_span_data_count = cls.max_data_span / cls.time_series_interval
        required_decimation_levels = math.ceil(
            math.log(full_span_data_count / approximate_pixel_count, cls.aggregation_step_factor))
        return required_decimation_levels

    @classmethod
    def get_available_aggregation_levels(cls) -> Sequence[int]:
        """
        Return the sorted levels (hierarchical level, not decimation factor) of aggregation which exist for this dataset
        , *exclusive* of level 0 (full-resolution)
        """
        return [x for x in range(1, cls.get_required_aggregation_depth() + 1)]

    @classmethod
    def get_available_downsampling_factors(cls) -> Sequence[int]:
        """
        Return the sorted downsampling resolution factors (full-res and aggregated) which exist for this dataset
        """
        return [1] + [cls.aggregation_step_factor ** level for level in cls.get_available_aggregation_levels()]

    @classmethod
    def get_nominal_data_interval(cls, downsampling_level: int) -> timedelta:
        """
        For a given downsampling level (hierarchical level, not factor), return the nominal interval between data.  For
        aggregated data, this is the bucket width
        """
        return cls.time_series_interval * cls.aggregation_step_factor ** downsampling_level
