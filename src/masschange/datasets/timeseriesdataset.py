import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import List, Dict, Set, Type, Union

import psycopg2
from psycopg2 import extras

from masschange.api.errors import TooMuchDataRequestedError
from masschange.datasets.timeseriesdatasetfield import TimeSeriesDatasetField
from masschange.db import get_db_connection
from masschange.db.utils import list_table_columns as list_db_table_columns
from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.missions import Mission
from masschange.utils.misc import get_human_readable_timedelta

log = logging.getLogger()


class TimeSeriesDataset(ABC):
    # TODO: Document this class properly
    mission: Type[Mission]
    id_suffix: str  # TODO: come up with a better name for this - it's used as a full id in the API so need to iron out the nomenclature
    stream_ids: Set[str]
    time_series_interval: timedelta

    aggregation_step_factor: int = 10
    max_data_span = timedelta(weeks=52 * 30)  # extent of full data span for determining aggregation steps
    max_full_res_query_temporal_span: timedelta = timedelta(
        minutes=60)  # TODO: When downsampling and non-10Hz data are implemented this will need to be dynamically generated

    TIMESTAMP_COLUMN_NAME = 'timestamp'

    @classmethod
    def get_full_id(cls) -> str:
        return f'{cls.mission.id}_{cls.id_suffix}'

    @classmethod
    def describe(cls) -> Dict:
        """

        Returns
        -------
        An object which describes this dataset's attributes/configuration to an end-user, providing details which are
        useful or necessary for querying it.
        """
        return {
            'mission': cls.mission.id,
            'id': cls.id_suffix,
            'full_id': cls.get_full_id(),
            'streams': [{'id': id, 'data_begin': cls.get_data_begin(id), 'data_end': cls.get_data_end(id)} for id in
                        sorted(cls.stream_ids)],
            'available_fields': sorted([field.describe() for field in cls.get_available_fields()], key=lambda description: description['name']),
            'timestamp_field': cls.TIMESTAMP_COLUMN_NAME,
            'approximate_query_result_limit': cls.max_full_res_query_temporal_span / cls.time_series_interval
        }

    @classmethod
    def _get_data_span_stat(cls, agg: str, stream_id: str) -> Union[datetime, None]:
        """"""
        if agg not in {'min', 'max'}:
            raise ValueError(f'"{agg}" is not a supported timespan stat')

        with get_db_connection() as conn, conn.cursor() as cur:
            table_name = cls.get_table_name(stream_id)

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
    def get_data_begin(cls, stream_id: str) -> Union[datetime, None]:
        return cls._get_data_span_stat('min', stream_id)

    @classmethod
    def get_data_end(cls, stream_id: str) -> Union[datetime, None]:
        return cls._get_data_span_stat('max', stream_id)

    @classmethod
    def select(cls, stream_id: str, from_dt: datetime, to_dt: datetime,
               fields: Collection[TimeSeriesDatasetField] = None, aggregation_level: int = 0, limit_data_span: bool = True) -> List[Dict]:
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
        max_query_temporal_span = cls.max_full_res_query_temporal_span * downsampling_factor
        requested_temporal_span = to_dt - from_dt
        if limit_data_span and requested_temporal_span > max_query_temporal_span:
            raise TooMuchDataRequestedError(f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} at 1:{downsampling_factor} aggregation exceeds maximum allowed by server ({get_human_readable_timedelta(cls.max_full_res_query_temporal_span)})')

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            table_name = cls.get_table_or_view_name(stream_id, aggregation_level)
            select_columns_clause = ','.join(column_names)

            try:
                sql = f"""
                    SELECT {select_columns_clause}
                    FROM {table_name}
                    WHERE   {cls.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                        AND {cls.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                    """
                cur.execute(sql, {'from_dt': from_dt, 'to_dt': to_dt})
                results = cur.fetchall()
                results.reverse()  # timescale indexes in time-descending order, probably for a reason
            except psycopg2.errors.UndefinedTable as err:
                logging.error(f'Query failed with {err}: {sql}')
                raise RuntimeError(f'Table {table_name} is not present in db.  Files may not been ingested for this dataset.')
            except psycopg2.errors.UndefinedColumn as err:
                logging.error(f'Query failed due to mismatch between dataset definition and database schema: {err}')
                available_columns = list_db_table_columns(table_name)
                missing_columns = {f.name for f in cls.get_available_fields() if f.name.lower() not in available_columns}
                raise ValueError(f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
            except Exception as err:
                logging.error(f'query failed with {err}: {sql}')
                raise Exception

        return [cls._structure_results(fields, using_aggregations, result) for result in results]

    @classmethod
    def _get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()

    @classmethod
    def get_table_name(cls, stream_id: str) -> str:
        """Return the name of the SQL table storing the data for this dataset for a given stream"""
        return cls.get_table_or_view_name(stream_id, aggregation_depth=0)

    @classmethod
    def get_table_or_view_name(cls, stream_id: str, aggregation_depth: int) -> str:
        """
        Return the name of the SQL table or view providing access to data for this dataset for a given stream at a given
        aggregation level
        """
        if stream_id not in cls.stream_ids:
            raise ValueError(f'stream id "{stream_id}" not recognized (expected one of {sorted(cls.stream_ids)})')

        aggregation_depth_pad_width = 2
        padded_aggregation_depth = str(aggregation_depth).rjust(aggregation_depth_pad_width, "0")
        if len(padded_aggregation_depth) > aggregation_depth_pad_width:
            raise ValueError(f'aggregation_depth "{aggregation_depth}" exceeds maximum accounted for ({aggregation_depth_pad_width} digits)')

        aggregation_suffix = f'agg{padded_aggregation_depth}'

        table_base_name = f'{cls._get_table_name_prefix()}_{stream_id}'.lower()

        return table_base_name if aggregation_depth == 0 else f'{table_base_name}_{aggregation_suffix}'

    @classmethod
    def _validate_requested_fields(cls, requested_fields: Collection[TimeSeriesDatasetField], using_aggregations: bool) -> None:
        requested_fields = set(requested_fields)
        available_fields = {f for f in cls.get_available_fields() \
                                 if (f.has_aggregations  or f.name == cls.TIMESTAMP_COLUMN_NAME or not using_aggregations) and not f.is_constant}
        if not all([f in available_fields for f in requested_fields]):
            available_field_names = [f.name for f in available_fields]
            # requested fields which aren't available for selection
            unavailable_field_names = [f.name for f in requested_fields.difference(available_fields)]
            # requested fields which aren't available for selection due to lack of defined aggregations
            unavailable_aggregate_field_names = [f.name for f in requested_fields.difference(available_fields).difference(unavailable_field_names)] if using_aggregations else set()

            msg = f'Some requested fields {sorted(unavailable_field_names)} not present in available fields ({sorted(available_field_names)}).'

            if len(unavailable_aggregate_field_names) > 0:
                msg += f' The following fields are unavailable due to lack of defined aggregations: {sorted(unavailable_aggregate_field_names)}'

            raise ValueError(msg)

    @classmethod
    def _structure_results(cls, requested_fields: Collection[TimeSeriesDatasetField], using_aggregations: bool,  result: Dict) -> Dict:
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
    def get_sql_table_create_statement(cls, stream_id: str) -> str:
        # TODO: Perhaps generate this from column definitions rather than hardcoding per-class?  Need to think about it.
        """Get an SQL statement to create a table for this dataset/stream"""
        if stream_id not in cls.stream_ids:
            raise ValueError(f'stream_id {stream_id} not in {cls.__name__}.stream_ids - expected one of {cls.stream_ids}')

        sql = f"""
            create table public.{cls.get_table_name(stream_id)}
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
    def get_available_fields(cls) -> Set[TimeSeriesDatasetField]:
        return {TimeSeriesDatasetField(cls.TIMESTAMP_COLUMN_NAME)}.union(cls.get_reader().get_fields())

    @classmethod
    def get_required_aggregation_depth(cls) -> int:
        full_span_data_count = cls.max_data_span / cls.time_series_interval
        required_decimation_levels = math.ceil(math.log(full_span_data_count, cls.aggregation_step_factor))
        return required_decimation_levels

    @classmethod
    def get_aggregation_levels(cls) -> Sequence[int]:
        """
        Return the sorted levels (hierarchical level, not decimation factor) of aggregation which exist for this dataset
        """
        return [x for x in range(1, cls.get_required_aggregation_depth() + 1)]


    @classmethod
    def get_aggregation_interval(cls, aggregation_depth: int) -> timedelta:
        """
        For a given aggregation depth (hierarchical level, not decimation factor), return the duration of the bucket
        interval
        """
        return cls.time_series_interval * cls.aggregation_step_factor ** aggregation_depth
