import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Collection
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
    max_query_temporal_span: timedelta = timedelta(
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
            'available_fields': sorted([f.name for f in cls.get_available_fields()]),
            'timestamp_field': cls.TIMESTAMP_COLUMN_NAME,
            'query_span_max_seconds': cls.max_query_temporal_span.total_seconds()
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
               filter_to_fields: Collection[str] = None, limit_data_span: bool = True) -> List[Dict]:
        requested_field_names = filter_to_fields or [f.name for f in cls.get_available_fields() if not f.is_constant]
        cls._validate_requested_fields(requested_field_names)

        requested_temporal_span = to_dt - from_dt
        if limit_data_span and requested_temporal_span > cls.max_query_temporal_span:
            raise TooMuchDataRequestedError(f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} exceeds maximum allowed by server ({get_human_readable_timedelta(cls.max_query_temporal_span)})')

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            table_name = cls.get_table_name(stream_id)
            select_columns_clause = ','.join(requested_field_names)

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

        return results

    @classmethod
    def _get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()

    @classmethod
    def get_table_name(cls, stream_id: str, aggregation_depth: int = 0) -> str:
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
    def _validate_requested_fields(cls, requested_field_names: Collection[str]) -> None:
        requested_field_names = set(requested_field_names)
        available_field_names = {f.name for f in cls.get_available_fields()}
        if not all([f in available_field_names for f in requested_field_names]):
            unavailable_field_names = requested_field_names.difference(available_field_names)
            raise ValueError(
                f'Some requested fields {sorted(unavailable_field_names)} not present in available fields ({sorted(available_field_names)})')

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
