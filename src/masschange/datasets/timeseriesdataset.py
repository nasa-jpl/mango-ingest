import logging
from abc import ABC
from collections.abc import Collection
from datetime import datetime
from typing import List, Dict, Set, Type

import psycopg2
from psycopg2 import extras

from masschange.db import get_db_connection
from masschange.missions import Mission

log = logging.getLogger()


class TimeSeriesDataset(ABC):
    mission: Type[Mission]
    id_suffix: str
    stream_ids: Set[str]
    available_fields: Set[str]

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
            'id': cls.get_full_id(),
            'streams': [{'id': id, 'data_begin': cls.get_data_begin(id), 'data_end': cls.get_data_end(id)} for id in sorted(cls.stream_ids)],
            'available_fields': sorted(cls.available_fields),
            'timestamp_field': cls.TIMESTAMP_COLUMN_NAME
        }

    @classmethod
    def _get_data_span_stat(cls, agg: str, stream_id: str) -> datetime:
        """"""
        if agg not in {'min', 'max'}:
            raise ValueError(f'"{agg}" is not a supported timespan stat')

        with get_db_connection() as conn:
            table_name = cls._get_table_name(stream_id)

            try:
                sql = f"""
                    SELECT {agg}({cls.TIMESTAMP_COLUMN_NAME})
                    FROM {table_name}
                    """
                cur = conn.cursor()
                cur.execute(sql)
                result = cur.fetchone()[0]
            except Exception as err:
                logging.info(f'query failed with {err}: {sql}')

        return result
    @classmethod
    def get_data_begin(cls, stream_id: str) -> datetime:
        return cls._get_data_span_stat('min', stream_id)

    @classmethod
    def get_data_end(cls, stream_id: str) -> datetime:
        return cls._get_data_span_stat('max', stream_id)


    @classmethod
    def select(cls, stream_id: str, from_dt: datetime, to_dt: datetime,
               filter_to_fields: Collection[str] = None) -> List[Dict]:
        requested_fields = filter_to_fields or cls.available_fields
        cls._validate_requested_fields(requested_fields)

        with get_db_connection() as conn:
            table_name = cls._get_table_name(stream_id)
            select_columns_clause = ','.join(requested_fields)

            try:
                sql = f"""
                    SELECT {select_columns_clause}
                    FROM {table_name}
                    WHERE   {cls.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s
                        AND {cls.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s
                    """
                cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
                cur.execute(sql, {'from_dt': from_dt, 'to_dt': to_dt})
                results = cur.fetchall()
                results.reverse()  # timescale indexes in time-descending order, probably for a reason
            except Exception as err:
                logging.info(f'query failed with {err}: {sql}')

        return results

    @classmethod
    def _get_table_name_prefix(cls) -> str:
        return cls.get_full_id().lower()

    @classmethod
    def _get_table_name(cls, stream_id: str) -> str:
        if stream_id not in cls.stream_ids:
            raise ValueError(f'stream id "{stream_id}" not recognized (expected one of {sorted(cls.stream_ids)})')

        return f'{cls._get_table_name_prefix()}_{stream_id}'.lower()

    @classmethod
    def _validate_requested_fields(cls, requested_fields: Collection[str]) -> None:
        requested_fields = set(requested_fields)
        if not all([f in cls.available_fields for f in requested_fields]):
            unavailable_fields = requested_fields.difference(cls.available_fields)
            raise ValueError(
                f'Some requested fields {sorted(unavailable_fields)} not present in available fields ({sorted(cls.available_fields)})')
