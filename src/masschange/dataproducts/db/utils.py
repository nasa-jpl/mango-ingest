import os
from datetime import datetime
from typing import List, Dict, Set, Union

import psycopg2
from psycopg2.sql import Composed, SQL, Identifier

from masschange.api.utils.misc import KeyValueFilterSet
from masschange.db.conn import get_db_cursor


def list_table_columns(table_name: str) -> Set[str]:
    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            sql = f"""
                       SELECT *
                       FROM {table_name}
                       LIMIT 1
                       """
            cur.execute(sql)
            results = cur.fetchone()
            return set(results.keys())
        except psycopg2.errors.UndefinedTable:
            raise ValueError(f'Table "{table_name}" does not exist in db')


def prepare_where_clause_conditions(timestamp_column_name: str, filters: KeyValueFilterSet) -> List[
    Composed]:
    """
    Given a start/end datetime and a set of filter conditions, prepare conditions for use in SQL WHERE
    Multiple values provided for the same key will be combined with OR (e.g. "key in (value1, value2, value3)")
    """
    conditions = [
                     SQL('{} >= %(from_dt)s').format(Identifier(timestamp_column_name)),
                     SQL('{} <= %(to_dt)s').format(Identifier(timestamp_column_name))
                 ] + [SQL(f'{{}} in %(filter_{filter_key})s').format(Identifier(filter_key)) for filter_key in filters.as_dict()]

    return conditions


def prepare_where_clause_parameters(from_dt: datetime, to_dt: datetime, filters: KeyValueFilterSet) -> Dict:
    """Given a start/end datetime and a set of filter conditions, prepare parameters for use in SQL parametrized query"""
    parameters: dict[str, Union[datetime, str, tuple]] = {'from_dt': from_dt, 'to_dt': to_dt}
    filter_parameters = {f'filter_{k}': tuple(v) for k, v in filters.as_dict().items()}
    parameters.update(filter_parameters)

    return parameters
