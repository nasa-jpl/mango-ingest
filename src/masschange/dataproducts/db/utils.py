import os
from datetime import datetime
from typing import AbstractSet, List, Dict, Set

import psycopg2
from psycopg2.sql import Composed, SQL, Identifier

from masschange.api.utils.misc import KeyValueQueryParameter
from masschange.db.utils import get_db_connection as _get_db_connection


def get_db_connection(without_db: bool = False):
    database = None if without_db else os.environ['TSDB_DATABASE']
    return _get_db_connection(database)


def list_table_columns(table_name: str) -> Set[str]:
    with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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


def prepare_where_clause_conditions(timestamp_column_name: str, filters: List[KeyValueQueryParameter]) -> List[
    Composed]:
    """Given a start/end datetime and a set of filter conditions, prepare conditions for use in SQL WHERE"""
    conditions = [
                     SQL('{} >= %(from_dt)s').format(Identifier(timestamp_column_name)),
                     SQL('{} <= %(to_dt)s').format(Identifier(timestamp_column_name))
                 ] + [SQL(f'{{}}=%(filter_{i})s').format(Identifier(f.key)) for i, f in enumerate(sorted(filters))]

    return conditions


def prepare_where_clause_parameters(from_dt: datetime, to_dt: datetime, filters: List[KeyValueQueryParameter]) -> Dict:
    """Given a start/end datetime and a set of filter conditions, prepare parameters for use in SQL parametrized query"""
    parameters = {'from_dt': from_dt, 'to_dt': to_dt}
    filter_parameters = {f'filter_{i}': f.value for i, f in enumerate(sorted(filters))}
    parameters.update(filter_parameters)

    return parameters
