from collections.abc import Set
import psycopg2

from masschange.db import get_db_connection


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