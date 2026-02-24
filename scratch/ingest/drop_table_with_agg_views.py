################################################################################################
#
# Script to drop a table with all materialized aggregation views.
#
# In the tables with aggregation, a "Level N+1" aggregation depends on a "Level N" aggregation,
# which in turn depends on the base table.
# 'DROP" with 'CASCADE' keyword fails to resolve these multi-level relationships correctly,
# leading to "object still depends on" errors.
#
# As a way around, this script uses the table name to find all aggregation views and drop them
# in reverse order (from the highest to the lowest aggregation) before dropping the table itself.
#
# Usage:
# python3 usage: drop_table_with_agg_views.py [-h] table_name
#   positional arguments:
#       table_name  Name of the table to drop, for example, gracefo_act1b_04_c
#
# Example:
#   python3 drop_table_with_agg_views.py gracefo_ddic_00_y
#
################################################################################################
import psycopg2
import argparse
import os
from psycopg2 import sql

def get_check_env(env_name: str) -> str:
    try:
        return os.environ[env_name]
    except KeyError:
        print(f"Error: The environment variable {env_name} is not set.")
        exit(1)

def drop_table_with_agg_views(target_substring):
    host = get_check_env("TSDB_HOST")
    port = get_check_env("TSDB_PORT")
    user = get_check_env("TSDB_USER")
    password = get_check_env("TSDB_PASSWORD")
    dbname = get_check_env("TSDB_DATABASE")

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password,
            host=host, port=port
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            # 1. Query the information_schema to find tables/views matching the name
            find_sql = """
                       SELECT table_schema, \
                              table_name, \
                              table_type
                       FROM information_schema.tables
                       WHERE table_name LIKE %s
                       ORDER BY table_name DESC; \
                       """

            cur.execute(find_sql, (f'%{target_substring}%',))
            targets = cur.fetchall()

            if not targets:
                print(f"No tables or views found matching: {target_substring}")
                return

            # 2. Iterate through results and drop each with CASCADE
            for schema, name, obj_type in targets:
                # Determine the correct SQL keyword
                prefix = "MATERIALIZED VIEW" if "VIEW" in obj_type else "TABLE"

                # Use sql.Identifier to safely handle schema and table names
                drop_query = sql.SQL("DROP {} IF EXISTS {}.{} CASCADE").format(
                    sql.SQL(prefix),
                    sql.Identifier(schema),
                    sql.Identifier(name)
                )

                print(f"Executing: {drop_query.as_string(conn)}")
                cur.execute(drop_query)
                print(f"Successfully dropped {name} ({obj_type}).")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=f"Drop a table with ll associated views. "
                                                 f"Before run, please set the following  env variables: "
                                                 "'TSDB_HOST', "
                                                 "'TSDB_PORT', "
                                                 "'TSDB_USER', "
                                                 "'TSDB_PASSWORD', "
                                                 "'TSDB_DATABASE'")

    parser.add_argument("table_name", help="Name of the table to drop, for example, gracefo_act1b_04_c")
    args = parser.parse_args()
    drop_table_with_agg_views(args.table_name)


