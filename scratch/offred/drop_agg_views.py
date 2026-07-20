################################################################################################
#
# Script to drop all materialized aggregation views for a table (leaves the base table intact).
#
# In the tables with aggregation, a "Level N+1" aggregation depends on a "Level N" aggregation,
# which in turn depends on the base table.
# 'DROP" with 'CASCADE' keyword fails to resolve these multi-level relationships correctly,
# leading to "object still depends on" errors.
#
# As a way around, this script uses the base table name to find all aggregation views and drop them
# in reverse order (from the highest aggregation to the lowest) without dropping the base table.
#
# Usage:
# python3 usage: drop_agg_views.py [-h] table_name
#   positional arguments:
#       table_name  Name of the base table to drop views for, for example, gracefo_act1b_04_c
#
# Example:
#   python3 drop_agg_views.py gracefo_ddic_00_y
#
################################################################################################
import argparse
from typing import List
import os
from masschange.dataproducts.utils import resolve_dataset
from psycopg2 import connect, sql
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.datasetfactory import DatasetFactory


def get_view_names(table_name) -> List[str]:
    """
    Get lists of view names (if applicable) starting with the highest aggregation level,
    excluding the base table itself (level 0).

    Parameters
    ----------
    table_name name of the base table

    Returns
    -------
    List of view names, starting with the highest aggregation level
    """
    parts = table_name.split('_')
    instrument = parts[-1].upper()
    version = DatasetVersion(parts[-2])
    dataset_id = '_'.join(parts[:-2]).upper()
    product = resolve_dataset(dataset_id)
    dataset = DatasetFactory.create(product, version, instrument)

    if dataset.is_time_series_dataset():
        level_count = product.get_required_aggregation_level_count()
    else:
        level_count = 0

    names = []
    # Start at level_count and go down to 1.
    # Skipping 0 ensures we don't include the base table.
    for i in reversed(range(1, level_count + 1)):
        names.append(dataset.get_table_or_view_name(i))
    return names


def get_check_env(env_name: str) -> str:
    try:
        return os.environ[env_name]
    except KeyError:
        print(f"Error: The environment variable {env_name} is not set.")
        exit(1)


def drop_agg_views(table_name):
    host = get_check_env("TSDB_HOST")
    port = get_check_env("TSDB_PORT")
    user = get_check_env("TSDB_USER")
    password = get_check_env("TSDB_PASSWORD")
    dbname = get_check_env("TSDB_DATABASE")

    conn = None

    names = get_view_names(table_name)

    if not names:
        print(f"No aggregation views required or found for dataset related to {table_name}.")
        return

    try:
        conn = connect(
            dbname=dbname, user=user, password=password,
            host=host, port=port
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            for name in names:
                find_sql = """
                           SELECT table_schema, \
                                  table_name, \
                                  table_type
                           FROM information_schema.tables
                           WHERE table_name = %s; \
                           """

                cur.execute(find_sql, (f'{name}',))
                targets = cur.fetchall()
                if not targets:
                    print(f"No views found matching: {name}")
                    continue

                schema, name, obj_type = targets[0]

                # Safeguard: Ensure we are only dropping views
                if "VIEW" not in obj_type:
                    print(f"Skipping {name} because it is a {obj_type} (not a materialized view).")
                    continue

                # Use sql.Identifier to safely handle schema and table names
                drop_query = sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {}.{} CASCADE").format(
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


def get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog='MassChange Data Ingester',
        description='Drop associated materialized views for a given base table.'
    )
    ap.add_argument('table_name',
                    help='name of the base table whose views should be dropped')
    return ap.parse_args()


if __name__ == '__main__':
    args = get_args()
    drop_agg_views(args.table_name)