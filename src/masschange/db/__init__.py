import os

import psycopg2


def get_db_connection(without_db: bool = False):
    host = os.environ['TSDB_HOST']
    port = int(os.environ['TSDB_PORT'])
    user = os.environ['TSDB_USER']
    password = os.environ['TSDB_PASSWORD']
    database = os.environ['TSDB_DATABASE']

    return psycopg2.connect(database=None if without_db else database, user=user, password=password, host=host, port=port)
