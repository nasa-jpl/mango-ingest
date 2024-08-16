import os
from typing import Union

import psycopg2


def get_db_connection(database: Union[str, None]):
    host = os.environ['TSDB_HOST']
    port = int(os.environ['TSDB_PORT'])
    user = os.environ['TSDB_USER']
    password = os.environ['TSDB_PASSWORD']

    return psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
