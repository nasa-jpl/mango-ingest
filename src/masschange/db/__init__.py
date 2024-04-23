import os

import psycopg2
from sqlalchemy import create_engine


def get_db_connection(without_db: bool = False):
    host = os.environ['TSDB_HOST']
    port = int(os.environ['TSDB_PORT'])
    user = os.environ['TSDB_USER']
    password = os.environ['TSDB_PASSWORD']
    database = os.environ['TSDB_DATABASE']

    return psycopg2.connect(database=None if without_db else database, user=user, password=password, host=host, port=port)

def get_sqlalchemy_engine(without_db: bool = False):
    host = os.environ['TSDB_HOST']
    port = int(os.environ['TSDB_PORT'])
    user = os.environ['TSDB_USER']
    password = os.environ['TSDB_PASSWORD']
    database = os.environ['TSDB_DATABASE']

    # URL: dialect+driver://username:password@host:port/database
    url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    return create_engine(url)