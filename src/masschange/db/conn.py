import os

import psycopg2.pool
from contextlib import contextmanager

default_database = os.environ['TSDB_DATABASE']
dbpool = psycopg2.pool.ThreadedConnectionPool(
    host=os.environ['TSDB_HOST'],
    port=int(os.environ['TSDB_PORT']),
    dbname=default_database,
    user=os.environ['TSDB_USER'],
    password=os.environ['TSDB_PASSWORD'],
    minconn=4,  # arbitrarily chosen for the time being
    maxconn=32  # arbitrarily chosen for the time being
)


# def get_db_connection(without_db: bool = False):
#     if without_db:
#         host = os.environ['TSDB_HOST']
#         port = int(os.environ['TSDB_PORT'])
#         user = os.environ['TSDB_USER']
#         password = os.environ['TSDB_PASSWORD']
#
#         return psycopg2.connect(database=None, user=user, password=password, host=host, port=port)
#     else:
#         return dbpool.getconn()


@contextmanager
def get_db_cursor(autocommit: bool = False, **kwargs):
    """
    Get a context-managed db cursor which commits when the (with) context is closed.  This is (confusingly) not the same
    as setting the associated connection's autocommit=True, which is required for operations which are not supported in
    a Transaction, and is therefore supported via the autocommit parameter.
    :param autocommit: Whether to set conn.autocommit = True
    :param kwargs: kwargs to be passed through to the cursor constructor
    :return:
    """
    conn = dbpool.getconn()
    try:
        with conn.cursor(**kwargs) as cur:
            if autocommit:
                conn.autocommit = True

            yield cur

            if not autocommit:
                conn.commit()
    # You can have multiple exception types here.
    # For example, if you wanted to specifically check for the
    # 23503 "FOREIGN KEY VIOLATION" error type, you could do:
    # except psycopg2.Error as e:
    #     conn.rollback()
    #     if e.pgcode = '23503':
    #         raise KeyError(e.diag.message_primary)
    #     else
    #         raise Exception(e.pgcode)
    except Exception:
        conn.rollback()
        raise

    finally:
        dbpool.putconn(conn)
