import os
import threading

import psycopg2.pool
from contextlib import contextmanager

from retry import retry

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def get_conn_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """ Obtain a connection pool as a singleton"""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = _instantiate_conn_pool()
    return _pool

def _instantiate_conn_pool() -> psycopg2.pool.ThreadedConnectionPool:
    return psycopg2.pool.ThreadedConnectionPool(
        host=os.environ['TSDB_HOST'],
        port=int(os.environ['TSDB_PORT']),
        dbname=os.environ['TSDB_DATABASE'],
        user=os.environ['TSDB_USER'],
        password=os.environ['TSDB_PASSWORD'],
        minconn=4,  # arbitrarily chosen for the time being
        maxconn=32  # arbitrarily chosen for the time being
    )


def _is_transient_connection_error(exc: Exception) -> bool:
    """True only for server-side connection saturation — not auth/config errors."""
    if not isinstance(exc, psycopg2.OperationalError):
        return False
    msg = str(exc).lower()
    return "too many clients" in msg or "remaining connection slots" in msg


@retry(
    exceptions=psycopg2.OperationalError,
    tries=10,
    delay=0.5,
    max_delay=16,
    backoff=2,
    jitter=(0, 0.5),
)
def _getconn_with_backoff(pool):
    try:
        return pool.getconn()
    except psycopg2.OperationalError as e:
        if _is_transient_connection_error(e):
            raise  # let @retry catch and retry this one
        raise RuntimeError(f"Non-transient connection failure, not retrying: {e}") from e


def get_db_connection(without_db: bool = False):
    if without_db:
        host = os.environ['TSDB_HOST']
        port = int(os.environ['TSDB_PORT'])
        user = os.environ['TSDB_USER']
        password = os.environ['TSDB_PASSWORD']

        return psycopg2.connect(database=None, user=user, password=password, host=host, port=port)
    else:
        return _getconn_with_backoff(get_conn_pool())

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
    pool = get_conn_pool()
    conn = get_db_connection()
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
        pool.putconn(conn)
