from datetime import datetime
from typing import Union

from masschange.datasets.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.datasets.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.db import get_db_connection
from masschange.utils.timespan import TimeSpan


def update_metadata(dataset: TimeSeriesDataProduct, dataset_version: TimeSeriesDatasetVersion, stream_id: str,
                    data_span: Union[TimeSpan, None] = None, populate_versions = False):
    if populate_versions:
        raise NotImplementedError(f'update_metadata() does not yet support populate_versions - go ahead and implement population of queries from available table names')

    with get_db_connection() as conn, conn.cursor() as cur:

        sql = """
            INSERT INTO _meta_dataproducts
            VALUES (DEFAULT, %(name)s, %(label)s)
            ON CONFLICT DO NOTHING;
            """
        cur.execute(sql, {'name': dataset.get_full_id(), 'label': dataset.get_full_id()})
        conn.commit()

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = """
            INSERT INTO _meta_instruments
            VALUES (DEFAULT, %(name)s, %(label)s)
            ON CONFLICT DO NOTHING;
            """
        cur.execute(sql, {'name': stream_id, 'label': stream_id})
        conn.commit()

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = """
            SELECT id
            FROM _meta_dataproducts
            WHERE name = %(name)s;
        """
        cur.execute(sql, {'name': dataset.get_full_id()})
        data_product_db_id = cur.fetchone()[0]

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = """
            SELECT id
            FROM _meta_instruments
            WHERE name = %(name)s;
        """
        cur.execute(sql, {'name': stream_id})
        instrument_db_id = cur.fetchone()[0]

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = """
            INSERT INTO _meta_dataproducts_versions
            VALUES (DEFAULT, %(dataproduct)s, %(name)s, %(label)s)
            ON CONFLICT DO NOTHING;
            """
        cur.execute(sql, {'dataproduct': data_product_db_id, 'name': str(dataset_version), 'label': str(dataset_version)})

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = """
            SELECT id
            FROM _meta_dataproducts_versions
            WHERE _meta_dataproducts_id= %(dataproduct)s AND name = %(name)s;
        """
        cur.execute(sql, {'dataproduct': data_product_db_id, 'name': str(dataset_version)})
        dataproducts_versions_id = cur.fetchone()[0]

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = """
            INSERT INTO _meta_dataproducts_versions_instruments
            (_meta_dataproducts_versions_id, _meta_instruments_id)
            VALUES (%(dataproduct_version)s, %(instrument)s)
            ON CONFLICT DO NOTHING;
        """
        cur.execute(sql,
                    {'dataproduct_version': dataproducts_versions_id, 'instrument': instrument_db_id})
        conn.commit()

    if data_span is not None:
        with get_db_connection() as conn, conn.cursor() as cur:
            sql = """
                UPDATE _meta_dataproducts_versions_instruments
                SET data_begin = %(data_begin)s, data_end = %(data_end)s, last_updated = %(last_updated)s
                WHERE _meta_dataproducts_versions_id = %(dataproduct)s AND _meta_instruments_id = %(instrument)s;
            """
            cur.execute(sql, {'dataproduct': data_product_db_id, 'instrument': instrument_db_id, 'data_begin': data_span.begin, 'data_end': data_span.end, 'last_updated': datetime.now()})
            conn.commit()