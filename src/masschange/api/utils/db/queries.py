import logging

import psycopg2

from masschange.dataproducts.db.utils import get_db_connection


def fetch_bulk_metadata():
    """
    Normally, fetching dataset metadata requires one query per dataset, but that is unnecessarily slow if many
    datasets are desired.

    This optimized query returns metadata dicts for all datasets, keyed by dataset full-id

    """
    supported_properties = {'data_begin', 'data_end', 'last_updated'}

    with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            sql = f"""
SELECT mdp.name as product, mdv.name as version, mi.name as instrument, {','.join(sorted(supported_properties))}
FROM _meta_dataproducts_versions_instruments as mdpvi
JOIN _meta_dataproducts_versions mdv on mdv.id = mdpvi._meta_dataproducts_versions_id
JOIN _meta_dataproducts mdp on mdp.id = mdv._meta_dataproducts_id
JOIN _meta_instruments mi on mi.id = mdpvi._meta_instruments_id
                        """
            cur.execute(sql)
            results = cur.fetchall()
        except Exception as err:
            logging.warning(f'query failed with {err}: {sql}')
            return None
    return results