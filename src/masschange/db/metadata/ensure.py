import logging

from masschange.db.conn import get_db_cursor

log = logging.getLogger()

def ensure_metadata_tables_exist(db_name: str) -> None:
    """
    Ensure existence of metadata tables used to track inherent dataset properties (such as column definitions), as well
    as mutable properties like extant data span and extant dataset versions.
    """

    with get_db_cursor() as cur:
        sql = f"""
            CREATE TABLE IF NOT EXISTS _meta_dataproducts
            (
            id SERIAL PRIMARY KEY,
            name  VARCHAR UNIQUE,
            label VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _meta_instruments
            (
            id SERIAL PRIMARY KEY ,
            name  VARCHAR UNIQUE ,
            label VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _meta_dataproducts_versions
            (
            id SERIAL PRIMARY KEY,
            _meta_dataproducts_id INT REFERENCES _meta_dataproducts (id) ON DELETE CASCADE,
            name  VARCHAR NOT NULL,
            label VARCHAR NOT NULL,
            UNIQUE (_meta_dataproducts_id, name) 
            );

            CREATE TABLE IF NOT EXISTS _meta_dataproducts_versions_instruments
            (
            _meta_dataproducts_versions_id INT REFERENCES _meta_dataproducts_versions (id) ON DELETE CASCADE,
            _meta_instruments_id INT REFERENCES _meta_instruments (id) ON DELETE CASCADE,
            data_begin TIMESTAMPTZ,
            data_end TIMESTAMPTZ,
            last_updated TIMESTAMPTZ,
            PRIMARY KEY (_meta_dataproducts_versions_id, _meta_instruments_id)
            );
        """
        cur.execute(sql)
        log.info(f'Ensured presence of dataset metadata tables!')
