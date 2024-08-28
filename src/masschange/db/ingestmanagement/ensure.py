from masschange.dataproducts.db.utils import get_db_connection
from masschange.db.ensure import log


def ensure_ingest_manager_tables_exist() -> None:
    """
    Ensure existence of ingest-manager tables used to track files which have been or have yet to be ingested.
    """
    table_name = '_ingestmanagement_targets'

    with get_db_connection() as conn, conn.cursor() as cur:
        sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
            id SERIAL PRIMARY KEY,
            src_filepath  VARCHAR NOT NULL,
            crawled_at DATE DEFAULT NULL,
            ingestion_started_at DATE DEFAULT NULL,
            ingestion_terminated_at INTERVAL DEFAULT NULL,
            ingestion_error_msg TEXT DEFAULT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_crawled_at_is_null
            ON {table_name} (crawled_at)
            WHERE crawled_at IS NULL;
            
            CREATE INDEX IF NOT EXISTS idx_ingestion_started_at_is_null
            ON {table_name} (ingestion_started_at)
            WHERE missing_value.ingestion_started_at IS NULL;
            
            CREATE INDEX IF NOT EXISTS idx_ingestion_error_msg_not_null
            ON {table_name} (ingestion_error_msg)
            WHERE ingestion_error_msg IS NOT NULL;
        """
        cur.execute(sql)
        conn.commit()
        log.info(f'Ensured presence of ingest manager tables!')
