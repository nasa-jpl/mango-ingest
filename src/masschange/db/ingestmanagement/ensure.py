import logging

from masschange.db.conn import get_db_cursor
from masschange.db.constants.tablenames import INGEST_MANAGER_TABLE_NAME

log = logging.getLogger()


def ensure_ingest_manager_tables_exist() -> None:
    """
    Ensure existence of ingest-manager tables used to track files which have been or have yet to be ingested.
    """

    with get_db_cursor() as cur:
        sql = f"""
            CREATE TABLE IF NOT EXISTS {INGEST_MANAGER_TABLE_NAME}
            (
            id SERIAL PRIMARY KEY,
            src_filepath  VARCHAR NOT NULL,
            staged_filepath  VARCHAR DEFAULT NULL,
            status  VARCHAR NOT NULL,
            product_id_str  VARCHAR NOT NULL, -- TODO: fkey this off _meta_dataproducts
            src_file_last_modified TIMESTAMP NOT NULL,
            crawled_at TIMESTAMP DEFAULT NOW(),
            staged_at TIMESTAMP DEFAULT NULL,
            ingestion_started_at TIMESTAMP DEFAULT NULL,
            ingestion_terminated_at TIMESTAMP DEFAULT NULL,
            ingestion_error_msg TEXT DEFAULT NULL,
            
--             TODO: product_id_str is currently used as a proxy for the disambiguation string, though that assumes a 1:1 
--              relationship between reader and product.  If this results in a problem, an explicit column for the 
--              reader disambiguation string must be created and used instead
            UNIQUE (src_filepath, product_id_str, src_file_last_modified)
            );
            
            
            CREATE INDEX IF NOT EXISTS idx_crawled_at_is_null
            ON {INGEST_MANAGER_TABLE_NAME} (crawled_at)
            WHERE crawled_at IS NULL;
                        
            CREATE INDEX IF NOT EXISTS idx_staged_at_is_null
            ON {INGEST_MANAGER_TABLE_NAME} (staged_at)
            WHERE staged_at IS NULL;
            
            CREATE INDEX IF NOT EXISTS idx_ingestion_started_at_is_null
            ON {INGEST_MANAGER_TABLE_NAME} (ingestion_started_at)
            WHERE ingestion_started_at IS NULL;
            
            CREATE INDEX IF NOT EXISTS idx_ingestion_terminated_at_is_null
            ON {INGEST_MANAGER_TABLE_NAME} (ingestion_started_at)
            WHERE ingestion_terminated_at IS NULL;
            
            CREATE INDEX IF NOT EXISTS idx_ingestion_error_msg_not_null
            ON {INGEST_MANAGER_TABLE_NAME} (ingestion_error_msg)
            WHERE ingestion_error_msg IS NOT NULL;
        """
        cur.execute(sql)
        log.info(f'Ensured presence of ingest manager tables!')
