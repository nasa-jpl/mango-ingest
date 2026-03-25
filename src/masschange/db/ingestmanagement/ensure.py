import logging

from masschange.db.conn import get_db_cursor
from masschange.db.constants.tablenames import INGEST_MANAGER_TABLE_NAME

log = logging.getLogger()


def ensure_ingest_manager_tables_exist() -> None:
    """
    Ensure existence of ingest-manager tables used to track files which have been or have yet to be ingested.
    """

    with get_db_cursor() as cur:
        id_seq_name = f'{INGEST_MANAGER_TABLE_NAME}_id_seq'
        sql = f"""
            -- sequence is necessary due to inability to apply SERIAL to a partitioned table
            CREATE SEQUENCE IF NOT EXISTS {id_seq_name};
        
            CREATE TABLE IF NOT EXISTS {INGEST_MANAGER_TABLE_NAME}
            (
            id INTEGER NOT NULL DEFAULT nextval('{id_seq_name}'),
            src_filepath  VARCHAR NOT NULL,
            staged_filepath  VARCHAR DEFAULT NULL,
            status  VARCHAR NOT NULL,
            product_id_str  VARCHAR NOT NULL, -- TODO: fkey this off _meta_dataproducts
            src_file_last_modified TIMESTAMP NOT NULL,
            crawled_at TIMESTAMP DEFAULT NOW(),
            rejected_at TIMESTAMP DEFAULT NULL,
            staged_at TIMESTAMP DEFAULT NULL,
            ingestion_started_at TIMESTAMP DEFAULT NULL,
            ingestion_terminated_at TIMESTAMP DEFAULT NULL,
            ingestion_error_msg TEXT DEFAULT NULL,
            
--             TODO: product_id_str is currently used as a proxy for the disambiguation string, though that assumes a 1:1 
--              relationship between reader and product.  If this results in a problem, an explicit column for the 
--              reader disambiguation string must be created and used instead
            UNIQUE (src_filepath, product_id_str, src_file_last_modified, status)
            ) PARTITION BY LIST (status);
            
            ALTER SEQUENCE _ingestmgr_crawled_files_id_seq
            OWNED BY _ingestmgr_crawled_files.id;
            
            -- partitions prevent active-job query performance from degrading as completed jobs pile up
            CREATE TABLE _ingestmgr_crawled_files_active
            PARTITION OF _ingestmgr_crawled_files
            FOR VALUES IN ('CRAWLED', 'STAGED', 'INGEST_STARTED', 'INGEST_TERMINATED', 'REJECTED');
        
            CREATE TABLE _ingestmgr_crawled_files_completed
            PARTITION OF _ingestmgr_crawled_files
            FOR VALUES IN ('INGEST_SUCCESS');
            
            -- sparse partial indices for common query patterns
            CREATE INDEX idx_ready_jobs
            ON _ingestmgr_crawled_files (id, product_id_str)
            WHERE status = 'STAGED';
            
            CREATE INDEX idx_inprogress_jobs
            ON _ingestmgr_crawled_files (id, product_id_str)
            WHERE status = 'INGEST_STARTED';
            
            CREATE INDEX idx_terminated_jobs
            ON _ingestmgr_crawled_files (id, product_id_str)
            WHERE status = 'INGEST_TERMINATED';
        """
        cur.execute(sql)
        log.info(f'Ensured presence of ingest manager tables!')
