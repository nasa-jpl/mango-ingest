"""
Defines static database table names for use throughout project
"""
# TODO: remove remaining hardcoded references to this table name throughout codebase
INGEST_MANAGER_TABLE_NAME = '_ingestmgr_crawled_files'
INGEST_MANAGER_ACTIVE_PARTITION_TABLE_NAME = f'{INGEST_MANAGER_TABLE_NAME}_active'
INGEST_MANAGER_COMPLETED_PARTITION_TABLE_NAME = f'{INGEST_MANAGER_TABLE_NAME}_completed'