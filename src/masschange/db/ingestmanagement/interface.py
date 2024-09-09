"""
Provides a managed interface to the catalog db table
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Union

from masschange.db.conn import get_db_cursor
from masschange.db.constants.tablenames import INGEST_MANAGER_TABLE_NAME


class Catalog:
    @staticmethod
    def add_target(filepath: Path, file_last_modified: datetime):
        sql = f"""
            INSERT INTO {INGEST_MANAGER_TABLE_NAME}
            VALUES (DEFAULT, %(filepath)s, %(timestamp)s, None, None, None)
        """

        parameters = {
            'filepath': filepath,
            'file_last_modified': file_last_modified,  # datetime.fromtimestamp(os.stat(filepath).st_ctime)
            'timestamp': datetime.now
        }

        with get_db_cursor() as cur:
            cur.execute(sql, parameters)

    @staticmethod
    def set_ingestion_started(filepath: Path):
        sql = f"""
            UPDATE {INGEST_MANAGER_TABLE_NAME}
            VALUES (DEFAULT, %(filepath)s, %(timestamp)s, None, None, None)
        """

        parameters = {
            'filepath': filepath,
            'timestamp': datetime.now
        }

        with get_db_cursor() as cur:
            cur.execute(sql, parameters)
