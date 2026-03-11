import os
from datetime import datetime
from pathlib import Path
from typing import Union, Optional

import psycopg2
from psycopg2 import extras

from masschange.dataproducts.dataproduct import DataProduct
from masschange.db.conn import get_db_cursor
from masschange.db.constants.tablenames import INGEST_MANAGER_TABLE_NAME
from masschange.db.ingestmanagement.ensure import ensure_ingest_manager_tables_exist
from masschange.ingest.manager.fileingestrecord import FileIngestRecord
from masschange.ingest.manager.filestatus import FileStatus


class IngestManager:

    def __init__(self):
        ensure_ingest_manager_tables_exist()

    def register(self, filepath: Path, product: DataProduct) -> FileIngestRecord:
        # TODO: integrate product attribute (currently not included in

        file_last_modified = datetime.fromtimestamp(
            os.stat(filepath).st_ctime)  # TODO: double-check that this behaves as expected

        sql = f"""
              INSERT INTO {INGEST_MANAGER_TABLE_NAME} (id, src_filepath, product_id_str, status, src_file_last_modified)
              VALUES (DEFAULT, %(filepath)s, %(product_full_id_str)s, %(status)s, %(last_modified)s)
              ON CONFLICT (src_filepath, product_id_str, src_file_last_modified) DO UPDATE SET status = excluded.status, crawled_at = excluded.crawled_at
              RETURNING *
              """

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor, autocommit=True) as cur:
            try:
                cur.execute(sql, {'filepath': str(filepath), 'product_full_id_str': product.get_full_id(),
                                  'status': str(FileStatus.CRAWLED), 'last_modified': file_last_modified})
                result = cur.fetchone()
                return FileIngestRecord.from_postgres_dict(result)

            except Exception as e:
                raise RuntimeError(f'Registration of {filepath} with ingest manager failed with {e.__class__}:{e}')

    def set_staged(self, record: FileIngestRecord, staged_path: Union[Path, str]) -> FileIngestRecord:
        # TODO: rework to leverage IngestManager.set_status() - edunn 20260311

        status = FileStatus.STAGED
        sql = f"""
                      UPDATE {INGEST_MANAGER_TABLE_NAME}
                      SET status = %(status)s, staged_filepath = %(staged_filepath)s, {status.db_column_name} = NOW()
                      WHERE id = %(id)s
                      RETURNING *
                      """

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor, autocommit=True) as cur:
            try:
                cur.execute(sql, {'id': record.id, 'status': str(status), 'staged_filepath': staged_path})
                result = cur.fetchone()
                return FileIngestRecord.from_postgres_dict(result)

            except Exception as e:
                raise RuntimeError(f'Updating record id "{record.id}" to status {status} failed with "{e}"')

    @staticmethod
    def set_terminated(record: FileIngestRecord, success: bool, err_msg: Optional[str] = None) -> FileIngestRecord:
        # TODO: rework to leverage IngestManager.set_status() - edunn 20260311

        status = FileStatus.INGEST_SUCCESS if success else FileStatus.INGEST_TERMINATED
        sql = f"""
                      UPDATE {INGEST_MANAGER_TABLE_NAME}
                      SET status = '{status}', ingestion_terminated_at = NOW(), ingestion_error_msg = %(err_msg)s
                      WHERE id = %(id)s
                      RETURNING *
                      """

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor, autocommit=True) as cur:
            try:
                cur.execute(sql, {'id': record.id, 'err_msg': err_msg})
                result = cur.fetchone()
                return FileIngestRecord.from_postgres_dict(result)

            except Exception as e:
                raise RuntimeError(f'Updating record id "{record.id}" to status {status} failed with {e.__class__}: {e}')

    def set_status(self, record: FileIngestRecord, status: FileStatus) -> FileIngestRecord:
        """
        Update the status of the row corresponding to the given file ingest record (by id).
        :param record:
        :param status:
        :return: the up-to-date file ingest record
        """

        sql = f"""
              UPDATE {INGEST_MANAGER_TABLE_NAME}
              SET status = %(status)s, {status.db_column_name} = NOW()
              WHERE id = %(id)s
              RETURNING *
              """

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor, autocommit=True) as cur:
            try:
                cur.execute(sql, {'id': record.id, 'status': str(status)})
                # TODO: sanity check that exactly one result was changed
                result = cur.fetchone()
                return FileIngestRecord.from_postgres_dict(result)

            except Exception as e:
                raise RuntimeError(f'Updating record id "{record.id}" to status {status} failed with "{e}"')


    @staticmethod
    def fetch_next_valid_job() -> Union[FileIngestRecord, None]:
        sql = f"""
            WITH successfully_locked_valid_job_rows AS (
                SELECT *
                FROM {INGEST_MANAGER_TABLE_NAME}
                WHERE status = 'STAGED'
                    AND src_filepath NOT IN (
        --             The set of all src_filepaths with a job currently ingesting
                        SELECT DISTINCT src_filepath
                        FROM {INGEST_MANAGER_TABLE_NAME}
                        WHERE status = '{FileStatus.INGEST_STARTED}'
                    )
                LIMIT 1
                FOR UPDATE
            )
        
            UPDATE {INGEST_MANAGER_TABLE_NAME}
            SET status = '{FileStatus.INGEST_STARTED}', ingestion_started_at = NOW()
            FROM successfully_locked_valid_job_rows
            WHERE {INGEST_MANAGER_TABLE_NAME}.id = successfully_locked_valid_job_rows.id
            RETURNING *
        """

        with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor, autocommit=True) as cur:
            cur.execute(sql)
            result = cur.fetchone()
            return None if result is None else FileIngestRecord.from_postgres_dict(result)
