from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import psycopg2
from fastapi import APIRouter, Query

from masschange.db.conn import get_db_cursor
from masschange.ingest.manager.filestatus import FileStatus

router = APIRouter(prefix="/files", tags=["files"])


@router.get("/", response_model=List[Dict[str, Any]])
def list_files(
        id: Optional[int] = Query(None),
        status: Optional[FileStatus] = Query(None),
        product_id: Optional[str] = Query(None),
        limit: Optional[int] = Query(100, gt=0, le=100000, description="Limit the number of results returned")
):
    """
    View all rows with optional filters
    :param id:
    :param status:
    :param product_id:
    :param limit:
    :return:
    """

    # TODO: reimplement date filters iff use-case exists
    #  had it implemented previously but lost during conflict resolution - edunn 20250912
    query = "SELECT * FROM _ingestmgr_crawled_files WHERE TRUE"
    params = []

    if id is not None:
        query += " AND id = %s"
        params.append(id)
    if status is not None:
        query += " AND status = %s"
        params.append(status)
    if product_id is not None:
        query += " AND product_id_str = %s"
        params.append(product_id)

    query += f" ORDER BY id DESC LIMIT {limit}"

    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, tuple(params))
        results = list(cur.fetchall())

        # tweak output format slightly
        for result in results:
            result['product_id'] = result.pop('product_id_str')

        return results


@router.get("/status_counts")
def status_counts():
    """
    Counts of db rows by status grouped by product_id_str
    :return:
    """
    query = """
            SELECT product_id_str, status, COUNT(*) AS count
            FROM _ingestmgr_crawled_files
            GROUP BY product_id_str, status
            ORDER BY product_id_str, status
            """
    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        result = {"products": {}}
        products = result["products"]
        for row in rows:
            id_str = row["product_id_str"]
            status = row["status"]
            count = row["count"]
            if id_str not in products:
                products[id_str] = {"id": id_str, "statuses": {status_enum.value: 0 for status_enum in FileStatus}}
            statuses = products[id_str]["statuses"]
            statuses[status] = count
        return result


@router.get("/recent_errors")
def recent_errors():
    """
    Return details and counts of distinct errors encountered in the past 24 hours
    :return:
    """
    # TODO: implement lookback as qparam "since" if usecase exists - edunn 20250912
    lookback_duration = timedelta(hours=24)
    since = datetime.now() - lookback_duration
    query = """
            SELECT ingestion_error_msg as error, count(*) as count
            FROM _ingestmgr_crawled_files
            WHERE
              status = 'INGEST_TERMINATED'
              AND ingestion_terminated_at >= %(since)s
              AND ingestion_error_msg IS NOT NULL 
            GROUP BY ingestion_error_msg
            """
    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, {'since': since.isoformat()})
        results = cur.fetchall()
        return {"since": since.isoformat(), "errors": results}
