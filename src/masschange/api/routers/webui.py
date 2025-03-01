import json

import psycopg2
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from masschange.db.conn import get_db_cursor

router = APIRouter()


# Pydantic model for request body
class DataModel(BaseModel):
    data: dict


@router.post("/store/{key}", tags=['json-store'])
def store_data(key: str, data_model: DataModel):
    """Stores a JSON blob with a unique key in the database."""
    with get_db_cursor(autocommit=True) as cur:
        try:
            cur.execute(
                """
                INSERT INTO _jsonstore (id, content)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content
                """,
                (key, json.dumps(data_model.data)),
            )
            return {"message": "Data stored successfully"}

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/fetch/{key}", tags=['json-store'])
def fetch_data(key: str):
    """Fetches a JSON blob from the database by its unique key."""
    with get_db_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        try:
            cur.execute("SELECT content FROM _jsonstore WHERE id = %s", (key,))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Key not found")
            return {"data": row['content']}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
