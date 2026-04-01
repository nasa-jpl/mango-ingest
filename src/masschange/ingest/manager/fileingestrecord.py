# Yes, this is a job for an ORM like SQLAlchemy, but a fix-up later is preferable to delaying implementation of the
# ingestion manager even longer to incorporate one - alexdunnjpl 20250508
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.utils import resolve_dataproduct


class FileIngestRecord:
    def __init__(
            self,
            id: int,
            product: DataProduct,
            src_filepath: str,
            staged_filepath: Union[str, Path, None],
            crawled_at: datetime,
            staged_at: Optional[datetime] = None,
            ingest_started_at: Optional[datetime] = None,
            ingest_terminated_at: Optional[datetime] = None,
            ingestion_succeeded_at: Optional[datetime] = None,
            ingestion_error_message: Optional[str] = None,
    ):
        self.id = id
        self.product = product
        self.src_filepath = src_filepath
        self.staged_filepath: Path = None if staged_filepath is None else Path(staged_filepath)
        self.crawled_at = crawled_at
        self.staged_at = staged_at
        self.ingest_started_at = ingest_started_at
        self.ingest_terminated_at = ingest_terminated_at
        self.ingestion_succeeded_at = ingestion_succeeded_at
        self.ingestion_error_message = ingestion_error_message

    def to_postgres_dict(self) -> dict:
        """Convert the object into a dictionary suitable for psycopg2 INSERT/UPDATE."""
        return {
            "id": self.id,
            "product_id_str": self.product.get_full_id(),
            "src_filepath": self.src_filepath,
            "staged_filepath": None if self.staged_filepath is None else str(self.staged_filepath),
            "crawled_at": self.crawled_at,
            "staged_at": self.staged_at,
            "ingest_started_at": self.ingest_started_at,
            "ingest_terminated_at": self.ingest_terminated_at,
            "ingestion_succeeded_at": self.ingestion_succeeded_at,
            "ingestion_error_message": self.ingestion_error_message,
        }

    @classmethod
    def from_postgres_dict(cls, row: dict) -> 'FileIngestRecord':
        """Create an object from a dictionary returned by psycopg2 (e.g. from cursor.fetchone())."""
        return cls(
            id=row["id"],
            product=resolve_dataproduct(row["product_id_str"]),
            src_filepath=row["src_filepath"],
            staged_filepath=None if row["staged_filepath"] is None else Path(row["staged_filepath"]),
            crawled_at=row["crawled_at"],
            staged_at=row.get("staged_at"),
            ingest_started_at=row.get("ingest_started_at"),
            ingest_terminated_at=row.get("ingest_terminated_at"),
            ingestion_succeeded_at=row.get("ingestion_succeeded_at"),
            ingestion_error_message=row.get("ingestion_error_message"),
        )
