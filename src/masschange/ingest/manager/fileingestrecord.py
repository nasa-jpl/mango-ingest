# Yes, this is a job for an ORM like SQLAlchemy, but a fix-up later is preferable to delaying implementation of the
# ingestion manager even longer to incorporate one - alexdunnjpl 20250508
from datetime import datetime
from typing import Optional


class FileIngestRecord:
    def __init__(
            self,
            id: int,
            src_filepath: str,
            staged_filepath: str,
            crawled_at: datetime,
            staged_at: Optional[datetime] = None,
            ingestion_started_at: Optional[datetime] = None,
            ingestion_terminated_at: Optional[datetime] = None,
            ingestion_succeeded_at: Optional[datetime] = None,
            ingestion_error_message: Optional[str] = None,
    ):
        self.id = id
        self.src_filepath = src_filepath
        self.staged_filepath = staged_filepath
        self.crawled_at = crawled_at
        self.staged_at = staged_at
        self.ingestion_started_at = ingestion_started_at
        self.ingestion_terminated_at = ingestion_terminated_at
        self.ingestion_succeeded_at = ingestion_succeeded_at
        self.ingestion_error_message = ingestion_error_message

    def to_postgres_dict(self) -> dict:
        """Convert the object into a dictionary suitable for psycopg2 INSERT/UPDATE."""
        return {
            "id": self.id,
            "src_filepath": self.src_filepath,
            "staged_filepath": self.src_filepath,
            "crawled_at": self.crawled_at,
            "staged_at": self.staged_at,
            "ingestion_started_at": self.ingestion_started_at,
            "ingestion_terminated_at": self.ingestion_terminated_at,
            "ingestion_succeeded_at": self.ingestion_succeeded_at,
            "ingestion_error_message": self.ingestion_error_message,
        }

    @classmethod
    def from_postgres_dict(cls, row: dict) -> 'FileIngestRecord':
        """Create an object from a dictionary returned by psycopg2 (e.g. from cursor.fetchone())."""
        return cls(
            id=row["id"],
            src_filepath=row["src_filepath"],
            staged_filepath=row["staged_filepath"],
            crawled_at=row["crawled_at"],
            staged_at=row.get("staged_at"),
            ingestion_started_at=row.get("ingestion_started_at"),
            ingestion_terminated_at=row.get("ingestion_terminated_at"),
            ingestion_succeeded_at=row.get("ingestion_succeeded_at"),
            ingestion_error_message=row.get("ingestion_error_message"),
        )
