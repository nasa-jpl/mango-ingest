from enum import auto
from strenum import StrEnum  # only supported in stdlib from Python 3.11 onward


class FileStatus(StrEnum):
    CRAWLED = auto()
    REJECTED = auto()
    STAGED = auto()
    INGEST_STARTED = auto()
    INGEST_SUCCESS = auto()
    INGEST_TERMINATED = auto()

    @property
    def db_column_name(self) -> str:
        """Return the name of the database column used to track the timestamp of each status change."""
        # TODO: EXCISE THIS - PRONE TO ERROR
        return f'{self.lower()}_at'
