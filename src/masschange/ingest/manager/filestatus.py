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
        # TODO 2: iron out the distinction between success, termination, etc
        #  currently, the fact that success is tracked with INGEST_TERMINATED is causing brittleness
        #  suggest defining "terminated" as "ended, unsuccessfully" and tracking ingest_success_at separately

        #### Begin temporary bandaid to address TODO 2 issues
        if self == FileStatus.INGEST_SUCCESS:
            return FileStatus.INGEST_TERMINATED.db_column_name

        return f'{self.lower()}_at'
