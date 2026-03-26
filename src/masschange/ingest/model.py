"""
Contains a crude set of ORM classes
"""
from dataclasses import dataclass, KW_ONLY
from datetime import datetime
from pathlib import Path
from typing import Union


@dataclass
class JobManagerEntry:
    _: KW_ONLY
    id: int
    src_filepath: Path
    crawled_at: Union[datetime, None]
    ingest_started_at: Union[datetime, None]
    ingestion_terminated_at: Union[datetime, None]
    ingestion_error_msg: Union[str, None]

@dataclass
class IngestionTarget:
    src_filepath: Path
    last_modified: datetime