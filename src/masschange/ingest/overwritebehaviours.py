from enum import Enum


class ReaderOverwriteBehavior(Enum):
    """
    Enumerate approaches to overwriting of duplicated data during ingestion
    """
    OVERWRITE_SPAN_EXTRACTED_FROM_FNAME = 1
    OVERWRITE_ROWS_WITH_MATCHING_SRC_FNAME = 2
