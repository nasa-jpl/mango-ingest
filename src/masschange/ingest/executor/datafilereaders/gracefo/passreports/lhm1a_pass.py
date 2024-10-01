from collections.abc import Collection
from datetime import datetime

from masschange.ingest.executor.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOLhm1APassDataFileReader(ReportFileReader):
    # TODO: sample files has extra columns that are not listed at the GMAT website
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LHM1A_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.pass$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'



