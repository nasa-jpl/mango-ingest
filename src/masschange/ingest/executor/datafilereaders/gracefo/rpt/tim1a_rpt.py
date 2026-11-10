from datetime import datetime
from masschange.ingest.executor.datafilereaders.base import ReportFileReader


class GraceFOTim1ARptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^TIM1A_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'




