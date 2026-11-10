from datetime import datetime
from masschange.ingest.executor.datafilereaders.base import ReportFileReader


class GraceFOThr1ARptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^THR1A_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

