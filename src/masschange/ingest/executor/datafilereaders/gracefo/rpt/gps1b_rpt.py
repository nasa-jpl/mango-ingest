from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class GraceFOGps1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^GPS1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        # TODO: units are not specified in the Level-1 Data Product User Handbook
        return [

            AsciiDataFileReaderColumn(index=19, name='crms_ca', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=20, name='ca_nobs', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=21, name='crms_l1', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=22, name='l1_nobs', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=23, name='crms_l2', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=24, name='l2_nobs', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=25, name='breaks', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=26, name='low_l1_snr', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=27, name='low_l2_snr', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=28, name='ca_mis_lock', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=29, name='discards', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=30, name='nobs_in', np_type=int, unit=None)

        ]


