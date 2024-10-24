from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOKbr1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^KBR1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=19, name='crms_dowr', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=20, name='crms_ion', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=21, name='arc_length', np_type=np.double, unit='m'),
            AsciiDataFileReaderColumn(index=22, name='resid_nobs', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=23, name='resid_rms', np_type=np.double, unit='cm'),
            AsciiDataFileReaderColumn(index=24, name='resid_min', np_type=np.double, unit='cm'),
            AsciiDataFileReaderColumn(index=25, name='resid_max', np_type=np.double, unit='cm'),
            AsciiDataFileReaderColumn(index=26, name='number_of_arcs', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=27, name='clkdd_nobs', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=28, name='clkdd_mean', np_type=np.double, unit='ps'),
            AsciiDataFileReaderColumn(index=29, name='clkdd_sigma', np_type=np.double, unit='ps'),
            AsciiDataFileReaderColumn(index=30, name='clkdd_min', np_type=np.double, unit='ps'),
            AsciiDataFileReaderColumn(index=31, name='clkdd_max', np_type=np.double, unit='ps'),
            AsciiDataFileReaderColumn(index=32, name='clkdd_rms', np_type=np.double, unit='ps')
        ]



