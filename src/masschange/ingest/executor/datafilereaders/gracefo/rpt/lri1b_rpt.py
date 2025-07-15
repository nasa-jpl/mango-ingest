from collections.abc import Collection
from datetime import datetime
from masschange.ingest.executor.datafilereaders.base import ReportFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn
import numpy as np

class GraceFOLri1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LRI1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[Y])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=19, name='crms_twr', np_type=np.double, unit='m'),

            AsciiDataFileReaderColumn(index=21, name='arc_length', np_type=np.double, unit='h'),
            AsciiDataFileReaderColumn(index=22, name='resid_nobs', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=23, name='resid_rms', np_type=np.double, unit='cm'),
            AsciiDataFileReaderColumn(index=24, name='resid_min', np_type=np.double, unit='cm'),
            AsciiDataFileReaderColumn(index=25, name='resid_max', np_type=np.double, unit='cm'),
            AsciiDataFileReaderColumn(index=26, name='number_of_arcs', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=28, name='est_scale_1', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=29, name='est_scale_sigma_1', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=30, name='est_scale_2', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=31, name='est_scale_sigma_2', np_type=np.double, unit=None)

            # column 20, 27, 32 (0-based) are undefined
        ]