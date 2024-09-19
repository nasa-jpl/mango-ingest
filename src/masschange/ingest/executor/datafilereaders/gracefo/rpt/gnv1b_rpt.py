from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOGnv1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^GNV1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        # TODO: units are not specified in the Level-1 Data Product User Handbook
        return [

            AsciiDataFileReaderColumn(index=19, name='npoints_start', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=20, name='hpos_rms_start', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=21, name='cpos_rms_start', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=22, name='lpos_rms_start', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=23, name='hvel_rms_start', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=24, name='cvel_rms_start', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=25, name='lvel_rms_start', np_type=np.double, unit=None),

            AsciiDataFileReaderColumn(index=26, name='npoints_end', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=27, name='hpos_rms_end', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=28, name='cpos_rms_end', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=29, name='lpos_rms_end', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=30, name='hvel_rms_end', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=31, name='cvel_rms_end', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=32, name='lvel_rms_end', np_type=np.double, unit=None)
        ]



