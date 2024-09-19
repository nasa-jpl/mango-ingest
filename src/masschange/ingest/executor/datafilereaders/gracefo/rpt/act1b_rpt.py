from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOAct1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^ACT1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=19, name='Nr_nodatagapfill', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=20, name='CRMS_lin_accl_x',np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=21, name='CRMS_lin_accl_y', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=22, name='CRMS_lin_accl_z', np_type=np.double, unit='m/s2'),

            AsciiDataFileReaderColumn(index=23, name='CRMS_ang_accl_x', np_type=np.double, unit='rad/s2'),
            AsciiDataFileReaderColumn(index=24, name='CRMS_ang_accl_y', np_type=np.double, unit='rad/s2'),
            AsciiDataFileReaderColumn(index=25, name='CRMS_ang_accl_z', np_type=np.double, unit='rad/s2'),

            AsciiDataFileReaderColumn(index=26, name='rel_bias_x', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=27, name='rel_bias_y', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=28, name='rel_bias_z', np_type=np.double, unit='m/s2'),

            AsciiDataFileReaderColumn(index=29, name='rel_scale_x', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=30, name='rel_scale_y', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=31, name='rel_scale_z', np_type=np.double, unit=None),

            AsciiDataFileReaderColumn(index=32, name='rel_res_x', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=33, name='rel_res_y', np_type=np.double, unit='m/s2'),
            AsciiDataFileReaderColumn(index=34, name='rel_res_z', np_type=np.double, unit='m/s2')
        ]



