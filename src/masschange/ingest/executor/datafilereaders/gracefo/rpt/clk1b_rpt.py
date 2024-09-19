from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOClk1BRptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^CLK1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=19, name='overlap_bias_start', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=20, name='overlap_bias_sigma_start',np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=21, name='overlap_slope_start', np_type=np.double, unit='ns/s'),
            AsciiDataFileReaderColumn(index=22, name='overlap_slope_sigma_start', np_type=np.double, unit='ns/s'),
            AsciiDataFileReaderColumn(index=23, name='overlap_rms_zero_start', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=24, name='overlap_rms_fit_start', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=25, name='overlap_npoints_start', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=26, name='overlap_bias_end', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=27, name='overlap_bias_sigma_end', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=28, name='overlap_slope_end', np_type=np.double, unit='ns/s'),
            AsciiDataFileReaderColumn(index=29, name='overlap_slope_sigma_end', np_type=np.double, unit='ns/s'),
            AsciiDataFileReaderColumn(index=30, name='overlap_rms_zero_end', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=31, name='overlap_rms_fit_end', np_type=np.double, unit='ns'),
            AsciiDataFileReaderColumn(index=32, name='overlap_npoints_end', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=33, name='rel_res_y', np_type=int, unit=None)
        ]



