from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader, AsciiDataFileReaderColumn


class GraceFOPoe1ARptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^POE1A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        # no-match pattern, because the data are never zipped
        return '$^'

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        # Special case for POE1A:
        # For all other PRT, the file_tag and process_ttag are defined as follows:
        #     file_tag : Date in name of data product, expressed as the time of midnight of that date, in seconds
        #     past January 1, 2000 12:00 UTC
        #     process_ttag: Report creation time, expressed in seconds past January 1, 2000 12:00 UTC
        # so the data types were set to int for these columns
        # For POE1A, sample files have these data as floats

        return [
            AsciiDataFileReaderColumn(index=0, name='file_name', np_type='U40', unit=None),
            AsciiDataFileReaderColumn(index=1, name='file_tag', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=2, name='process_ttag', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=3, name='first_data_point_t_tag', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=4, name='last_data_point_t_tag', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=5, name='n_recs', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=6, name='time_gap_avg', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=7, name='time_gap_var', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=8, name='time_gap_min', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=9, name='time_gap_max', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=10, name='n_qual_bits', np_type=np.ubyte, unit=None),
            AsciiDataFileReaderColumn(index=11, name='bit_count_0', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=12, name='bit_count_1', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=13, name='bit_count_2', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=14, name='bit_count_3', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=15, name='bit_count_4', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=16, name='bit_count_5', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=17, name='bit_count_6', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=18, name='bit_count_7', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=19, name='rms_phase', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=20, name='n_phase', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=21, name='n_phase_rej', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=22, name='per_phase_rej', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=23, name='rms_range', np_type=np.double, unit=None),
            AsciiDataFileReaderColumn(index=24, name='n_range', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=25, name='n_range_rej', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=26, name='per_range_rej', np_type=np.double, unit=None)
        ]




