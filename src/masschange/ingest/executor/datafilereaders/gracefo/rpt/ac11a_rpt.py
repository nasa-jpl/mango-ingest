from collections.abc import Collection
from datetime import datetime
import numpy as np

from masschange.ingest.executor.datafilereaders.base import ReportFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class GraceFOAc11ARptDataFileReader(ReportFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^AC11A_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.rpt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [

            AsciiDataFileReaderColumn(index=19, name='nrec_read', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=20, name='nrec_read_used', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=21, name='nrec_written', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=22, name='nrec_nulled', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=23, name='nrec_non_incorporated', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=24, name='nrec_filled', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=25, name='nrec_consistency', np_type=int, unit=None),

            AsciiDataFileReaderColumn(index=26, name='n_yaw_minus_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=27, name='min_yaw_minus_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=28, name='max_yaw_minus_thrust_dur', np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=29, name='n_pitch_p_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=30, name='max_pitch_p_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=31, name='min_pitch_p_thrust_dur',  np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=32, name='n_yaw_p_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=33, name='max_yaw_p_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=34, name='min_yaw_p_thrust_dur',  np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=35, name='n_pitch_m_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=36, name='max_pitch_m_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=37, name='min_pitch_m_thrust_dur',  np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=38, name='n_roll_m_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=39, name='max_roll_m_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=40, name='min_roll_m_thrust_dur',  np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=41, name='n_roll_p_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=42, name='max_roll_p_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=43, name='min_roll_p_thrust_dur',  np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=44, name='n_oct1_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=45, name='max_oct1_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=46, name='min_oct1_thrust_dur',  np_type=int, unit='millisecond'),

            AsciiDataFileReaderColumn(index=47, name='n_oct2_thrust', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=48, name='max_oct2_thrust_dur',  np_type=int, unit='millisecond'),
            AsciiDataFileReaderColumn(index=49, name='min_oct2_thrust_dur',  np_type=int, unit='millisecond'),

        ]



