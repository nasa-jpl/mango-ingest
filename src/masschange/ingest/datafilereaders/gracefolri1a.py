from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import  DataFileWithProdFlagReader, \
    AsciiDataFileReaderColumn, VariableSchemaAsciiDataFileReaderColumn


class GraceFOLri1ADataFileReader(DataFileWithProdFlagReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^LRI1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.LRI\.tgz'

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='rcvtime_intg', np_type=np.ulonglong, unit='second'),
            AsciiDataFileReaderColumn(index=1, name='rcvtime_frac', np_type=np.uint, unit='nanosecond'),
            AsciiDataFileReaderColumn(index=2, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=3, name='prod_flag', np_type='U16', unit=None),
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8', unit=None),

            # skip definitions of columns defined by 'prod_flag'
            # add definitions for VariableSchemaAsciiDataFileReaderColumns
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=0, name='piston_phase', np_type=np.double, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=1, name='phase0_int', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=2, name='phase0_frac', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=3, name='phase1_int', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=4, name='phase1_frac', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=5, name='phase2_int', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=6, name='phase2_frac', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=7, name='phase3_int', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=8, name='phase3_frac', np_type=np.ulonglong, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=9, name=' tSnr', np_type=np.uint, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=10, name='noise8_9', np_type=np.uint, unit='counts'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=11, name='noise11_12', np_type=np.uint, unit='counts')
            # prod_flag_bit_index 12-15 are undefined
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.rcvtime_intg, microseconds=row.rcvtime_frac)

    @classmethod
    def _get_first_prod_flag_data_column_position(cls) -> int:
        return 5