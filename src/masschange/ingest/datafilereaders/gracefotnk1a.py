from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np

from masschange.ingest.datafilereaders.base import  DataFileWithProdFlagReader, \
    AsciiDataFileReaderColumn, VariableSchemaAsciiDataFileReaderColumn


class GraceFOTnk1ADataFileReader(DataFileWithProdFlagReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^TNK1A_\d{4}-\d{2}-\d{2}_(?P<stream_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1A_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.noLRI\.tgz'

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type=np.uint),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', const_value='R'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1'),
            AsciiDataFileReaderColumn(index=4, name='tank_id', np_type=np.uint),
            AsciiDataFileReaderColumn(index=5, name='qualflg', np_type='U8'),
            AsciiDataFileReaderColumn(index=6, name='prod_flag', np_type='U8'),
            # skip definitions of columns defined by 'prod_flag'
            # add definitions for VariableSchemaAsciiDataFileReaderColumns
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=0, name='tank_pres', np_type=np.double),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=1, name='reg_pres', np_type=np.double),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=2, name='skin_temp', np_type=np.double),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=3, name='skin_temp_r', np_type=np.double),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=4, name='adap_temp', np_type=np.double),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=5, name='boss_fixed', np_type=np.double),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=6, name='boss_sliding', np_type=np.double)
            # prod_flag_bit_index = 7 is undefined
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac)

    @classmethod
    def _get_first_prod_flag_data_column_position(cls) -> int:
        return 7