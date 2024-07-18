from collections.abc import Collection
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from masschange.ingest.datafilereaders.base import  DataFileWithProdFlagReader, \
    AsciiDataFileReaderColumn, VariableSchemaAsciiDataFileReaderColumn


class GraceFOMas1BDataFileReader(DataFileWithProdFlagReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        return '^MAS1B_\d{4}-\d{2}-\d{2}_(?P<instrument_id>[CD])_(?P<dataset_version>\d{2})\.txt$'

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        return 'gracefo_1B_\d{4}-\d{2}-\d{2}_RL(?P<dataset_version>\d{2})\.ascii\.(LRI|noLRI)\.tgz'

    @classmethod
    # Use for reading row data
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        return [
            AsciiDataFileReaderColumn(index=0, name='time_intg', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=1, name='time_frac', np_type=np.uint, unit='microsecond'),
            AsciiDataFileReaderColumn(index=2, name='time_ref', np_type='U1', unit=None, const_value='G'),
            AsciiDataFileReaderColumn(index=3, name='GRACEFO_id', np_type='U1', unit=None),
            AsciiDataFileReaderColumn(index=4, name='qualflg', np_type='U8', unit=None),
            AsciiDataFileReaderColumn(index=5, name='prod_flag', np_type='U8', unit=None),

            # add definitions for VariableSchemaAsciiDataFileReaderColumns
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=0, name='mass_thr', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=1, name='mass_thr_err', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=2, name='mass_tnk', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=3, name='mass_tnk_err', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=4, name='gas_mass_thr1', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=5, name='gas_mass_thr2', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=6, name='gas_mass_tnk1', np_type=np.double,
                                                    unit='kg'),
            VariableSchemaAsciiDataFileReaderColumn(prod_flag_bit_index=7, name='gas_mass_tnk2', np_type=np.double,
                                                    unit='kg')
        ]

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.time_intg, microseconds=row.time_frac)

    @classmethod
    def _get_first_prod_flag_data_column_position(cls) -> int:
        return 6