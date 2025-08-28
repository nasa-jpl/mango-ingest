from __future__ import annotations
from typing import List, Dict
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import os

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn,\
    DerivedAsciiDataFileReaderColumn
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.ingest.overwritebehaviours import ReaderOverwriteBehavior


class OffredFileReader(AsciiDataFileReader):
    """
    Data reader for offred file.
    """
    OVERWRITE_BEHAVIOR = ReaderOverwriteBehavior.OVERWRITE_ROWS_WITH_MATCHING_SRC_FNAME
    SOURCE_FILE_COLUMN_NAME='source_file_name'

    # Names for columns in output table.
    # They are used in multiple  places in the code,
    # so it is convenient to define names in one place
    col_name_pcf_name = 'pcf_name' # columns with field names
    col_name_unit = 'unit' # columns with units
    col_name_int = 'value_int'  # column with int data
    col_name_float = 'value_float'  # column with float data
    col_name_str = 'value_str'  # column with string data

    str_dtype = 'U100' # TODO: may me could be smaller
    float_dtype = np.float32 # np.float32 provides approximately 7 decimal digits of precision, should be enough
    int_dtype = pd.Int64Dtype # int type that supports None


    @classmethod
    def get_field_met_file_location(cls):
        env_name = 'OFFREAD_METADATA_FILE'
        fpath = os.getenv(env_name)
        if fpath is not None:
            return fpath
        raise RuntimeError(
            f'Please set env variable {env_name}...')

    @classmethod
    def get_fields_metadata_dict(cls)-> Dict:
        """
        Read OFFRED field metadata from a file in JSON format.
        Returns: metadat as a dictionary

        """
        with open(cls.get_field_met_file_location(), 'r') as file:
            data = json.load(file)
            return data

    @classmethod
    def _get_field_names(cls, filepath: str) -> list[str] | None:
        """
        Read field names from the OFFRED data file header and return them as a list
        Args:
            filepath (str): OFFRED data file

        Returns:
            List[str]: Field names as a list
        """
        # Assume that the header is a first non-commented line
        with open(filepath, 'r') as f:
            for line in f:
                if not line.startswith('#'):
                    return line.split()
        return None

    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str, check_time_col_names: bool = True) -> List[AsciiDataFileReaderColumn]:
        """
           Returns list of AsciiDataFileReaderColumn that defines columns for a current input file.
           Note that the output file will have different columns definitions

           Parameters
           ----------
           data_fpath: path to input data file
           check_time_col_names: flag to turn on/off check for the assumption that the first 4 time-related column names
                                are the same in all input files. Could be turned off for unit tests

           Returns: List of AsciiDataFileReaderColumn definitions for the current input file
           -------
        """
        # get names of the fields from the input file
        field_names = cls._get_field_names(data_fpath)

        if check_time_col_names:
            # sanity check for assumption that first 4 column names are always the same
            static_field_names = ['UTC', 'OBT_Integer', 'OBT_Fraction', 'OBT_Type']
            if field_names[0:4] != static_field_names:
                raise RuntimeError(
                    f'Unexpected format for  {data_fpath}: first 4 fields are: {field_names[0:4]}, '
                    f' expected: {static_field_names}...')

        dyn_col_defs = []
        types = cls._get_data_column_types(data_fpath, 100)

        # skip first 4 names because they are related to the timestamp and are assumed to be the same for all input files
        for idx, name, in enumerate(field_names[4:]):
            dyn_col_defs.append(AsciiDataFileReaderColumn(index=idx + 4, name=name, np_type=types[idx], unit=None))

        # combine time-related column definition with dynamic column definitions
        return cls.get_input_column_defs()[0:4] + dyn_col_defs

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        """
        So far, we have a single OFFFRED reader, so define the output columns here
        """
        return [
            AsciiDataFileReaderColumn(index=0, name='utc', np_type='U21', unit=None),
            AsciiDataFileReaderColumn(index=1, name='obt_integer', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=2, name='obt_fraction', np_type=np.uint, unit='millisecond'),
            AsciiDataFileReaderColumn(index=3, name='obt_type', np_type='U3', unit=None),
            DerivedAsciiDataFileReaderColumn(name=cls.SOURCE_FILE_COLUMN_NAME, np_type='U100', unit=None),
            DerivedAsciiDataFileReaderColumn(name=cls.col_name_pcf_name, np_type='U15', unit=None, is_channel_id_column=True),

            DerivedAsciiDataFileReaderColumn(name=cls.col_name_unit, np_type='U15', unit=None),
            DerivedAsciiDataFileReaderColumn(name=cls.col_name_int, np_type=cls.int_dtype, unit=None,
                                             aggregations=['min', 'max']),

            DerivedAsciiDataFileReaderColumn(name=cls.col_name_float, np_type=cls.float_dtype, unit=None,
                                             aggregations=['min', 'max']),
            DerivedAsciiDataFileReaderColumn(name=cls.col_name_str, np_type=cls.str_dtype, unit=None),

        ]


    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        datafile_column_defs = cls._get_current_input_file_column_def(filename)


        def _loadtxt_wrapper(encoding='utf-8') -> np.ndarray:
            """
            Wrapper for np.loadtxt call, to pass encoding and to avoid repeating all other arguments
            """
            return np.loadtxt(
                fname=filename,
                skiprows=2,
                delimiter='\t',  # split rows by tab
                usecols=([col.index for col in datafile_column_defs]),
                dtype=[(col.name, col.np_dtype) for col in datafile_column_defs],
                encoding = encoding,
                ndmin=1  # set to 1 to prevent returning a single row as a list instead of array
            )

        try:  # Try UTF-8 (default) first
            data = _loadtxt_wrapper()
        except UnicodeDecodeError:
            print(f"UTF-8 decoding failed for {filename}. Trying cp1252...")
            data =  _loadtxt_wrapper(encoding='cp1252')

        num_data_columns = len(datafile_column_defs) - 4

        # numbr of rows in the input file
        nrows_in = data.shape[0]

        # number of rows in the output recarray
        nrows_out = nrows_in * num_data_columns

        # create recaray to hold output data
        data_rec = np.recarray(nrows_out, dtype=np.dtype([(col.name, col.np_dtype)
                                                          for col in cls.get_input_column_defs()]))
        # repeat time-related fields for each data field
        for name in [col.name for col in datafile_column_defs[:4]]:
            data_rec[name] = np.tile(data[name], num_data_columns)

        # add source file name to the  array
        data_rec[cls.SOURCE_FILE_COLUMN_NAME] [:]= os.path.basename(filename)
        # init nullable columns to None or an empty string
        data_rec[cls.col_name_int] = None
        data_rec[cls.col_name_float] = None
        data_rec[cls.col_name_str] = ''
        data_rec[cls.col_name_unit] = ''

        # read into memory metadata (unit and description) associated with the fields
        met_dict = cls.get_fields_metadata_dict()

        # populate nullable columns
        for idx, col_def in enumerate([col for col in datafile_column_defs[4:]]):
            start_row = idx * nrows_in
            end_row = (idx + 1) * nrows_in

            if col_def.np_dtype == cls.int_dtype:
                out_col_name = cls.col_name_int
            elif col_def.np_dtype == cls.float_dtype:
                out_col_name = cls.col_name_float
            elif col_def.np_dtype == cls.str_dtype:
                out_col_name = cls.col_name_str
            else:
                raise RuntimeError(f"Unsupported dtype {col_def.np_dtype}")

            data_rec[out_col_name][start_row:end_row] = np.array(data[col_def.name])

            # we don't use input_col_name directly for pcf_name because
            # AsciiDataFileReaderColumn constructor converts names to lower case
            # pcf_name prefix should be upper case
            col_name_parts = col_def.name.split('.')
            data_rec[cls.col_name_pcf_name][start_row:end_row] = '.'.join([col_name_parts[0].upper(), col_name_parts[1]])

            # units are only make sense for .en fields
            if '.en' in col_def.name:
                data_rec[cls.col_name_unit][start_row:end_row] = met_dict[col_name_parts[0].upper()]['UNIT']
        return data_rec

    @classmethod
    def get_reference_epoch(cls) -> datetime:
        # 00:00 UTC on January 6, 1980
        return datetime(1980, 1, 6, 0)

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.obt_integer, milliseconds=row.obt_fraction)

    @classmethod
    def _get_data_column_types(cls, filename:str, num_rows:int):
        """
        Read up to num_rows from data file and return data type for each data column
        """
        types = []
        # read data from first num_rows as a strings
        field_names = cls._get_field_names(filename)

        def _loadtxt_wrapper(encoding='utf-8') -> np.ndarray:
            """
            Wrapper for np.loadtxt call, to pass encoding and to avoid repeating all other arguments
            """
            return np.loadtxt(
                fname=filename,
                skiprows=2,
                delimiter='\t',  # split rows by tab
                dtype=[(name, cls.str_dtype) for name in field_names],
                max_rows=num_rows,
                encoding = encoding,
                ndmin=1  # set to 1 to prevent returning a single row as a list instead of array
            )

        try:  # Try UTF-8 (default) first
            data = _loadtxt_wrapper()
        except UnicodeDecodeError:
            print(f"UTF-8 decoding failed while trying to solve column types, file: {filename}. Trying cp1252...")
            data =  _loadtxt_wrapper(encoding='cp1252')

        for idx, name in enumerate(field_names):
            if idx > 3:
                types.append(cls._get_column_type(data[name]))

        return types

    @classmethod
    def _get_column_type(cls, arr: np.array(str)) -> np.dtype | None:
        """
        Look at data fron a field and try to figure out if they are int, float or string
        Parameters
        ----------
        arr : np.array(str) - chunk of data from a field represented as strings
        """

        n_int = 0
        n_float = 0
        n_str = 0

        for val in arr:
            try:
                num = float(val)
            except ValueError:
                # if can't convert to a number, assume it is a string
                n_str += 1
                continue
            if '.' in val:  # check for '.' in the number
                n_float += 1
            else:
                n_int += 1

        # check that all values in the array are assigned to the same type
        expected_cnt = arr.shape[0]
        if max(n_int, n_float, n_str) != arr.shape[0]:
            raise RuntimeError("Can't figure out data type for array: {}".format(arr))

        if n_int == expected_cnt:
            return cls.int_dtype # int type that supports None
        elif n_float == expected_cnt:
            return cls.float_dtype
        elif n_str == expected_cnt:
            return cls.str_dtype
        else:
            raise RuntimeError("Can't figure out data type for array: {}".format(arr))

    @classmethod
    def extract_dataset_version(cls, filepath: str) -> DatasetVersion:
        # no versions for OFFREAD
        return DatasetVersion("01")

