from __future__ import annotations
from abc import abstractmethod
from typing import List
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from masschange.ingest.executor.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn,\
    DerivedAsciiDataFileReaderColumn

class OffredFileReader(AsciiDataFileReader):
    """
    Data reader for offred file.
    """
    value_col_name = 'field_value'
    pcf_name_col_name = 'pcf_name'
    unit_col_name = 'unit'
    @classmethod
    def _get_row_for_offred_field_name(cls, offred_excel_filepath: str, field_name: str) -> pd.Series:
        """
        Finds and returns a row in OFFRED Excel sheet that describes a field specified by field_name.

        Args:
            offred_excel_filepath (str): Path to the Excel file that describes OFFRED data
            field_name: Name of the OFFREAD data field

        Returns:
            pandas.Series: A matching row as a pandas Series (column names as index).
                           Throws if not found
        """

        # Read the Excel file.
        df = pd.read_excel(offred_excel_filepath, sheet_name='Sheet1', header=1, dtype=str)

        # Find row with 'field_name' in the "NAME" column.
        matching_rows = df[df["NAME"] == field_name]

        if not matching_rows.empty:
            # Return the first matching row as a Series
            return matching_rows.iloc[0]
        else:
            raise RuntimeError(
                f'Can not find description of {field_name} in OFFRED Excel sheet {offred_excel_filepath}...')
    @classmethod
    def _get_field_names(cls, filepath: str) -> List[str]:
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

    @classmethod
    def get_excel_location(cls):
        # TODO: !!!!!!! move Excel to a new directories for static files
        #  Find if it OK to check in GIT!!!!!!
        return './src/masschange/ingest/executor/datafilereaders/gracefo/offred/static/SRDB_G1.4.3e_PCFdat.xlsx'

    @classmethod
    def _get_current_data_file_dynamic_column_defs(cls, filename: str) -> List[AsciiDataFileReaderColumn]:
        # get names of the fields from the input file
        field_names = cls._get_field_names(filename)

        # sanity check for assumption that first 4 colunm names are always the same
        staic_field_names = ['UTC', 'OBT_Integer', 'OBT_Fraction', 'OBT_Type']
        if field_names[0:4] != staic_field_names:
            raise RuntimeError(
                f'Unexpected format for  {filename}: first 4 fields are: {field_names[0:4]}, '
                f' expected: {staic_field_names}...')

        # Get metadata for dynamic column from Excel
        # skip first 4 names because they are related to the timestamp and are not defined in the Excel
        dyn_col_defs = []
        for idx, name, in enumerate(field_names[4:]):

            name_parts = name.split('.')
            # sanity check
            if len(name_parts) != 2:
                raise RuntimeError(
                    f'Unexpected format of field name {name}. Expected: <name>.<suffix>...')

            if name_parts[1] in cls.get_fieldname_suffix():
                dyn_col_defs.append(AsciiDataFileReaderColumn(index=idx + 4, name=name, np_type=cls.get_field_value_type(), unit=None))
        return dyn_col_defs

    @classmethod
    def _get_current_input_file_column_def(cls, filename: str) -> List[AsciiDataFileReaderColumn]:

        return cls.get_input_column_defs()[0:4] + cls._get_current_data_file_dynamic_column_defs(filename)

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        """
        All OFFRED tables columns with the same names and types,
        except 'field_value' which could be of type 'double' of fixed length string.
        Data type for field_value is defined by abstract method get_field_value_type()
        """
        return [
            AsciiDataFileReaderColumn(index=0, name='utc', np_type='U21', unit=None),
            AsciiDataFileReaderColumn(index=1, name='obt_integer', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=2, name='obt_Fraction', np_type=np.uint, unit='millisecond'),
            AsciiDataFileReaderColumn(index=3, name='OBTfType', np_type='U3', unit=None),
            DerivedAsciiDataFileReaderColumn(name=cls.pcf_name_col_name, np_type='U15', unit=None, is_channel_id_column=True),
            DerivedAsciiDataFileReaderColumn(name=cls.unit_col_name, np_type='U15', unit=None),
            DerivedAsciiDataFileReaderColumn(name=cls.value_col_name, np_type=cls.get_field_value_type(), unit=None,
                                             aggregations=['min', 'max'])
        ]

    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        datafile_column_defs = cls._get_current_input_file_column_def(filename)
        # read all data into memory
        data = np.loadtxt(
            fname=filename,
            skiprows=2,
            delimiter=None,  # split rows by whitespace chunks
            usecols=([col.index for col in datafile_column_defs]),
            dtype=[(col.name, col.np_dtype) for col in datafile_column_defs],
            ndmin=1  # set to 1 to prevent returning a single row as a list instead of array
        )

        num_data_columns = len(datafile_column_defs) - 4
        nrows_in = data.shape[0]
        nrows_out = nrows_in * num_data_columns

        # create reccaray to hold output data
        data_rec = np.recarray(nrows_out, dtype=np.dtype([(col.name, col.np_dtype)
                                                          for col in cls.get_input_column_defs()]))
        # repeat time-related fields for each data field
        for name in [col.name for col in datafile_column_defs[:4]]:
            data_rec[name] = np.tile(data[name], num_data_columns)

        # populate 'pcf_name', 'unit', 'field_value' columns
        for idx, input_col_name in enumerate([col.name for col in datafile_column_defs[4:]]):

            # get name and unit
            col_name_parts = input_col_name.split('.')
            field_metadata = cls._get_row_for_offred_field_name(cls.get_excel_location(),
                                                                (col_name_parts[0]).upper())
            # we don't use input_col_name directly for pcf_name because
            # AsciiDataFileReaderColumn constructor converts names to lower case
            pcf_name = '.'.join([col_name_parts[0].upper(), col_name_parts[1]])

            # units are only make sense for .en fields
            if '.en' in input_col_name:
                unit = field_metadata['UNIT']
            else:
                unit = ""

            start_row = idx * nrows_in
            end_row = (idx + 1) * nrows_in

            data_rec[cls.value_col_name][start_row:end_row] = np.array(data[input_col_name])
            data_rec[cls.pcf_name_col_name][start_row:end_row] = pcf_name
            data_rec[cls.unit_col_name][start_row:end_row] = unit

        return data_rec

    @classmethod
    @abstractmethod
    def get_field_value_type(cls) -> np.dtype:
        """
        Return nc_dtype for field_value columns (double or fixed-length string)
        """
        pass

    @classmethod
    @abstractmethod
    def get_fieldname_suffix(cls) -> List[str]:
        """
        All field names have suffix after dot: <name>.rn, <name>.en, <name>.es
        The suffixes have the following meaning:
        rn -row numbers, integers or floats
        en - engineering numbers, integer or floats
        es - engineering string, string

        We want to have separate readers for string data and numerical data.
        This method return a list of suffixes for field names that the
        reader is supposed to reads
        """
        pass

    @classmethod
    def get_reference_epoch(cls) -> datetime:
        # 00:00 UTC on January 6, 1980
        return datetime(1980, 1, 6, 0)

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.obt_integer, milliseconds=row.obt_fraction)