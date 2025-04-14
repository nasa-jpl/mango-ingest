import numpy as np

class FlagField:
    """
    This class provides methods for adding flag field to a dataframe.
    The data frame must have columns xpos, ypos and zpos,
    that hold coordinates in Earth-fixed coordinate system
    """

    @classmethod
    def append_flag_fields(cls, df, source_col_name):
        """
        Parse array-like fields into boolean elements and add columns to the dataframe.
        The name of the columns is <source_col_name>_<index>

        Parameters
        ----------
        df: pd.DataFrame - Data frame to append boolean columns. Mast have a column with name specified in source_col_name
        source_col_name:str - Name of the array-like columns with

        """
        if source_col_name not in df.columns:
            raise ValueError(
                f'Can not append flag field columns to the dataframe. Source column "{source_col_name}" does not exist')
        num_elements = len(df[source_col_name][0])
        for i in range(num_elements):
            col_name = f'{source_col_name}_{str(i)}'
            df[col_name] = df.apply(cls.populate_flag, idx=i, axis=1, result_type='expand')

    @classmethod
    def populate_flag(cls, row, idx) -> str:
        return row.qualflg[idx]



