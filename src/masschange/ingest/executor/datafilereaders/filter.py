from abc import ABC, abstractmethod
import pandas as pd


class DataFilter(ABC):
    """Abstract Base Class for all dataframe filters."""

    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the filter logic to the given dataframe."""
        pass

class EqualsFilter(DataFilter):
    """
    Filters a dataframe to keep only rows where
    the column value exactly matches the expected value.
    """
    def __init__(self, column: str, expected_value):
        self.column = column
        self.expected_value = expected_value

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.column not in df.columns:
            return df
        # Returns only rows where the condition is True
        # reset the index to avoid gaps in row numbering
        return df[df[self.column] == self.expected_value].reset_index(drop=True)

