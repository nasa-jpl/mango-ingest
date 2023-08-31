from typing import Callable, List, Dict, Mapping


class AggregationRunConfig:
    """Provides a common configuration format for performing decimation."""
    aggregation_funcs: Dict[str, List[str | Callable]] = {}
    column_type_mappings: Dict[str, str] = {}
    column_name_mappings: Dict[str, str] = {}

    def __init__(
            self,
            aggregation_funcs: Dict[str, List[str | Callable]],
            column_type_mappings: Dict[str, str],
            column_name_mappings: Dict[str, str]
    ):
        """

        Parameters
        ----------
        aggregation_funcs - a dict-formatted argument compatible with pandas.DataFrame.agg(), to be run on each rowgroup
            produced when decimating.
            see: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.agg.html
        column_type_mappings - a dict-mapping of column names onto pandas dtypes, to be run on each rowgroup
            aggregate after the aggregations are applied.
            see: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
        column_name_mappings - a dict-formatted argument compatible with pandas.DataFrame.rename() columns
            kwarg.  This is used to rename columns after aggregation and type casts.
            see: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
        """
        self.aggregation_funcs = aggregation_funcs or {}
        self.column_type_mappings = column_type_mappings or {}
        self.column_name_mappings = column_name_mappings or {}

    def apply(self):
        pass
    # populate this later, rename class to AggregationRunConfig or something