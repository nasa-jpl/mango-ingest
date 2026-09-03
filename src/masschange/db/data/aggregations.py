from abc import ABC
from typing import Sequence, Callable, Dict, Optional


class Aggregation(ABC):
    """Defines an aggregation on a single column and provides utilities for interfacing with SQL"""

    def __init__(self, name: str, output_name_f: Callable[[str], str]):
        self._name = name
        self._output_name_f = output_name_f

    @property
    def name(self) -> str:
        """
        The label/name for the aggregation - used to identify aggregations in the API and to specify a friendly label
        for NestedAggregations
        """
        return self._name

    def get_sql_expression(self, operand_column_name: str) -> str:
        """
        Get the SQL expression for the aggregation applied to a given column.
        :param operand_column_name:
        :return:
        """
        raise NotImplementedError

    def get_aggregated_name(self, operand_column_name: str) -> str:
        """
        Get the resultant column label when the aggregation is applied to a given column.
        :param operand_column_name:
        :return:
        """
        return self._output_name_f(operand_column_name)

    def describe(self, operand_column_name: str) -> Dict:
        """Return a JSON representation of this aggregation for use in the API"""
        return {
            'type': self.name,
            'field_name': self.get_aggregated_name(operand_column_name)
        }


class TrivialAggregation(Aggregation):
    """
    A default Aggregation provided for backwards-compatibility, which applies a single named SQL function to a column
    and names the output by appending the aggregation function name to the column.
    """

    def __init__(self, func_name: str, filter_nulls: bool = True):
        self._func_name = func_name
        self.filter_nulls = filter_nulls

        super().__init__(func_name, lambda column_name: f'{column_name}_{func_name.lower()}')

    def get_sql_expression(self, operand_column_name: str) -> str:
        filter_expr_if_exists = f'FILTER (WHERE {operand_column_name} IS NOT NULL)' if self.filter_nulls else ''
        return f'{self._func_name}({operand_column_name}) {filter_expr_if_exists}'


class NestedAggregation(Aggregation):
    """
    An Aggregation which applies a series of named SQL functions to a column.  Provides an output column name equal to
    the input column name unless overridden in constructor.

    N.B. Functions are applied in the same order used when constructing, so if "F1(F2(somecolumn))" is desired, the
    correct construction would be NestedAggregation(["F2", "F1"])
    """

    def __init__(self, name, func_names: Sequence[str], output_name_f_override: Optional[Callable[[str], str]] = None):
        self._func_names = func_names

        super().__init__(
            name,
            output_name_f_override if output_name_f_override is not None else lambda column_name: column_name
        )

    def get_sql_expression(self, operand_column_name: str) -> str:
        expr = operand_column_name
        for func_name in self._func_names:
            expr = f'{func_name}({expr})'
        return expr
