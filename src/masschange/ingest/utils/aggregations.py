from abc import ABC
from typing import Sequence, Callable


class Aggregation(ABC):
    """Defines an aggregation on a single column and provides utilities for interfacing with SQL"""

    def __init__(self, name: str, sql_expr_f: Callable[[str], str], output_name_f: Callable[[str], str]):
        self._name = name
        self._sql_expr_f = sql_expr_f
        self._output_name_f = output_name_f

    @property
    def name(self) -> str:
        return self._name

    def get_sql_expression(self, operand_column_name: str) -> str:
        return self._sql_expr_f(operand_column_name)

    def get_aggregated_name(self, operand_column_name: str) -> str:
        return self._output_name_f(operand_column_name)


class TrivialAggregation(Aggregation):
    """
    A default Aggregation provided for backwards-compatibility, which applies a single named SQL function to a column
    and names the output by appending the aggregation function name to the column.
    """

    def __init__(self, func_name: str):
        super().__init__(func_name, lambda column_name: f'{func_name}({column_name})',
                         lambda column_name: f'{column_name}_{func_name.lower()}')


class NestedAggregation(Aggregation):
    """
    An Aggregation which applies a series of named SQL functions to a column.  Provides an output column name equal to
    the input column name unless overridden in constructor.

    N.B. Functions are applied in the same order used when constructing, so if "F1(F2(somecolumn))" is desired, the
    correct construction would be NestedAggregation(["F2", "F1"])
    """

    def __init__(self, name, func_names: Sequence[str], output_name_f_override: Callable[[str], str] = None):
        self._func_names = func_names

        super().__init__(
            name,
            self._compose_sql_expr,
            output_name_f_override if output_name_f_override is not None else lambda column_name: column_name
        )

    def _compose_sql_expr(self, column_name: str) -> str:
        expr = column_name
        for func_name in self._func_names:
            expr = f'{func_name}({expr})'
        return expr
