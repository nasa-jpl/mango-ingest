import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Collection, Set, Callable, Sequence

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.db import get_db_connection
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


def get_extant_continuous_aggregates(dataset: TimeSeriesDataset) -> Set[str]:
    with get_db_connection() as conn, conn.cursor() as cur:
        sql = f"""select table_name from information_schema.views where table_name like '{dataset.get_table_name()}_%';"""
        cur.execute(sql)
        results = cur.fetchall()
        return {result[0] for result in results}


def delete_caggs(table_names: Collection[str]):
    ordered_table_names = sorted(table_names, reverse=True)  # must be in reverse order due to dependencies
    pass  # TODO: IMPLEMENT
    for table_name in ordered_table_names:
        sql = f"drop materialized view {table_name};"
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            log.debug(f'Deleted continous aggregate "{table_name}"')


def get_continuous_aggregate_create_statements(dataset: TimeSeriesDataset, aggregation_level: int) -> str:
    aggregation_interval_seconds = dataset.product.get_nominal_data_interval(aggregation_level).total_seconds()
    source_name = dataset.get_table_or_view_name(aggregation_level - 1)
    new_view_name = dataset.get_table_or_view_name(aggregation_level)

    agg_column_exprs = []
    aggregable_fields = [field for field in dataset.product.get_available_fields() if field.has_aggregations]
    for field in aggregable_fields:
        for agg in field.aggregations:
            dest_column = f'{field.name}_{agg.lower()}'
            src_column = dest_column if aggregation_level > 1 else field.name
            column_expr = f'{agg}({src_column}) as {dest_column}'
            agg_column_exprs.append(column_expr)

    bucket_expr = f"time_bucket(INTERVAL '{aggregation_interval_seconds} SECOND', src.{dataset.product.TIMESTAMP_COLUMN_NAME})"
    agg_columns_block = ',\n'.join(agg_column_exprs)

    return f"""
         -- create materialized view without data
        CREATE MATERIALIZED VIEW {new_view_name}
        WITH (timescaledb.continuous) AS
        SELECT {bucket_expr} AS {dataset.product.TIMESTAMP_COLUMN_NAME},
        {agg_columns_block}
        FROM {source_name} as src
        GROUP BY {bucket_expr}
        WITH NO DATA;
        
         ---- disable realtime aggregation
         -- RTA is prohibitively expensive, so data availability will be determined by the values used in the continuous
         -- aggregation refresh policy
        ALTER MATERIALIZED VIEW {new_view_name} set (timescaledb.materialized_only = true);
    """


def refresh_continuous_aggregates(dataset: TimeSeriesDataset):
    log.info(f'refreshing continuous aggregates for {dataset.get_table_name()}')
    for aggregation_level in dataset.product.get_available_aggregation_levels():
        materialized_view_name = dataset.get_table_or_view_name(aggregation_level)
        bucket_interval = dataset.product.get_nominal_data_interval(aggregation_level)  # TODO: Why is this unused?
        # Refresh span calculation soft-disabled as initial tests indicate that refreshing continuous aggregates for
        #   which no relevant data change has taken place is essentially free
        # refresh_span = get_refresh_span(materialized_view_name, bucket_interval, data_temporal_span)
        refresh_span = TimeSpan(begin=datetime.min, end=datetime.max)
        conn = get_db_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
            sql = f"CALL refresh_continuous_aggregate('{materialized_view_name}', %(from_dt)s, %(to_dt)s);"
            cur.execute(sql, {'from_dt': refresh_span.begin, 'to_dt': refresh_span.end})
            log.debug(f'refreshed cont. agg. {materialized_view_name} for buckets spanning {refresh_span}')
        conn.close()


def get_refresh_span(view_name: str, bucket_interval: timedelta, data_span: TimeSpan) -> TimeSpan:
    """
    Get a dataspan enclosing all extant buckets which overlap a given data_span.  If no data exists in the materialized
    view yet, instead return a safe value which will ensure timescaledb does not complain about too-small a window.

    Parameters
    ----------
    view_name - the name of the materialized view
    bucket_interval - the interval/size of this view's buckets
    data_span - the span of data for which to resolve a refresh span

    Returns
    -------
    an inclusive bucket span over which to refresh the continuous aggregate/materialized view

    """

    sql = f"""
    select min(bucket), max(bucket)
    from {view_name}
    where bucket >= ('{data_span.begin.isoformat()}'::timestamp - INTERVAL '{bucket_interval.total_seconds()} SECONDS')
      and bucket <= ('{data_span.end.isoformat()}'::timestamp + INTERVAL '{bucket_interval.total_seconds()} SECONDS');
      """

    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        results = cur.fetchone()
        if None not in results:
            return TimeSpan(begin=results[0], end=results[1] + bucket_interval)
        else:
            return TimeSpan(begin=datetime.min, end=datetime.max)


class Aggregation(ABC):
    """Defines an aggregation on a single column and provides utilities for interfacing with SQL"""

    def __init__(self, sql_expr_f: Callable[[str], str], output_name_f: Callable[[str], str]):
        self._sql_expr_f = sql_expr_f
        self._output_name_f = output_name_f

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
        super().__init__(
            lambda column_name: f'{func_name}({column_name})',
            lambda column_name: f'{column_name}_{func_name.lower()}'
        )


class NestedAggregation(Aggregation):
    """
    An Aggregation which applies a series of named SQL functions to a column.  Provides an output column name equal to
    the input column name unless overridden in constructor.

    N.B. Functions are applied in the same order used when constructing, so if "F1(F2(somecolumn))" is desired, the
    correct construction would be NestedAggregation(["F2", "F1"])
    """

    def __init__(self, func_names: Sequence[str], output_name_f_override: Callable[[str], str] = None):
        self._func_names = func_names

        super().__init__(
            self._compose_sql_expr,
            output_name_f_override if output_name_f_override is not None else lambda column_name: column_name
        )

    def _compose_sql_expr(self, column_name: str) -> str:
        expr = column_name
        for func_name in self._func_names:
            expr = f'{func_name}({expr})'
        return expr
