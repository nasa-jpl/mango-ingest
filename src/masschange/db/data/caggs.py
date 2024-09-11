import logging
import math
from datetime import datetime, timedelta
from typing import Collection, Set

from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.db.utils import get_db_connection
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


def get_extant_continuous_aggregates(dataset: TimeSeriesDataset) -> Set[str]:
    with get_db_connection() as conn, conn.cursor() as cur:
        sql = f"""select table_name from information_schema.views where table_name like '{dataset.get_table_name()}_%';"""
        cur.execute(sql)
        results = cur.fetchall()
        return {result[0] for result in results}


def delete_caggs(table_names: Collection[str]):
    if len(table_names) == 0:
        log.debug('Nothing to delete')

    ordered_table_names = sorted(table_names, reverse=True)  # must be in reverse order due to dependencies
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
            dest_column = agg.get_aggregated_name(field.name)
            src_column = dest_column if aggregation_level > 1 else field.name
            column_expr = f'{agg.get_sql_expression(src_column)} as {dest_column}'
            agg_column_exprs.append(column_expr)

    bucket_expr = f"time_bucket(INTERVAL '{aggregation_interval_seconds} SECOND', src.{dataset.product.TIMESTAMP_COLUMN_NAME})"
    time_series_id_columns = sorted(field.name for field in dataset.product.get_available_fields() if field.is_time_series_id_column)
    time_series_id_select_block = ''.join(f'{column}, ' for column in time_series_id_columns)
    group_by_expr =', '.join([bucket_expr] + time_series_id_columns)
    agg_columns_block = ',\n'.join(agg_column_exprs)

    return f"""
         -- create materialized view without data
        CREATE MATERIALIZED VIEW {new_view_name}
        WITH (timescaledb.continuous) AS
        SELECT {bucket_expr} AS {dataset.product.TIMESTAMP_COLUMN_NAME}, {time_series_id_select_block}
        {agg_columns_block}
        FROM {source_name} as src
        GROUP BY {group_by_expr}
        WITH NO DATA;
        
         ---- disable realtime aggregation
         -- RTA is prohibitively expensive, so data availability will be determined by the values used in the continuous
         -- aggregation refresh policy
        ALTER MATERIALIZED VIEW {new_view_name} set (timescaledb.materialized_only = true);
    """


def refresh_continuous_aggregates(dataset: TimeSeriesDataset, enable_chunking: bool = False):
    log.info(f'refreshing continuous aggregates for {dataset.get_table_name()}')
    for aggregation_level in dataset.product.get_available_aggregation_levels():
        materialized_view_name = dataset.get_table_or_view_name(aggregation_level)
        if enable_chunking:
            chunk_max_row_count = 10e6
            data_span = dataset.get_data_span()
            if data_span is None:
                chunking_required = False
            else:
                input_downsampling_ratio = dataset.product.get_available_downsampling_factors()[aggregation_level - 1]
                estimated_row_count = int(data_span.duration / dataset.product.time_series_interval / input_downsampling_ratio)
                chunking_required = estimated_row_count > chunk_max_row_count

            if chunking_required:
                chunk_count = math.ceil(estimated_row_count / chunk_max_row_count)
                chunk_duration = data_span.duration / chunk_count

                chunk_span = TimeSpan(begin=data_span.begin, duration=chunk_duration)
                while chunk_span.end < data_span.end:
                    _refresh_continuous_aggregate(materialized_view_name, chunk_span)
                    chunk_span = TimeSpan(chunk_span.end, duration=chunk_span.duration)
                    _refresh_continuous_aggregate(materialized_view_name, chunk_span)

            else:
                refresh_span = TimeSpan(begin=datetime.min, end=datetime.max)
                _refresh_continuous_aggregate(materialized_view_name, refresh_span)
        else:
            refresh_span = TimeSpan(begin=datetime.min, end=datetime.max)
            _refresh_continuous_aggregate(materialized_view_name, refresh_span)


def _refresh_continuous_aggregate(materialized_view_name: str, refresh_span: TimeSpan):
    log.info(f'refreshing {materialized_view_name} for {refresh_span}')

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
