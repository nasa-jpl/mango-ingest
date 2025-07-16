import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Collection, Set

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.db.conn import get_db_cursor
from masschange.utils.timespan import TimeSpan

log = logging.getLogger()


def get_extant_continuous_aggregates(dataset: TimeSeriesDataset) -> Set[str]:
    with get_db_cursor() as cur:
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
        with get_db_cursor() as cur:
            cur.execute(sql)
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
    channel_id_columns = sorted(field.name for field in dataset.product.get_available_fields() if field.is_channel_id_column)
    channel_id_select_block = ''.join(f'{column}, ' for column in channel_id_columns)
    group_by_expr =', '.join([bucket_expr] + channel_id_columns)
    agg_columns_block = ',\n'.join(agg_column_exprs)

    return f"""
         -- create materialized view without data
        CREATE MATERIALIZED VIEW {new_view_name}
        WITH (timescaledb.continuous) AS
        SELECT {bucket_expr} AS {dataset.product.TIMESTAMP_COLUMN_NAME}, {channel_id_select_block}
        {agg_columns_block}
        FROM {source_name} as src
        GROUP BY {group_by_expr}
        WITH NO DATA;
        
         ---- disable realtime aggregation
         -- RTA is prohibitively expensive, so data availability will be determined by the values used in the continuous
         -- aggregation refresh policy
        ALTER MATERIALIZED VIEW {new_view_name} set (timescaledb.materialized_only = true);
    """


def refresh_continuous_aggregates(dataset: TimeSeriesDataset, temporal_span_limit: TimeSpan = None, enable_chunking: bool = False):
    """
    Refresh all continuous aggregates for a given TimeSeriesDataset.
    Optionally, split the refresh operations into chunks, for faster runtime and improved log responsiveness.
    Unexpectedly, the refresh runtime increases superlinearly with timespan, so this is necessary when refreshing a
    large span.
    """

    temporal_span_limit = temporal_span_limit or TimeSpan(begin=datetime.min.replace(tzinfo=timezone.utc), end=datetime.max.replace(tzinfo=timezone.utc))

    log.info(f'requesting refreshes for continuous aggregates for {dataset.get_table_name()} over {temporal_span_limit}')

    # TODO: consider optimising this to use metadata cache, as using min/max(timestamp) directly becomes expensive at
    #  long data spans (60-120sec for 30 years, at time of testing) - this will require that the metadata is updated
    #  *prior* to calling  refresh_continuous_aggregates() in all relevant contexts.
    #  For safety, this really means a wrapper function that ensures ordering.
    data_span = dataset.get_data_span()
    log.debug(f'data_span: {data_span}')
    refresh_span = temporal_span_limit.intersection(data_span)
    log.debug(f'refresh_span intersects resolving to: {refresh_span}')

    if refresh_span is None:
        log.warning(f'No intersection between temporal_span_limit {temporal_span_limit} and {dataset.get_table_name()} data_span {data_span} - no cagg refresh triggered')
        return

    for aggregation_level in dataset.product.get_available_aggregation_levels():
        if enable_chunking:
            chunk_max_row_count = 10e6

            input_downsampling_ratio = dataset.product.get_available_downsampling_factors()[aggregation_level - 1]
            estimated_row_count = int(refresh_span.duration / dataset.product.time_series_interval / input_downsampling_ratio)
            chunking_required = estimated_row_count > chunk_max_row_count

            if chunking_required:
                chunk_count = math.ceil(estimated_row_count / chunk_max_row_count)
                chunk_duration = refresh_span.duration / chunk_count

                chunk_span = TimeSpan(begin=refresh_span.begin, duration=chunk_duration)
                while chunk_span.end < refresh_span.end:
                    _refresh_continuous_aggregate(dataset, aggregation_level, chunk_span)
                    chunk_span = TimeSpan(chunk_span.end, duration=chunk_span.duration)
                    _refresh_continuous_aggregate(dataset, aggregation_level, chunk_span)

            else:
                refresh_span = TimeSpan(begin=datetime.min, end=datetime.max)
                _refresh_continuous_aggregate(dataset, aggregation_level, refresh_span)
        else:
            refresh_span = TimeSpan(begin=datetime.min, end=datetime.max)
            _refresh_continuous_aggregate(dataset, aggregation_level, refresh_span)


def _refresh_continuous_aggregate(dataset: TimeSeriesDataset, aggregation_level: int, refresh_span: TimeSpan):
    """Refresh a single cagg over a given span"""
    materialized_view_name = dataset.get_table_or_view_name(aggregation_level)
    refresh_span = get_refresh_span(dataset.product, aggregation_level, refresh_span)
    log.info(f'refreshing {materialized_view_name} over {refresh_span}')

    with get_db_cursor(autocommit=True) as cur:
        sql = f"CALL refresh_continuous_aggregate('{materialized_view_name}', %(from_dt)s, %(to_dt)s);"
        cur.execute(sql, {'from_dt': refresh_span.begin, 'to_dt': refresh_span.end})
        log.debug(f'refreshed cont. agg. {materialized_view_name} for buckets spanning {refresh_span}')


def get_refresh_span(dataset: TimeSeriesDataset, aggregation_level: int, data_span: TimeSpan) -> TimeSpan:
    """
    Get a refresh span enclosing all extant buckets which overlap a given data_span.  If no data exists in the materialized
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

    # TODO: This should be resolved dynamically, but can be statically-set for now
    timestamp_column_name = TimeSeriesDataProduct.TIMESTAMP_COLUMN_NAME

    view_name = dataset.get_table_or_view_name(aggregation_level)
    bucket_interval = dataset.product.get_cagg_bucket_interval(aggregation_level)
    log.debug(
        f'dataset {dataset.get_table_name()} bucket interval: {bucket_interval} at aggregation depth {aggregation_level}')

    sql = f"""
    select min({timestamp_column_name}), max({timestamp_column_name})
    from {view_name}
    where {timestamp_column_name} >= ('{data_span.begin.isoformat()}'::timestamp - INTERVAL '{bucket_interval.total_seconds()} SECONDS')
      and {timestamp_column_name} <= ('{data_span.end.isoformat()}'::timestamp + INTERVAL '{bucket_interval.total_seconds()} SECONDS');
      """

    with get_db_cursor() as cur:
        cur.execute(sql)
        results = cur.fetchone()
        if None not in results:
            data_begin = results[0]
            data_end = results[1]
            resolved_span = TimeSpan(begin=data_begin - bucket_interval, end=data_end + bucket_interval)
            log.debug(f'resolved span {resolved_span} from data begin {data_begin} end {data_end}')
        else:
            resolved_span = TimeSpan(begin=datetime.min, end=datetime.max)
            log.debug(f'No data found, resolving to maximal span')

    return resolved_span