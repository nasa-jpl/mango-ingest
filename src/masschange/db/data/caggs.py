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

    create_statement_block = f"""
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

    return create_statement_block



def get_offred_continuous_aggregate_create_statements(dataset: TimeSeriesDataset, aggregation_level: int) -> str:
    # Development prototype of special case behaviour to test performance impact - edunn 20260318
    aggregation_interval_seconds = dataset.product.get_nominal_data_interval(aggregation_level).total_seconds()
    source_base_name = dataset.get_table_or_view_name(aggregation_level - 1)
    new_view_base_name = dataset.get_table_or_view_name(aggregation_level)
    new_int_view_name = f'{new_view_base_name}int'
    new_float_view_name = f'{new_view_base_name}float'

    bucket_expr = f"time_bucket(INTERVAL '{aggregation_interval_seconds} SECOND', src.{dataset.product.TIMESTAMP_COLUMN_NAME})"
    channel_id_columns = sorted(
        field.name for field in dataset.product.get_available_fields() if field.is_channel_id_column)
    channel_id_select_block = ''.join(f'{column}, ' for column in channel_id_columns)
    group_by_expr = ', '.join([bucket_expr] + channel_id_columns)
    int_source_name = f'{source_base_name}int' if aggregation_level > 1 else source_base_name
    value_int_min_src = 'value_int_min' if aggregation_level > 1 else 'value_int'
    value_int_max_src = 'value_int_max' if aggregation_level > 1 else 'value_int'
    float_source_name = f'{source_base_name}float' if aggregation_level > 1 else source_base_name
    value_float_min_src = 'value_float_min' if aggregation_level > 1 else 'value_float'
    value_float_max_src = 'value_float_max' if aggregation_level > 1 else 'value_float'

    return f"""
             ---- create int matviews
            CREATE MATERIALIZED VIEW {new_int_view_name}
            WITH (timescaledb.continuous) AS
            SELECT {bucket_expr} AS {dataset.product.TIMESTAMP_COLUMN_NAME}, {channel_id_select_block}
            MIN({value_int_min_src}) FILTER (WHERE {value_int_min_src} IS NOT NULL) as value_int_min ,
            MAX({value_int_max_src}) FILTER (WHERE {value_int_max_src} IS NOT NULL) as value_int_max
            FROM {int_source_name} as src
            GROUP BY {group_by_expr}
            WITH NO DATA;

             ---- create float matviews
            CREATE MATERIALIZED VIEW {new_float_view_name}
            WITH (timescaledb.continuous) AS
            SELECT {bucket_expr} AS {dataset.product.TIMESTAMP_COLUMN_NAME}, {channel_id_select_block}
            MIN({value_float_min_src}) FILTER (WHERE {value_float_min_src} IS NOT NULL) as value_float_min,
            MAX({value_float_max_src}) FILTER (WHERE {value_float_max_src} IS NOT NULL) as value_float_max
            FROM {float_source_name} as src
            GROUP BY {group_by_expr}
            WITH NO DATA;

             ---- disable realtime aggregation
             -- RTA is prohibitively expensive, so data availability will be determined by the values used in the continuous
             -- aggregation refresh policy
            ALTER MATERIALIZED VIEW {new_int_view_name} set (timescaledb.materialized_only = true);
            ALTER MATERIALIZED VIEW {new_float_view_name} set (timescaledb.materialized_only = true);
            
             ---- create union view as base name
            CREATE VIEW {new_view_base_name} AS
              SELECT {dataset.product.TIMESTAMP_COLUMN_NAME}, {channel_id_select_block} value_int_min, value_int_max, NULL as value_float_min, NULL as value_float_max from {new_int_view_name}
              UNION ALL
              SELECT {dataset.product.TIMESTAMP_COLUMN_NAME}, {channel_id_select_block} NULL as value_int_min, NULL as value_int_max, value_float_min, value_float_max from {new_float_view_name}
              ;
              
        """


def refresh_continuous_aggregates(dataset: TimeSeriesDataset, temporal_span_limit: TimeSpan = None,
                                  enable_chunking: bool = False, chunk_duration_seconds: int = None):
    """
    Refresh all continuous aggregates for a given TimeSeriesDataset.
    Optionally, split the refresh operations into chunks, for faster runtime and improved log responsiveness.
    Unexpectedly, the refresh runtime increases superlinearly with timespan, so this is necessary when refreshing a
    large span.
    """

    if temporal_span_limit is None:
        accurate_data_span = dataset.get_data_span(use_cache=False)

        if accurate_data_span is None:
            log.info(f'No data exists for {dataset.get_table_name()} - aborting cagg refresh')
            return
        else:
            temporal_span_limit = accurate_data_span

    # TODO: consider optimising this to use metadata cache, as using min/max(timestamp) directly becomes expensive at
    #  long data spans (60-120sec for 30 years, at time of testing) - this will require that the metadata is updated
    #  *prior* to calling  refresh_continuous_aggregates() in all relevant contexts.
    #  For safety, this really means a wrapper function that ensures ordering.
    data_span = dataset.get_data_span()
    refresh_span = temporal_span_limit.intersection(data_span)

    if refresh_span is None:
        log.warning(f'No intersection between temporal_span_limit {temporal_span_limit} and {dataset.get_table_name()} data_span {data_span} - no cagg refresh triggered')
        return

    log.info(f'requesting cagg refreshes for {dataset.get_table_name()} over {temporal_span_limit}')

    for aggregation_level in dataset.product.get_available_aggregation_levels():
        if enable_chunking:
            input_downsampling_ratio = dataset.product.get_available_downsampling_factors()[aggregation_level - 1]
            if chunk_duration_seconds:
                chunking_required = refresh_span.duration > timedelta(seconds=chunk_duration_seconds)
            else:
                chunk_max_row_count = 10e6
                estimated_row_count = int(refresh_span.duration / dataset.product.time_series_interval / input_downsampling_ratio)
                chunking_required = estimated_row_count > chunk_max_row_count

            if chunking_required:
                if chunk_duration_seconds:
                    chunk_duration = timedelta(seconds=chunk_duration_seconds *
                                                       dataset.product.get_downsampling_factor(aggregation_level-1))
                else:
                    chunk_count = math.ceil(estimated_row_count / chunk_max_row_count)
                    chunk_duration = refresh_span.duration / chunk_count

                chunk_span = TimeSpan(begin=refresh_span.begin, duration=chunk_duration)
                while chunk_span.end < refresh_span.end:
                    _refresh_continuous_aggregate(dataset, aggregation_level, chunk_span)
                    chunk_span = TimeSpan(chunk_span.end, duration=chunk_span.duration)
                _refresh_continuous_aggregate(dataset, aggregation_level, chunk_span)

            else:
                _refresh_continuous_aggregate(dataset, aggregation_level, refresh_span)
        else:
            _refresh_continuous_aggregate(dataset, aggregation_level, refresh_span)


def _refresh_continuous_aggregate(dataset: TimeSeriesDataset, aggregation_level: int, requested_refresh_span: TimeSpan):
    """Refresh a single cagg over a given span"""

    materialized_view_name = dataset.get_table_or_view_name(aggregation_level)
    bucket_interval = dataset.product.get_cagg_bucket_interval(aggregation_level)
    refresh_span = TimeSpan(requested_refresh_span.begin - bucket_interval, requested_refresh_span.end + bucket_interval)
    log.debug(f'refreshing cagg {materialized_view_name} over {refresh_span}')

    with get_db_cursor(autocommit=True) as cur:
        sql = f"CALL refresh_continuous_aggregate('{materialized_view_name}', %(from_dt)s, %(to_dt)s);"
        cur.execute(sql, {'from_dt': refresh_span.begin, 'to_dt': refresh_span.end})
        log.debug(f'refreshed cont. agg. {materialized_view_name} for buckets spanning {refresh_span}')
