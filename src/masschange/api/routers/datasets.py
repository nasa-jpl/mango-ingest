import math
from datetime import datetime, timedelta, date, time
import logging
from typing import Annotated, List

import psycopg2
from fastapi import APIRouter, HTTPException, Query, Path
from fastapi.params import Depends
from psycopg2.sql import Identifier, SQL
from strenum import StrEnum  # only supported in stdlib from Python 3.11 onward

from masschange.api.errors import TooMuchDataRequestedError
from masschange.api.utils.misc import KeyValueQueryParameter
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion
from masschange.dataproducts.utils import get_time_series_dataproducts
from masschange.db import get_db_connection
from masschange.db.utils import list_table_columns as list_db_table_columns
from masschange.utils.misc import get_human_readable_timedelta

router = APIRouter(tags=['datasets'])


def dataset_parameters(mission_id: str, product_id_suffix: str, version_id: str,
                       instrument_id: str) -> TimeSeriesDataset:
    try:
        product = next(p for p in get_time_series_dataproducts() if
                       p.mission.id == mission_id and p.id_suffix == product_id_suffix)
    except StopIteration:
        raise HTTPException(status_code=404,
                            detail=f'No product found with mission-id {mission_id} and id-suffix {product_id_suffix}')

    # TODO: Consider re-enabling version validation
    # try:
    #     validated_version_id = next(v for v in product.get_available_versions() if v == version_id)
    # except StopIteration:
    #     raise HTTPException(status_code=404, detail=f'No version with id {version_id} found for product {product_id}')
    version = TimeSeriesDatasetVersion(version_id)

    if instrument_id not in product.instrument_ids:
        raise ValueError(f'Provided instrument_id "{instrument_id}" not in allowed values ({product.instrument_ids})')

    return TimeSeriesDataset(product, version, instrument_id)


@router.get('/', tags=['metadata'])
async def describe_dataset_instance(dataset: Annotated[TimeSeriesDataset, Depends(dataset_parameters)]):
    description = dataset.product.describe(exclude_available_versions=True)
    metadata = dataset.get_metadata_properties()
    description.update(metadata)
    return description


@router.get('/versions/{version_id}/instruments/{instrument_id}/data', tags=['data'])
async def get_data(
        dataset: Annotated[TimeSeriesDataset, Depends(dataset_parameters)],
        from_isotimestamp: datetime = datetime(2022, 1, 1, 12, 0),
        to_isotimestamp: datetime = datetime(2022, 1, 1, 12, 1),
        fields: Annotated[List[str], Query()] = None,
        downsampling_factor: int = 1,
        filter: Annotated[List[str], Query()] = None
):
    product = dataset.product
    # TODO: Test this conditional
    if fields == None:
        fields = sorted(f.name for f in product.get_available_fields() if not f.is_constant and not f.is_lookup_field)

    if downsampling_factor not in product.get_available_downsampling_factors():
        raise ValueError(
            f'Provided downsampling_factor "{downsampling_factor}" not in allowed values ({sorted(product.get_available_downsampling_factors())})')

    if filter is not None:
        filter = [KeyValueQueryParameter(s) for s in filter]

    else:
        filter = []
    extant_filter_keys = {f.key for f in filter}
    expected_filter_keys = {field.name for field in product.get_available_fields() if field.is_time_series_id_column}
    if not expected_filter_keys.issubset(extant_filter_keys):
        raise HTTPException(status_code=400,
                            detail=f'One or more required fields missing as "filter" qparam (expected {expected_filter_keys} with syntax "filter={{field}}={{value}}")')

    field_names = fields
    fields = set()
    dataset_fields_by_name = {field.name: field for field in product.get_available_fields()}
    using_aggregations = downsampling_factor > 1
    for field_name in field_names:
        try:
            field = dataset_fields_by_name[field_name]
            # when downsampling, only pick valid aggregable fields
            # silently dropping non-aggregable fields isn't ideal, but the alternative is to lose the API default
            # fields value, which would be a loss since it significantly improves the docs
            if not using_aggregations or field.has_aggregations or field.is_lookup_field:
                fields.add(field)
        except KeyError:
            raise HTTPException(status_code=400,
                                detail=f'Field "{field_name}" not defined for dataset {product.get_full_id()} (expected one of {sorted([f.name for f in product.get_available_fields()])})')

    #  ensure that timestamp column name is always present in query
    fields.add(dataset_fields_by_name[product.TIMESTAMP_COLUMN_NAME])

    resolve_location = dataset_fields_by_name.get(product.LOCATION_COLUMN_NAME) in fields
    downsampling_level = int(math.log(downsampling_factor, product.aggregation_step_factor))

    try:
        query_start = datetime.now()
        results = dataset.select(
            from_isotimestamp,
            to_isotimestamp,
            fields=fields,
            aggregation_level=downsampling_level,
            resolve_location=resolve_location,
            filters=filter
        )
        query_elapsed_ms = int((datetime.now() - query_start).total_seconds() * 1000)
    except TooMuchDataRequestedError as err:
        raise HTTPException(status_code=400, detail=str(err))
    except Exception as err:  # TODO: Make this specific
        raise HTTPException(status_code=500, detail=str(err))

    return {
        'from_isotimestamp': from_isotimestamp.isoformat(),
        'to_isotimestamp': to_isotimestamp.isoformat(),
        'data_begin': None if len(results) < 1 else results[0][product.TIMESTAMP_COLUMN_NAME].isoformat(),
        'data_end': None if len(results) < 1 else results[-1][product.TIMESTAMP_COLUMN_NAME].isoformat(),
        'data_count': len(results),
        'downsampling_factor': downsampling_factor,
        'nominal_data_interval_seconds': product.get_nominal_data_interval(downsampling_level).total_seconds(),
        'query_elapsed_ms': query_elapsed_ms,
        'data': results
    }


SupportedStatisticsEnum = StrEnum('SupportedStatistics',
                                  sorted({'avg', 'min', 'max', 'count', 'stddev_pop', 'var_pop'}))


@router.get('/versions/{version_id}/instruments/{instrument_id}/fields/{field_name}/statistics/{statistic}',
            tags=['statistics', 'data'])
async def get_statistic_for_field(
        dataset: Annotated[TimeSeriesDataset, Depends(dataset_parameters)],
        field_name: str,
        statistic: SupportedStatisticsEnum,
        from_isotimestamp: datetime = datetime.combine(date.today(), time(0, 0, 0)) - timedelta(days=30),
        to_isotimestamp: datetime = datetime.combine(date.today(), time(0, 0, 0)) + timedelta(days=1),
        filter: Annotated[List[str], Query()] = None
):
    # validate temporal span
    max_query_temporal_span = timedelta(days=31)
    requested_temporal_span = to_isotimestamp - from_isotimestamp
    if requested_temporal_span > max_query_temporal_span:
        raise TooMuchDataRequestedError(
            f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} exceeds maximum allowed by server ({get_human_readable_timedelta(max_query_temporal_span)})')

    with get_db_connection() as conn, conn.cursor() as cur:
        table_name = dataset.get_table_or_view_name(aggregation_depth=0)
        select_clause = SQL('{}({})').format(SQL(statistic), Identifier(field_name)).as_string(conn)
        where_clause = f'{dataset.product.TIMESTAMP_COLUMN_NAME} >= %(from_dt)s AND {dataset.product.TIMESTAMP_COLUMN_NAME} <= %(to_dt)s'

        try:
            sql = f"""
                        SELECT {select_clause}
                        FROM {table_name}
                        WHERE {where_clause}
                        """
            cur.execute(sql, {'from_dt': from_isotimestamp, 'to_dt': to_isotimestamp})
            result = cur.fetchone()
        except psycopg2.errors.UndefinedTable as err:
            logging.warning(f'Query failed with {err}: {sql}')
            raise RuntimeError(
                f'Table {table_name} is not present in db.  Files may not been ingested for this dataset.')
        except psycopg2.errors.UndefinedColumn as err:
            logging.error(f'Query failed due to mismatch between dataset definition and database schema: {err}')
            available_columns = list_db_table_columns(table_name)
            missing_columns = {f.name for f in dataset.product.get_available_fields() if
                               f.name not in available_columns and not f.is_lookup_field}
            raise ValueError(
                f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
        except Exception as err:
            logging.warning(f'query failed with {err}: {sql}')
            raise Exception

    return {
        'from_isotimestamp': from_isotimestamp.isoformat(),
        'to_isotimestamp': to_isotimestamp.isoformat(),
        'field': field_name,
        'statistic': statistic,
        'result': result[0]
    }
