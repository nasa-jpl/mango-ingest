from collections.abc import Collection
from datetime import datetime, timedelta, date, time, timezone
import logging
from typing import Annotated, List, Union, Iterable
from psycopg2.extensions import cursor as Cursor
import psycopg2
from fastapi import APIRouter, HTTPException, Query, Path
from fastapi.params import Depends
from psycopg2.sql import Identifier, SQL
from strenum import StrEnum  # only supported in stdlib from Python 3.11 onward

from masschange.api.errors import TooMuchDataRequestedError
from masschange.api.utils.misc import KeyValueQueryParameter
from masschange.dataproducts.dataproductfield import DataProductField
from masschange.db.conn import get_db_cursor
from masschange.dataproducts.db.utils import list_table_columns as list_db_table_columns, prepare_where_clause_conditions, prepare_where_clause_parameters

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.dataset import Dataset
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.utils import get_dataproducts
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.utils.misc import get_human_readable_timedelta

router = APIRouter(tags=['datasets'])


def dataset_parameters(mission_id: str, product_id_suffix: str, version_id: str,
                       instrument_id: str) -> Dataset:
    try:
        product = next(p for p in get_dataproducts() if
                       p.mission.id == mission_id and p.id_suffix == product_id_suffix)
    except StopIteration:
        raise HTTPException(status_code=404,
                            detail=f'No product found with mission-id {mission_id} and id-suffix {product_id_suffix}')

    # TODO: Consider re-enabling version validation
    # try:
    #     validated_version_id = next(v for v in product.get_available_versions() if v == version_id)
    # except StopIteration:
    #     raise HTTPException(status_code=404, detail=f'No version with id {version_id} found for product {product_id}')
    version = DatasetVersion(version_id)

    if instrument_id not in product.instrument_ids:
        raise HTTPException(status_code=400, detail=f'Provided instrument_id "{instrument_id}" not in allowed values ({product.instrument_ids})')

    return DatasetFactory.create(product, version, instrument_id)


def instantiate_filters(product: TimeSeriesDataProduct,
                        filter_qparam_strs: Union[List[str], None]) -> List[KeyValueQueryParameter]:
    if filter_qparam_strs is not None:
        filters = [KeyValueQueryParameter(s) for s in filter_qparam_strs]
    else:
        filters = []

    extant_filter_keys = {f.key for f in filters}
    expected_filter_keys = {field.name for field in product.get_available_fields() if field.is_channel_id_column}
    if not expected_filter_keys.issubset(extant_filter_keys):
        raise HTTPException(status_code=400,
                            detail=f'One or more required fields missing as "filter" qparam (expected {expected_filter_keys} with syntax "filter={{field}}={{value}}")')

    return filters

@router.get('/versions/{version_id}/instruments/{instrument_id}', tags=['metadata'])
async def describe_dataset_instance(dataset: Annotated[Dataset, Depends(dataset_parameters)]):
    metadata = dataset.product.describe(exclude_available_versions=True)

    dataset_specific_metadata = dataset.get_metadata_properties()
    if dataset_specific_metadata is None:
        raise HTTPException(status_code=404, detail=f'Could not resolve metadata for dataset {dataset.get_table_name()} - dataset may not exist or its metadata may be missing')
    additional_fields_metadata = dataset_specific_metadata.pop('channel_id_enums')
    for field_name, enumeration in additional_fields_metadata.items():
        field_metadata = next(f for f in metadata['available_fields'] if f['name'] == field_name)
        field_metadata['enum_values'] = enumeration
    metadata.update(dataset_specific_metadata)
    return metadata


@router.get('/versions/{version_id}/instruments/{instrument_id}/data', tags=['data'])
async def get_data(
        dataset: Annotated[TimeSeriesDataset, Depends(dataset_parameters)],
        from_isotimestamp: datetime = datetime(2022, 1, 1, 12, 0, tzinfo=timezone.utc),
        to_isotimestamp: datetime = datetime(2022, 1, 1, 12, 1, tzinfo=timezone.utc),
        fields: Annotated[List[str], Query()] = None,
        downsampling_factor: int = None,
        filter: Annotated[List[str], Query()] = None
):
    requested_field_names = fields or []
    product = dataset.product

    if from_isotimestamp.tzinfo is None:
        from_isotimestamp = from_isotimestamp.replace(tzinfo=timezone.utc)
    if to_isotimestamp.tzinfo is None:
        to_isotimestamp = to_isotimestamp.replace(tzinfo=timezone.utc)

    filters = instantiate_filters(product, filter)

    try:
        downsampling_factor = _get_downsampling_factor(dataset, downsampling_factor, from_isotimestamp, to_isotimestamp)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    fields = _get_fields(dataset, downsampling_factor, requested_field_names or None)
    aggregation_level = _get_aggregation_level(product, downsampling_factor)
    resolve_location = dataset.product.LOCATION_COLUMN_NAME in requested_field_names

    try:
        query_start = datetime.now()
        results = dataset.select(
            from_isotimestamp,
            to_isotimestamp,
            fields=fields,
            aggregation_level=aggregation_level,
            resolve_location=resolve_location,
            filters=filters
        )

        query_elapsed_ms = int((datetime.now() - query_start).total_seconds() * 1000)
    except TooMuchDataRequestedError as err:
        raise HTTPException(status_code=400, detail=str(err))
    except Exception as err:  # TODO: Make this specific
        raise HTTPException(status_code=500, detail=str(err))

    return _get_results_with_metadata(product, from_isotimestamp, to_isotimestamp,
                                      results, query_elapsed_ms, downsampling_factor)

def _get_results_with_metadata(product, from_isotimestamp, to_isotimestamp, results, query_elapsed_ms, downsampling_factor ):
    data = {
            'from_isotimestamp': from_isotimestamp.isoformat(),
            'to_isotimestamp': to_isotimestamp.isoformat(),
            'data_begin': None if len(results) < 1 else results[0][product.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_end': None if len(results) < 1 else results[-1][product.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_count': len(results),
            'query_elapsed_ms': query_elapsed_ms,
            'data': results,
            'downsampling_factor': downsampling_factor}
    if product.is_time_series_dataproduct():
        aggregation_level = _get_aggregation_level(product, downsampling_factor)
        data['nominal_data_interval_seconds'] = product.get_nominal_data_interval(aggregation_level).total_seconds()

    return data

def _get_fields(dataset: TimeSeriesDataset, downsampling_factor: int, requested_field_names: Iterable[str] = None) -> Collection[DataProductField]:
    dataset_fields_by_name = {field.name: field for field in dataset.product.get_available_fields()}
    requested_field_names = requested_field_names or []
    return_all_default_fields = len(requested_field_names) == 0  # if no field names given, return everything available
    using_aggregations = dataset.is_time_series_dataset() and downsampling_factor > 1
    fields = set()
    fields.add(dataset_fields_by_name[dataset.product.TIMESTAMP_COLUMN_NAME])  # timestamp must always be included

    if return_all_default_fields:
        fields.update({field for field in dataset.product.get_available_fields() if (
            not field.is_constant
            and not field.is_lookup_field # omit expensive lookup fields when not explicitly asked for
            and (field.is_aggregable or not using_aggregations) # if fields not specified, respond with all available fields regardless of whether aggregation is required
        )})
    # else validate requested field_names as requested fields are accumulated
    # exclude timestamp from validation- it is never aggregable but must always be present
    else:
        field_names_to_validate = sorted(fn for fn in requested_field_names if fn != dataset.product.TIMESTAMP_COLUMN_NAME)

        for field_name in field_names_to_validate:
            try:
                field = dataset_fields_by_name[field_name]

                if using_aggregations and not field.is_aggregable:
                    raise HTTPException(status_code=400,
                                        detail=f'Specified/required downsampling factor "{downsampling_factor}" is >1, but field {field_name} cannot be aggregated')
                else:
                    fields.add(field)

            except KeyError:
                raise HTTPException(status_code=400,
                                    detail=f'Field "{field_name}" not defined for dataset {dataset.product.get_full_id()} (expected one of {sorted([f.name for f in dataset.product.get_available_fields()])})')

    return fields

def _get_aggregation_level(product, downsampling_factor):
    if product.is_time_series_dataproduct():
        return product.get_available_downsampling_factors().index(downsampling_factor)
    else:
        return 0  # always full resolution

def _get_downsampling_factor(dataset, downsampling_factor, from_isotimestamp, to_isotimestamp):
    if dataset.is_time_series_dataset():
        # Resolve an appropriate downsampling factor, or check the provided value if present in qparams
        if downsampling_factor is None:
            try:
                aggregation_level = dataset.product.get_minimum_aggregation_level(from_isotimestamp, to_isotimestamp)
            except TooMuchDataRequestedError as err:
                raise HTTPException(status_code=400, detail=str(err))

            downsampling_factor = dataset.product.get_available_downsampling_factors()[aggregation_level]
        elif downsampling_factor not in dataset.product.get_available_downsampling_factors():
            raise ValueError(
                f'Provided downsampling_factor "{downsampling_factor}" not in allowed values ({sorted(dataset.product.get_available_downsampling_factors())})')
        return downsampling_factor
    else:
        return 1

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
    filters = instantiate_filters(dataset.product, filter)

    # validate requested field_name
    try:
        field = dataset.product.get_field_by_name(field_name)
    except ValueError as err:
        raise HTTPException(status_code=400, detail=err)
    if not field.is_valid_statistical_target:
        reason = "Field is const-valued" if field.is_constant else f"Field is of unsupported type {field.python_type.__name__}"
        raise HTTPException(status_code=400,detail=f'Cannot request statistical aggregate - {reason}')

    # validate temporal span
    max_query_temporal_span = timedelta(days=31)
    requested_temporal_span = to_isotimestamp - from_isotimestamp
    if requested_temporal_span > max_query_temporal_span:
        raise TooMuchDataRequestedError(
            f'Requested temporal span {get_human_readable_timedelta(requested_temporal_span)} exceeds maximum allowed by server ({get_human_readable_timedelta(max_query_temporal_span)})')

    with get_db_cursor() as cur:
        table_name = dataset.get_table_name()
        select_clause = SQL('{}({})').format(SQL(statistic), Identifier(field_name)).as_string(cur.connection)

        parameters = prepare_where_clause_parameters(from_isotimestamp, to_isotimestamp, filters)
        conditions = prepare_where_clause_conditions(dataset.product.TIMESTAMP_COLUMN_NAME, filters)
        where_clause = SQL(' AND ').join(conditions).as_string(cur.connection)

        try:
            sql = f"""
                        SELECT {select_clause}
                        FROM {table_name}
                        WHERE {where_clause}
                        """
            query_start = datetime.now()
            cur.execute(sql, parameters)
            result = cur.fetchone()
            query_elapsed_ms = int((datetime.now() - query_start).total_seconds() * 1000)

        except psycopg2.errors.UndefinedTable as err:
            logging.warning(f'Query failed with {err}: {sql}')
            raise RuntimeError(
                f'Table {table_name} is not present in db.  Files may not been ingested for this dataset.')
        except psycopg2.errors.UndefinedColumn as err:
            logging.error(f'Query failed due to mismatch between dataset definition and database schema: {err}')
            available_columns = list_db_table_columns(table_name)
            missing_columns = {f.name for f in dataset.product.get_available_fields() if
                               f.name not in available_columns and not f.is_lookup_field and not f.is_constant}
            if missing_columns:
                raise ValueError(
                    f'Some fields are currently unavailable: {missing_columns}. Please remove these fields from your request and try again.')
            else:
                raise err
        except Exception as err:
            logging.warning(f'query failed with {err}: {sql}')
            raise Exception

    return {
        'from_isotimestamp': from_isotimestamp.isoformat(),
        'to_isotimestamp': to_isotimestamp.isoformat(),
        'field': field_name,
        'statistic': statistic,
        'result': result[0],
        'query_elapsed_ms': query_elapsed_ms,
    }

