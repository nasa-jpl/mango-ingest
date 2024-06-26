import math
from datetime import datetime, timedelta
from enum import IntEnum
from typing import Type, Union, Annotated, List

from fastapi import APIRouter, HTTPException, Query
from strenum import StrEnum  # only supported in stdlib from Python 3.11 onward
from pydantic import BaseModel

from masschange.api.errors import TooMuchDataRequestedError
from masschange.api.utils.db.queries import fetch_bulk_metadata
from masschange.api.utils.misc import KeyValueQueryParameter
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion


def construct_router(product: TimeSeriesDataProduct) -> APIRouter:
    router = APIRouter(prefix=f'/{product.id_suffix}')

    InstrumentsEnum = StrEnum('Instruments', sorted(product.instrument_ids))
    # TODO: this approach will require API restart after new dataset version is ingested to register the new id
    available_version_strs = sorted(str(v) for v in product.get_available_versions())
    if len(available_version_strs) == 1:
        available_version_strs.append(' ')  # TODO: see https://github.com/pydantic/pydantic/discussions/7441
    DatasetVersionEnum = StrEnum('DatasetVersion', available_version_strs)

    downsampling_factors = [product.aggregation_step_factor ** exp for exp in
                            range(0, product.get_required_aggregation_depth() + 1)]
    DownsamplingFactorEnum = IntEnum(value='DownsamplingFactor', names=[(str(f), f) for f in downsampling_factors])

    @router.get('/versions/{dataset_version}/instruments/{instrument_id}',
                tags=[product.mission.id, product.get_full_id(), 'metadata'])
    async def describe_dataset_instance(dataset_version: DatasetVersionEnum, instrument_id: InstrumentsEnum):
        dataset = TimeSeriesDataset(product, TimeSeriesDatasetVersion(dataset_version.value), instrument_id.value)
        description = product.describe(exclude_available_versions=True)
        metadata = dataset.get_metadata_properties()
        description.update(metadata)
        return description

    @router.get('/versions/{dataset_version}/instruments/{instrument_id}/data',
                tags=[product.mission.id, product.get_full_id(), 'data'])
    async def get_data(
            instrument_id: InstrumentsEnum,
            dataset_version: DatasetVersionEnum,
            # default values are chosen to allow users to immediately run a fast query from docs page
            # these may be removed later if they are confusing

            # TODO: Re-enable these dynamic default values
            # from_isotimestamp: datetime = DatasetCls.get_data_begin(sorted(DatasetCls.instrument_ids)[0]) or datetime.min,
            # to_isotimestamp: datetime = (DatasetCls.get_data_begin(
            #     sorted(DatasetCls.instrument_ids)[0]) or datetime.min) + timedelta(minutes=1),
            # TODO: Remove these ad-hoc placeholders (used while versioning implementation is in-prog)
            from_isotimestamp: datetime = datetime(2022, 1, 1, 12, 0),
            to_isotimestamp: datetime = datetime(2022, 1, 1, 12, 1),

            fields: Annotated[List[str], Query()] = sorted(
                f.name for f in product.get_available_fields() if not f.is_constant and not f.is_lookup_field),
            downsampling_factor: DownsamplingFactorEnum = DownsamplingFactorEnum['1'],
            filter: Annotated[List[str], Query()] = None
    ):
        if dataset_version.value == ' ':
            raise HTTPException(400,
                                "Come on bro, don't try to break it.  Check out https://github.com/pydantic/pydantic/discussions/7441 if you're that curious.")

        if filter is not None:
            filter = [KeyValueQueryParameter(s) for s in filter]

        else:
            filter = []
        extant_filter_keys = {f.key for f in filter}
        expected_filter_keys = {field.name for field in product.get_available_fields() if field.is_time_series_id_column}
        if not expected_filter_keys.issubset(extant_filter_keys):
            raise HTTPException(status_code=400, detail=f'One or more required fields missing as "filter" qparam (expected {expected_filter_keys} with syntax "filter={{field}}={{value}}")')

        field_names = fields
        fields = set()
        dataset_fields_by_name = {field.name: field for field in product.get_available_fields()}
        using_aggregations = downsampling_factor.value > 1
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
        downsampling_level = int(math.log(downsampling_factor.value, product.aggregation_step_factor))

        try:
            query_start = datetime.now()
            dataset = TimeSeriesDataset(product, TimeSeriesDatasetVersion(dataset_version.value), instrument_id.name)
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
            'downsampling_factor': downsampling_factor.value,
            'nominal_data_interval_seconds': product.get_nominal_data_interval(downsampling_level).total_seconds(),
            'query_elapsed_ms': query_elapsed_ms,
            'data': results
        }

    @router.get('/', tags=[product.mission.id, product.get_full_id(), 'metadata'])
    async def describe_data_product_definition():
        # use metadata cache to enable population of datasets with full metadata
        metadata_cache = list(fetch_bulk_metadata())
        return product.describe(metadata_cache=fetch_bulk_metadata())

    return router
