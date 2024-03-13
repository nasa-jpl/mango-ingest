import math
from datetime import datetime, timedelta
from enum import IntEnum
from typing import Type, Union, Annotated, List

from fastapi import APIRouter, HTTPException, Query
from strenum import StrEnum  # only supported in stdlib from Python 3.11 onward

from masschange.api.errors import TooMuchDataRequestedError
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


def construct_router(DatasetCls: Type[TimeSeriesDataset]) -> APIRouter:
    router = APIRouter(prefix=f'/{DatasetCls.id_suffix}')

    StreamEnum = StrEnum('Stream', sorted(DatasetCls.stream_ids))

    downsampling_factors = [DatasetCls.aggregation_step_factor**exp for exp in range(0, DatasetCls.get_required_aggregation_depth() + 1)]
    DownsamplingFactorEnum = IntEnum(value='DownsamplingFactor', names=[(str(f), f) for f in downsampling_factors])

    @router.get('/streams/{stream_id}/data', tags=[DatasetCls.mission.id, DatasetCls.get_full_id(), 'data'])
    async def get_data(
            stream_id: StreamEnum,
            # default values are chosen to allow users to immediately run a fast query from docs page
            # these may be removed later if they are confusing
            from_isotimestamp: datetime = DatasetCls.get_data_begin(sorted(DatasetCls.stream_ids)[0]) or datetime.min,
            to_isotimestamp: datetime = (DatasetCls.get_data_begin(sorted(DatasetCls.stream_ids)[0]) or datetime.min) + timedelta(minutes=1),
            fields: Annotated[List[str], Query()] = sorted(f.name for f in DatasetCls.get_available_fields() if not f.is_constant),
            downsampling_factor: DownsamplingFactorEnum = DownsamplingFactorEnum['1']
    ):
        #  ensure that timestamp column name is always present in query
        field_names = fields
        fields = set()
        dataset_fields_by_name = {field.name: field for field in DatasetCls.get_available_fields()}
        for field_name in field_names:
            try:
                fields.add(dataset_fields_by_name[field_name])
            except KeyError:
                raise HTTPException(status_code=400, detail=f'Field "{field_name}" not defined for dataset {DatasetCls.get_full_id()} (expected one of {sorted([f.name for f in DatasetCls.get_available_fields()])})')
        fields.add(dataset_fields_by_name[DatasetCls.TIMESTAMP_COLUMN_NAME])

        downsampling_level = int(math.log(downsampling_factor.value, DatasetCls.aggregation_step_factor))

        try:
            query_start = datetime.now()
            results = DatasetCls.select(stream_id.name, from_isotimestamp, to_isotimestamp, aggregation_level=downsampling_level, fields=fields)
            query_elapsed_ms = int((datetime.now() - query_start).total_seconds() * 1000)
        except TooMuchDataRequestedError as err:
            raise HTTPException(status_code=400, detail=str(err))
        except Exception as err:  # TODO: Make this specific
            raise HTTPException(status_code=500, detail=str(err))

        return {
            'from_isotimestamp': from_isotimestamp.isoformat(),
            'to_isotimestamp': to_isotimestamp.isoformat(),
            'data_begin': None if len(results) < 1 else results[0][DatasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_end': None if len(results) < 1 else results[-1][DatasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_count': len(results),
            'downsampling_factor': downsampling_factor.value,
            'nominal_data_interval_seconds': DatasetCls.get_nominal_data_interval(downsampling_level).total_seconds(),
            'query_elapsed_ms': query_elapsed_ms,
            'data': results
        }

    @router.get('/', tags=[DatasetCls.mission.id, DatasetCls.get_full_id(), 'metadata'])
    async def describe_dataset():
        return DatasetCls.describe()

    return router
