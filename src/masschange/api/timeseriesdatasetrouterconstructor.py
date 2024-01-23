from datetime import datetime, timedelta
from typing import Type, Union, Annotated, List

from fastapi import APIRouter, HTTPException, Query
from strenum import StrEnum  # only supported in stdlib from Python 3.11 onward

from masschange.datasets.timeseriesdataset import TimeSeriesDataset


def construct_router(DatasetCls: Type[TimeSeriesDataset]) -> APIRouter:
    router = APIRouter(prefix=f'/{DatasetCls.id_suffix}')

    StreamEnum = StrEnum('Stream', sorted(DatasetCls.stream_ids))

    @router.get('/streams/{stream_id}/data', tags=[DatasetCls.mission.id, DatasetCls.get_full_id(), 'data'])
    async def get_data(
            stream_id: StreamEnum,
            # default values are chosen to allow users to immediately run a fast query from docs page
            # these may be removed later if they are confusing
            from_isotimestamp: datetime = DatasetCls.get_data_begin(sorted(DatasetCls.stream_ids)[0]),
            to_isotimestamp: datetime = DatasetCls.get_data_begin(sorted(DatasetCls.stream_ids)[0]) + timedelta(minutes=1),
            fields: Annotated[List[str], Query()] = sorted(DatasetCls.available_fields),
    ):

        try:
            query_start = datetime.now()
            results = DatasetCls.select(stream_id.name, from_isotimestamp, to_isotimestamp, filter_to_fields=fields)
            query_elapsed_ms = int((datetime.now() - query_start).total_seconds() * 1000)
        except Exception as err:  # TODO: Make this specific
            raise HTTPException(status_code=400, detail=str(err))

        return {
            'from_isotimestamp': from_isotimestamp.isoformat(),
            'to_isotimestamp': to_isotimestamp.isoformat(),
            'data_begin': None if len(results) < 1 else results[0][DatasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_end': None if len(results) < 1 else results[-1][DatasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_count': len(results),
            'query_elapsed_ms': query_elapsed_ms,
            'data': results
        }

    @router.get('/', tags=[DatasetCls.mission.id, DatasetCls.get_full_id(), 'metadata'])
    async def describe_dataset():
        return DatasetCls.describe()

    return router
