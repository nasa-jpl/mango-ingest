from datetime import datetime
from typing import Type

from fastapi import APIRouter, HTTPException

from masschange.datasets.timeseriesdataset import TimeSeriesDataset


def construct_router(DatasetCls: Type[TimeSeriesDataset]) -> APIRouter:
    router = APIRouter(prefix=f'/{DatasetCls.id_suffix}')

    @router.get('/streams/{stream_id}/data', tags=[DatasetCls.mission.id, DatasetCls.get_full_id(), 'data'])
    async def get_data(
            stream_id: str,
            from_isotimestamp: datetime = datetime.min,
            to_isotimestamp: datetime = datetime.max):

        try:
            results = DatasetCls.select(stream_id, from_isotimestamp, to_isotimestamp, )
        except Exception as err:  # TODO: Make this specific
            raise HTTPException(status_code=400, detail=str(err))

        return {
            'from_isotimestamp': from_isotimestamp.isoformat(),
            'to_isotimestamp': to_isotimestamp.isoformat(),
            'data_begin': None if len(results) < 1 else results[0][DatasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_end': None if len(results) < 1 else results[-1][DatasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
            'data_count': len(results),
            'data': results
        }

    @router.get('/', tags=[DatasetCls.mission.id, DatasetCls.get_full_id(), 'metadata'])
    async def describe_dataset():
        return DatasetCls.describe()

    return router
