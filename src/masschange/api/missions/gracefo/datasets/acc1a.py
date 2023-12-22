from datetime import datetime

from fastapi import APIRouter, HTTPException

from masschange.datasets.gracefo import GraceFOAcc1ADataset

# TODO: Break this out into a factory which can generate endpoints/labels/etc when given a specific TimeSeriesDataSet subclass
datasetCls = GraceFOAcc1ADataset

router = APIRouter(prefix=f'/{datasetCls.id_suffix}')

@router.get('/streams/{stream_id}/data', tags=[datasetCls.mission.id, datasetCls.get_full_id(), 'data'])
async def get_data(
        stream_id: str,
        from_isotimestamp: datetime = datetime.min,
        to_isotimestamp: datetime = datetime.max):

    try:
        results = datasetCls.select(stream_id, from_isotimestamp, to_isotimestamp, )
    except Exception as err:  #  TODO: Make this specific
        raise HTTPException(status_code=400, detail=str(err))

    return {
        'from_isotimestamp': from_isotimestamp.isoformat(),
        'to_isotimestamp': to_isotimestamp.isoformat(),
        'data_begin': None if len(results) < 1 else results[0][datasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
        'data_end': None if len(results) < 1 else results[-1][datasetCls.TIMESTAMP_COLUMN_NAME].isoformat(),
        'data_count': len(results),
        'data': results
    }


@router.get('/', tags=[datasetCls.mission.id, datasetCls.get_full_id(), 'metadata'])
async def describe_dataset():
    return datasetCls.describe()
