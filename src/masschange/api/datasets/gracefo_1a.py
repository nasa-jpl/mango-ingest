from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from masschange.datasets.gracefo_1a import GraceFO1ADataset
from masschange.datasets.timeseriesdataset import TooMuchDataRequestedError

router = APIRouter(prefix='/GRACEFO-1A/streams')

config = GraceFO1ADataset.get_config()


@router.get('/{stream_id}/data', tags=['GRACEFO-1A', 'data'])
async def get_full_resolution_data(
        stream_id: str,
        from_isotimestamp: datetime = datetime.min,
        to_isotimestamp: datetime = datetime.max):

    try:
        config.validate_decimation_ratio(1)
        config.validate_stream_id(stream_id)
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))

    try:
        results = GraceFO1ADataset.select(stream_id, from_isotimestamp, to_isotimestamp)
    except TooMuchDataRequestedError as err:
        raise HTTPException(status_code=400, detail=str(err))

    return {
        'from_dt': from_isotimestamp.isoformat(),
        'to_dt': to_isotimestamp.isoformat(),
        'data': results
    }


@router.get('/fields', tags=['GRACEFO-1A', 'metadata'])
async def get_available_fields():
    return {
        'data': sorted(list(GraceFO1ADataset.get_available_fields()))
    }
