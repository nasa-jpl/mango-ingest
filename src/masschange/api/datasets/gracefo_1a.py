from datetime import date, datetime
from typing import Dict, Iterable, List, Tuple

from fastapi import APIRouter, HTTPException

from masschange.datasets.gracefo_1a import GraceFO1AFullResolutionDataset
from masschange.datasets.timeseriesdataset import TooMuchDataRequestedError

# from masschange.datasets import mappers


router = APIRouter(prefix='/GRACEFO-1A')


@router.get('/data', tags=['GRACEFO-1A', 'data'])
async def get_full_resolution_data(
        from_isotimestamp: datetime = datetime.min,
        to_isotimestamp: datetime = datetime.max):
    exclude_fields = {'rcvtime_frac', 'rcvtime_intg', 'temporal_partition_key'}
    try:
        results = GraceFO1AFullResolutionDataset.select(from_isotimestamp, to_isotimestamp)
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
        'data': sorted(list(GraceFO1AFullResolutionDataset.get_available_fields()))
    }
