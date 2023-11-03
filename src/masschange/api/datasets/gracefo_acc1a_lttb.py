from datetime import datetime

from fastapi import APIRouter, HTTPException

from masschange.datasets.gracefo_acc1a_lttb import GraceFOACC1ALTTBDataset
from masschange.datasets.timeseriesdataset import TooMuchDataRequestedError

router = APIRouter(prefix='/GRACEFO-ACC1A-LTTB')

config = GraceFOACC1ALTTBDataset.get_config()


@router.get('/streams/{stream_id}/data', tags=['GRACEFO-1A-LTTB', 'data'])
async def get_data(
        stream_id: str,
        from_isotimestamp: datetime = datetime.min,
        to_isotimestamp: datetime = datetime.max,
        decimation_ratio: int = 1):
    try:
        config.validate_decimation_ratio(decimation_ratio)
        config.validate_stream_id(stream_id)
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))

    try:
        results = GraceFOACC1ALTTBDataset.select(stream_id, from_isotimestamp, to_isotimestamp,
                                          requested_decimation_factor=decimation_ratio)
    except TooMuchDataRequestedError as err:
        raise HTTPException(status_code=400, detail=str(err))

    return {
        'from_dt': from_isotimestamp.isoformat(),
        'to_dt': to_isotimestamp.isoformat(),
        'data': results
    }


@router.get('/', tags=['GRACEFO-1A-LTTB', 'metadata'])
async def get_dataset_details():
    return {
        'available_decimation_ratios': list(GraceFO1ADataset.get_config().available_decimation_ratios),
        'available_fields': sorted(list(GraceFO1ADataset.get_available_fields())),
        'available_streams': [
            {
                'id': 1,
                'name': 'GRACE-FO 1 (GRACE C)'
            },
            {
                'id': 2,
                'name': 'GRACE-FO 2 (GRACE D)'
            }
        ]
    }
