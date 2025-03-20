import logging
from typing import Iterable, Type

from fastapi import APIRouter, HTTPException

from masschange.api.utils.db.queries import fetch_bulk_metadata, fetch_bulk_channel_id_enums
from masschange.dataproducts.utils import get_dataproduct_classes, get_dataproducts
from masschange.missions import Mission

available_missions: Iterable[Type[Mission]] = {dataset.mission for dataset in get_dataproduct_classes()}

router = APIRouter()


@router.get('/', tags=['missions', 'metadata'])
def get_available_missions():
    return {'data': sorted([mission.id for mission in available_missions])}


@router.get('/{mission_id}/products', tags=['missions', 'metadata'])
def get_available_data_products_for_mission(mission_id: str):
    if mission_id not in [m.id for m in available_missions]:
        raise HTTPException(status_code=400,
                            detail=f'No mission found with id {mission_id} in extant missions ({sorted(mission.id for mission in available_missions)})')
    # use metadata cache to enable population of datasets with full metadata
    # TODO: Clean up the keying system here to use TimeSeriesDataSets rather than id-keyed or keyless list elements - ***REMOVED*** 20250320
    metadata_cache = fetch_bulk_metadata()
    for dataset_id, channel_id_enums in fetch_bulk_channel_id_enums().items():
        try:
            metadata_cache[dataset_id]['channel_id_enums'] = channel_id_enums
        except KeyError as err:
            logging.warning(f'Failed to assign channel-id metadata for dataset with id "{dataset_id}": {err}')
    metadata_cache = list(metadata_cache.values())

    mission_data_products = [p for p in get_dataproducts() if p.mission.id == mission_id]
    return {'data': [product.describe(metadata_cache=metadata_cache) for product in
                     sorted(mission_data_products, key=lambda product: product.id_suffix)]}
