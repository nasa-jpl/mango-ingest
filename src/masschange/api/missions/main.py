from typing import Iterable, Type

from fastapi import APIRouter

from masschange.api.timeseriesdataproductrouterconstructor import construct_router

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_time_series_dataproduct_classes
from masschange.missions import Mission


missions: Iterable[Type[Mission]] = {dataset.mission for dataset in get_time_series_dataproduct_classes()}

# Constructs routing for everything in the /missions/{id}/datasets/{id} tree
# This isn't super-clean, but saves having an equivalent "vine" of two-line files to navigate through.
# Feel free to break it out into an import tree if it gets complicated when non-TimeSeriesDataset stuff is implemented
missions_router = APIRouter(prefix='/missions')

for mission in missions:
    mission_router = APIRouter(prefix=f'/{mission.id}')

    mission_data_products_router = APIRouter(prefix='/datasets')

    mission_data_products = [product() for product in get_time_series_dataproduct_classes() if product.mission == mission]

    @mission_data_products_router.get('/', tags=['dataproducts', 'metadata'])
    def get_available_data_products_for_mission():
        return {'data': [product.describe() for product in sorted(mission_data_products, key=lambda product: product.id_suffix)]}

    for product in mission_data_products:
        product_router = construct_router(product)
        mission_data_products_router.include_router(product_router)

    mission_router.include_router(mission_data_products_router)
    missions_router.include_router(mission_router)


@missions_router.get('/', tags=['missions', 'metadata'])
def get_available_missions():
    return {'data': sorted([mission.id for mission in missions])}
