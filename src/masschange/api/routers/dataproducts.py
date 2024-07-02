from typing import Annotated

from fastapi import APIRouter, HTTPException, Depends

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_time_series_dataproducts

router = APIRouter()


def map_product(mission_id: str, product_id_suffix: str) -> TimeSeriesDataProduct:
    try:
        product = next(p for p in get_time_series_dataproducts() if
                       p.mission.id == mission_id and p.id_suffix == product_id_suffix)
    except StopIteration:
        raise HTTPException(status_code=404,
                            detail=f'No product found with mission-id {mission_id} and id-suffix {product_id_suffix}')

    return product


@router.get('/{product_id_suffix}', tags=['products', 'metadata'])
async def describe_data_product(product: Annotated[TimeSeriesDataProduct, Depends(map_product)]):
    return product.describe()
