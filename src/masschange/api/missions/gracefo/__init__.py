from fastapi import APIRouter

from masschange import missions
from masschange.api.missions.gracefo import datasets

router = APIRouter(prefix=f'/{missions.GraceFO.id}')

router.include_router(datasets.router)

