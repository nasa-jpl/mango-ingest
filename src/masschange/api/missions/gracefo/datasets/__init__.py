from fastapi import APIRouter
from masschange.api.missions.gracefo.datasets import acc1a

router = APIRouter(prefix='/datasets')

router.include_router(acc1a.router)

