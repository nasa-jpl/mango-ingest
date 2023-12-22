from fastapi import APIRouter
from masschange.api.missions import gracefo

router = APIRouter(prefix='/missions')

router.include_router(gracefo.router)

