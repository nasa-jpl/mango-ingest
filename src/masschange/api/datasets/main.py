from fastapi import APIRouter
from masschange.api.datasets import gracefo_1a

router = APIRouter(prefix='/datasets')

router.include_router(gracefo_1a.router)

