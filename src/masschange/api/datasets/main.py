from fastapi import APIRouter
from masschange.api.datasets import gracefo_1a
from masschange.api.datasets import gracefo_acc1a_lttb

router = APIRouter(prefix='/datasets')

router.include_router(gracefo_1a.router)
router.include_router(gracefo_acc1a_lttb.router)

