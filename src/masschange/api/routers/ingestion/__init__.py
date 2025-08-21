from fastapi import APIRouter
from masschange.api.routers.ingestion.readonly import router as readonly_router

router = APIRouter()
router.include_router(readonly_router)
