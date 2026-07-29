from fastapi import APIRouter

from app.api.v1.endpoints import ingestions, sources, reports

api_router = APIRouter()
api_router.include_router(sources.router, prefix="/sources", tags=["sources"])
api_router.include_router(ingestions.router, prefix="/ingestions", tags=["ingestions"])
api_router.include_router(reports.router, prefix="/reports", tags=["reports"])