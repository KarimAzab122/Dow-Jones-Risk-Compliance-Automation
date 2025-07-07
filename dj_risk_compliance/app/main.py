from fastapi import FastAPI
from app.api.endpoints import router as api_router
from app.config import settings

app = FastAPI(
    title="Dow Jones Risk & Compliance API",
    description="FastAPI interface for Dow Jones Risk & Compliance Search API",
    version="1.0.0"
)

app.include_router(api_router, prefix="/api/v1")

@app.get("/")
async def root():
    return {
        "message": "Dow Jones Risk & Compliance API Service",
        "status": "running"
    }