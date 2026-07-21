from fastapi import FastAPI
from app.api.v1.api import api_router

app = FastAPI(title="AI Data Engineering Platform", version="0.1.0")
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
def root():
    return {
        "message": "AI Data Engineering Platform is running."
    }

@app.get("/health")
def health_check():
    return {
        "status": "ok"
    }