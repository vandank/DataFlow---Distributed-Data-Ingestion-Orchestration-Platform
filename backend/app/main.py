from fastapi import FastAPI

app = FastAPI(title="AI Data Engineering Platform", version="0.1.0")

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