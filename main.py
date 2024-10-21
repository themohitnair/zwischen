import log_config

import logging
from pathlib import Path
import asyncio
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from middleware import ZwischenMiddleware
from database import init_zwischen_db

from crud import (
    number_of_requests,
    requests_by_country,
    requests_by_city,
    requests_by_method,
    requests_by_endpoint,
    requests_by_os,
    requests_by_browser,
    requests_by_status_code
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_zwischen_db()
    yield

app = FastAPI(lifespan=lifespan)

app.mount("/assets", StaticFiles(directory=Path("metrics/dist/assets")), name="assets")
app.mount("/favicon", StaticFiles(directory=Path("metrics/dist/favicon")), name="favicon")


app.add_middleware(ZwischenMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def greet():
    return {
        "message": "root endpoint"
    }

@app.get("/something")
async def greet():
    return {
        "message": "some other endpoint"
    }

@app.get("/metrics")
async def get_metrics(request: Request):    
    metrics = {
        "total_requests": await number_of_requests(),
        "requests_by_country": await requests_by_country(),
        "requests_by_city": await requests_by_city(),
        "requests_by_method": await requests_by_method(),
        "requests_by_endpoint": await requests_by_endpoint(),
        "requests_by_status_code": await requests_by_status_code(),
        "requests_by_browser": await requests_by_browser(),
        "requests_by_os": await requests_by_os(),
    }
    return JSONResponse(content=metrics)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(port=8000, host="localhost", app="main:app", reload=True)