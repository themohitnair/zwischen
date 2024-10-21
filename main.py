import log_config

import logging
from pathlib import Path
import asyncio
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from middleware import ZwischenMiddleware

app = FastAPI()

app.add_middleware(ZwischenMiddleware)

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
async def get_metrics():    
    ...

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(port=8000, host="localhost", app="main:app", reload=True)