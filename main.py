import log_config
import logging

from fastapi import FastAPI
from middleware import ZwischenMiddleware
from database import init_zwischen_db
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_zwischen_db()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(ZwischenMiddleware)

@app.get("/")
async def greet():
    return {
        "message": "hello from zwischen"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(port=8000, host="localhost", app="main:app", reload=True)