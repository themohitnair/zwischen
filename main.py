from fastapi import FastAPI
from middleware import ZwischenMiddleware

app = FastAPI()

app.add_middleware(ZwischenMiddleware)

@app.get("/")
async def greet():
    return {
        "message": "hello from zwischen"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(port=8000, host="localhost", app="main:app", reload=True)