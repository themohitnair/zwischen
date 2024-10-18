from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def greet():
    return {
        "message": "hello from zwischen"
    }