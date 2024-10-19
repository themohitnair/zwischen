from sqlmodel import SQLModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine
from contextlib import asynccontextmanager

class Log(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    timestamp: str
    method: str
    ip: str    
    city: str
    country: str
    latitude: float
    longitude: float
    url: str 
    status_code: int
    browser: str
    referrer: str 

DATABASE_FILE = "logs.db"
DATABASE_URL = f"sqlite+aiosqlite:///{DATABASE_FILE}"

async_engine: AsyncEngine = create_async_engine(
    url=DATABASE_URL,
    connect_args={"check_same_thread": False},
)

async def init_zwischen_db() -> None:
    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

@asynccontextmanager
async def yield_session() -> AsyncSession:
    async with AsyncSession(async_engine) as session:
        try:
            yield session
        finally:
            await session.close()