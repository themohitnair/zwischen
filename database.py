import aiosqlite
from contextlib import asynccontextmanager

DATABASE_FILE = "logs.db"

async def init_zwischen_db() -> None:
    async with aiosqlite.connect(DATABASE_FILE) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS Log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            method TEXT,
            ip TEXT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            url TEXT,
            status_code INTEGER,
            browser TEXT,
            referrer TEXT
        )
        """)
        await db.commit()

@asynccontextmanager
async def yield_conn():
    async with aiosqlite.connect(DATABASE_FILE) as db:
        try:
            yield db
        finally:
            await db.close()