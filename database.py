import aiosqlite
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

DATABASE_FILE = "zwischen.db"

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
            endpoint TEXT,
            status_code INTEGER,
            browser TEXT,
            os TEXT, 
            device TEXT,
            referrer TEXT
        )
        """)
        await db.commit()
        logger.info("SQLite Database created")

@asynccontextmanager
async def yield_conn():
    async with aiosqlite.connect(DATABASE_FILE) as db:
        try:
            yield db
        finally:
            await db.close()