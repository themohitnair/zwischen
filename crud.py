from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from database import yield_session, Log
from utils import validate_ip, retrieve_geoloc
from fastapi import Depends
from models import LocationData

async def insert_log(ip: str, method: str, url: str, status_code: str, timestamp: str, browser: str, referrer: str):
    async with yield_session() as session:
        if validate_ip(ip):
            locdata: LocationData = await retrieve_geoloc(ip)

            log_entry: Log = Log(
                ip=ip,
                country=locdata.country,
                city=locdata.city,
                latitude=locdata.latitude,
                longitude=locdata.longitude,
                method=method,
                url=url,
                status_code=status_code,
                timestamp=timestamp,
                browser=browser,
                referrer=referrer,
            )

            session.add(log_entry)
            await session.commit()
            await session.refresh(log_entry)

            return log_entry