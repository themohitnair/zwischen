import aiosqlite
import logging
import pandas as pd
from urllib.parse import urlparse
from typing import List, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

from utils import validate_ip, retrieve_geoloc
from database import yield_conn
from models import LocationData, Log

async def insert_log(ip: str, method: str, endpoint: str, status_code: str, timestamp: str, browser: str, os: str, device: str, referrer: str) -> Log:
    async with yield_conn() as db:
        try:
            if validate_ip(ip):
                locdata: LocationData = await retrieve_geoloc(ip)             
                query = """
                INSERT INTO Log 
                (ip, country, city, latitude, longitude, method, endpoint, 
                status_code, timestamp, os, browser, referrer) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                values = (ip, locdata.country, locdata.city, locdata.latitude, locdata.longitude, method, endpoint, status_code, timestamp, browser, referrer)
                
                await db.execute(query, values)
                await db.commit()
                logger.info(f"Inserted record for {ip}")
                
                return Log(
                    ip=ip, 
                    country=locdata.country, 
                    city=locdata.city,
                    latitude=locdata.latitude, 
                    longitude=locdata.longitude,
                    method=method, 
                    endpoint=endpoint, 
                    status_code=status_code,
                    timestamp=timestamp, 
                    browser=browser, 
                    os=os,
                    referrer=referrer
                )
            else:
                logger.warning(f"Invalid IP: {ip}")
                return {"error": "Invalid IP address"}
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}

async def number_of_requests() -> int:
    async with yield_conn() as db:
        try:
            query = """
            SELECT COUNT(*) 
            FROM Log
            """
            cursor: aiosqlite.Cursor = await db.execute(query)
            result = await cursor.fetchone()
            return result[0]
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}

async def requests_by_country() -> List[Dict]:
    async with yield_conn() as db:
        try:
            query = """ 
            SELECT country, COUNT(*) as request_count
            FROM Log
            GROUP BY country
            ORDER BY request_count DESC
            """
            cursor: aiosqlite.Cursor = await db.execute(query)
            result = await cursor.fetchall()
            df = pd.DataFrame(result, columns=['country', 'request_count'])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}

async def requests_by_city() -> List[Dict]:
    async with yield_conn() as db:
        try:
            query = """ 
            SELECT city, COUNT(*) as request_count
            FROM Log
            GROUP BY city
            ORDER BY request_count DESC
            """
            cursor: aiosqlite.Cursor = await db.execute(query)
            result = await cursor.fetchall()
            df = pd.DataFrame(result, columns=['city', 'request_count'])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}

async def requests_by_method() -> List[Dict]:
    async with yield_conn() as db:
        try:
            query = """ 
            SELECT method, COUNT(*) as request_count
            FROM Log
            GROUP BY method
            ORDER BY request_count DESC
            """
            cursor: aiosqlite.Cursor = await db.execute(query)
            result = await cursor.fetchall()
            df = pd.DataFrame(result, columns=['method', 'request_count'])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}

async def requests_by_city() -> List[Dict]:
    async with yield_conn() as db:
        try:
            query = """ 
            SELECT endpoint, COUNT(*) as request_count
            FROM Log
            GROUP BY endpoint
            ORDER BY request_count DESC
            """
            cursor: aiosqlite.Cursor = await db.execute(query)
            result = await cursor.fetchall()
            df = pd.DataFrame(result, columns=['endpoint', 'request_count'])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}

async def requests_by_status_code() -> List[Dict]:
    async with yield_conn() as db:
        try:
            query = """ 
            SELECT status_code, COUNT(*) as request_count
            FROM Log
            GROUP BY status_code
            ORDER BY request_count DESC
            """
            cursor: aiosqlite.Cursor = await db.execute(query)
            result = await cursor.fetchall()
            df = pd.DataFrame(result, columns=['status', 'request_count'])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"Database Error: {e}")
            await db.rollback()
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            await db.rollback()
            return {"error": str(e)}