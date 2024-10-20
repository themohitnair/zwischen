import aiosqlite
import logging
import pandas as pd
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from utils import validate_ip, retrieve_geoloc
from database import yield_conn
from models import LocationData, Log

async def insert_log(ip: str, method: str, url: str, status_code: str, timestamp: str, browser: str, referrer: str) -> Log:
    async with yield_conn() as db:
        try:
            if validate_ip(ip):
                locdata: LocationData = await retrieve_geoloc(ip)

                parsed_url = urlparse(url)
                endpoint = parsed_url.path

                query = "INSERT INTO Log (ip, country, city, latitude, longitude, method, endpoint, status_code, timestamp, browser, referrer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                values = (ip, locdata.country, locdata.city, locdata.latitude, locdata.longitude, method, endpoint, status_code, timestamp, browser, referrer)
                logger.info(f"Retrieved information from Maxmind for {ip}")
                await db.execute(query, values)
                await db.commit()
                logger.info(f"Inserted record of request from {ip}.")
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
                    referrer=referrer
                )
            else:
                logger.warning(f"Invalid IP address: {ip}")
                return {"error": "Invalid IP address"}
        except ValueError as ve:
            logger.error(f"Likely unable to find IP in Maxmind Database (probable use of Localhost): {ve}")
            return {"error": "Invalid data format"}
        except aiosqlite.Error as e:
            logger.error(f"SQLite error while inserting a log: {e}")
            await db.rollback()
            return {"error": "Database error occurred"}
        except Exception as e:
            logger.error(f"Unexpected error while inserting a log: {e}")
            await db.rollback()
            return {"error": "An unexpected error occurred"}

def get_total_requests() -> int:
    async with yield_conn() as db:
        try:
            query = "SELECT COUNT(*) FROM Log"
            result = await db.execute(query).fetchone()
            return result[0]
        except aiosqlite.Error as e:
            logger.error(f"SQLite error while inserting a log: {e}")
            await db.rollback()
            return {"error": "Database error occurred"}
        except Exception as e:
            logger.error(f"Unexpected error while inserting a log: {e}")
            await db.rollback()
            return {"error": "An unexpected error occurred"}

def get_requests_by_country() -> list[dict]:
    async with yield_conn() as db:
        try:
            query = "SELECT country, COUNT(*) FROM Log GROUP BY country"
            result = await db.execute(query).fetchall()
            df = pd.DataFrame(result, columns=["country", "request_count"])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"SQLite error occurred: {e}")
            await db.rollback()
            return {
                "error": "Database error occurred"
            }
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await db.rollback()
            return {
                "error": "An unexpected error occurred"
            }

def get_requests_by_city() -> list[dict]:
    async with yield_conn() as db:
        try:
            query = "SELECT city, COUNT(*) FROM Log GROUP BY city"
            result = await db.execute(query).fetchall()
            df = pd.DataFrame(result, columns=["city", "request_count"])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"SQLite error occurred: {e}")
            await db.rollback()
            return {"error": "Database error occurred"}
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await db.rollback()
            return {"error": "An unexpected error occurred"}

def get_requests_by_method() -> list[dict]:
    async with yield_conn() as db:
        try:
            query = "SELECT method, COUNT(*) FROM Log GROUP BY method"
            result = await db.execute(query).fetchall()
            df = pd.DataFrame(result, columns=["method", "request_count"])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"SQLite error occurred: {e}")
            await db.rollback()
            return {"error": "Database error occurred"}
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await db.rollback()
            return {"error": "An unexpected error occurred"}

async def get_top_n_endpoints(n: int) -> list[dict]:
    async with yield_conn() as db:
        try:
            query = f"SELECT endpoint, COUNT(*) as request_count FROM Log GROUP BY endpoint ORDER BY request_count DESC LIMIT {n}"
            result = await db.execute(query)
            result = await result.fetchall()
            df = pd.DataFrame(result, columns=["endpoint", "request_count"])
            return df.to_dict(orient="records")
        except aiosqlite.Error as e:
            logger.error(f"SQLite error occurred: {e}")
            await db.rollback()
            return {"error": "Database error occurred"}
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            await db.rollback()
            return {"error": "An unexpected error occurred"}