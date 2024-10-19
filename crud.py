import aiosqlite
import logging
import pandas as pd
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from utils import validate_ip, retrieve_geoloc
from database import yield_conn
from models import LocationData

async def insert_log(ip: str, method: str, url: str, status_code: str, timestamp: str, browser: str, referrer: str):
    async with yield_conn() as db:
        try:
            if validate_ip(ip):
                locdata: LocationData = await retrieve_geoloc(ip)

                parsed_url = urlparse(url)
                endpoint = parsed_url.path

                query = """
                INSERT INTO Log (ip, country, city, latitude, longitude, method, url, status_code, timestamp, browser, referrer)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                values = (
                    ip, locdata.country, locdata.city, locdata.latitude, locdata.longitude,
                    method, endpoint, status_code, timestamp, browser, referrer
                )
                logger.info(f"Retrieved information from Maxmind for {ip}")
                await db.execute(query, values)
                await db.commit()

                logger.info(f"Inserted record of request from {ip}.")
                return {
                    "ip": ip, "country": locdata.country, "city": locdata.city
                }
            else:
                logger.warning(f"Invalid IP address: {ip}")
                return {
                    "error": "Invalid IP address"
                }
        except ValueError as ve:
            logger.error(f"Likely unable to find IP in Maxmind Database (probable use of Localhost): {ve}")
            return {
                "error": "Invalid data format"
            }
        except aiosqlite.Error as e:
            logger.error(f"SQLite error while inserting a log: {e}")
            await db.rollback()
            return {
                "error": "Database error occurred"
            }
        except Exception as e:
            logger.error(f"Unexpected error while inserting a log: {e}")
            await db.rollback()
            return {
                "error": "An unexpected error occurred"
            }