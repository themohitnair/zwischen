import duckdb
import logging
from typing import Dict
from utils import validate_ip, retrieve_geoloc
from models import LocationData

logger = logging.getLogger(__name__)

async def insert_log(ip: str, method: str, endpoint: str, status_code: str, timestamp: str, browser: str, os: str, device: str, referrer: str, db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if validate_ip(ip):
            locdata: LocationData = await retrieve_geoloc(ip)
            
            query = """
            INSERT INTO Log
            (id, ip, country, city, latitude, longitude, method, endpoint,
            status_code, timestamp, browser, os, device, referrer)
            VALUES (nextval('serial'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = [ip, locdata.country, locdata.city, locdata.latitude, locdata.longitude, method, endpoint, status_code, timestamp, browser, os, device, referrer]
            
            db.execute(query, values)
            
            logger.info(f"Inserted record for {ip}")
            
            return {
                "ip": ip,
                "country": locdata.country,
                "city": locdata.city,
                "latitude": locdata.latitude,
                "longitude": locdata.longitude,
                "method": method,
                "endpoint": endpoint,
                "status_code": status_code,
                "timestamp": timestamp,
                "browser": browser,
                "os": os,
                "device": device,
                "referrer": referrer
            }
        else:
            logger.warning(f"Invalid IP: {ip}")
            return {"error": "Invalid IP address"}
    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}