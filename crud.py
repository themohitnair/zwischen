import aiosqlite
from database import yield_session
from utils import validate_ip, retrieve_geoloc
from models import LocationData

async def insert_log(ip: str, method: str, url: str, status_code: str, timestamp: str, browser: str, referrer: str):
    async with yield_db() as db:
        if validate_ip(ip):
            locdata: LocationData = await retrieve_geoloc(ip)

            query = """
            INSERT INTO Log (ip, country, city, latitude, longitude, method, url, status_code, timestamp, browser, referrer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = (
                ip, locdata.country, locdata.city, locdata.latitude, locdata.longitude,
                method, url, status_code, timestamp, browser, referrer
            )

            await db.execute(query, values)
            await db.commit()

            return {"ip": ip, "country": locdata.country, "city": locdata.city}