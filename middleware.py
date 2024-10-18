from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import logging
import os
from datetime import datetime
from database import init_db, yield_session
from utils import retrieve_geoloc
from models import LocationData

class ZwischenMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        init_db()

    async def dispatch(self, request: Request, call_next):
        start_time = datetime.utcnow()

        response = await call_next(request)

        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()

        ip =  "103.92.100.152" # request.client.host
        method = request.method
        url = str(request.url)
        status_code = response.status_code
        timestamp = datetime.utcnow().isoformat()
        browser = request.headers.get("user-agent", "")
        referer = request.headers.get("referer", "")

        locdata: LocationData = await retrieve_geoloc(ip)

        city = locdata.city
        country = locdata.country
        latitude = locdata.latitude
        longitude = locdata.longitude

        log_entry = Log(
            timestamp=timestamp,
            method=method,
            ip=ip,
            city=city,
            country=country,
            latitude=latitude,
            longitude=longitude,
            url=url,
            status_code=status_code,
            browser=browser,
            referer=referer,
            processing_time=processing_time
        )

        logging.info(f"IP - {ip}")

        async with yield_session() as session:
            session.add(log_entry)
            await session.commit()

        return response