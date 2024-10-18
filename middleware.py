from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import logging
import os
from database import init_db, yield_session

class ZwischenMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        init_db()

    async def dispatch(self, request: Request, call_next):
        start_time = datetime.utcnow()

        response = await call_next(request)

        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()

        ip = request.client.host
        method = request.method
        url = str(request.url)
        status_code = response.status_code
        timestamp = datetime.utcnow().isoformat()
        browser = request.headers.get("user-agent", "")
        referer = request.headers.get("referer", "")

        city = "Unknown" 
        country = "Unknown"
        latitude = 0.0
        longitude = 0.0
        isp = "Unknown"

        log_entry = Log(
            timestamp=timestamp,
            method=method,
            ip=ip,
            city=city,
            country=country,
            latitude=latitude,
            longitude=longitude,
            isp=isp,
            url=url,
            status_code=status_code,
            browser=browser,
            referer=referer,
            processing_time=processing_time
        )

        async with yield_session() as session:
            session.add(log_entry)
            await session.commit()

        return response