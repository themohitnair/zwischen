import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from datetime import datetime

logger = logging.getLogger(__name__)

from crud import insert_log
from utils import init_maxmind_geoipdb

class ZwischenMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        logger.info("Initiating MaxMind GeoIP City database update.")
        init_maxmind_geoipdb()
        logger.info("MaxMind GeoIP database initialized.")

    async def dispatch(self, request: Request, call_next):
        start_time = datetime.utcnow()
        ip = request.client.host
        
        response = await call_next(request)

        method = request.method
        url = str(request.url)
        status_code = response.status_code
        browser = request.headers.get('user-agent', 'unknown')
        referrer = request.headers.get('referer', 'unknown')
        timestamp = start_time.strftime("%Y-%m-%d %H:%M:%S")

        await insert_log(ip, method, url, status_code, timestamp, browser, referrer)

        return response