import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from datetime import datetime
from user_agents import parse
from crud import insert_log
from database import yield_conn, init_zwischen_db, create_serial_sequence
from utils import init_maxmind_geoipdb

logger = logging.getLogger(__name__)

class ZwischenMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        # logger.info("Initiating MaxMind GeoIP City database update.")
        # init_maxmind_geoipdb()
        # logger.info("MaxMind GeoIP database initialized.")
        create_serial_sequence(yield_conn())
        logger.info("Serial Sequence Created.")
        init_zwischen_db()
        logger.info("DuckDB Database Initialized.")

    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/")

        logger.info(f"Request path: {path}")

        exempt_paths = ["/metrics", "/dashboard"]

        if any(path.startswith(exempt) for exempt in exempt_paths):
            logger.info(f"Bypassing middleware for exempted path: {path}")
            return await call_next(request)

        start_time = datetime.utcnow()
        ip = request.client.host
        response = await call_next(request)
        method = request.method
        endpoint = request.url.path
        status_code = response.status_code
        user_agent_string = request.headers.get('user-agent', 'unknown')
        user_agent = parse(user_agent_string)
        browser = user_agent.browser.family
        os = user_agent.os.family

        if user_agent.is_mobile:
            device = 'mobile'
        elif user_agent.is_tablet:
            device = 'tablet'
        else:
            device = 'desktop'

        referrer = request.headers.get('referer', 'unknown')
        timestamp = start_time.strftime("%Y-%m-%d %H:%M:%S")

        db = yield_conn()
        
        await insert_log(ip, method, endpoint, status_code, timestamp, browser, os, device, referrer, db)
        return response