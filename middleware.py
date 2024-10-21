import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from datetime import datetime
from user_agents import parse
from crud import insert_log
from utils import init_maxmind_geoipdb

logger = logging.getLogger(__name__)

class ZwischenMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        logger.info("Initiating MaxMind GeoIP City database update.")
        init_maxmind_geoipdb()
        logger.info("MaxMind GeoIP database initialized.")

    async def dispatch(self, request: Request, call_next):
        exempt_paths = ["/metrics/"]

        if request.url.path in exempt_paths:
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
        
        await insert_log(ip, method, endpoint, status_code, timestamp, browser, os, device, referrer)
        return response