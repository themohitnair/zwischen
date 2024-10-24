import duckdb
import logging
from typing import Dict, Literal
from utils import validate_ip, retrieve_geoloc
from models import LocationData

logger = logging.getLogger(__name__)

async def insert_log(ip: str, method: str, endpoint: str, status_code: str, timestamp: str, browser: str, os: str, device: str, referrer: str, db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if validate_ip(ip):
            locdata: LocationData = await retrieve_geoloc(ip)
            
            query = """
            INSERT INTO Log
            (id, ip, timestamp, country, city, latitude, longitude, method, endpoint,
            status_code, browser, os, device, referrer)
            VALUES (nextval('serial'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = [ip, timestamp, locdata.country, locdata.city, locdata.latitude, locdata.longitude, method, endpoint, status_code, browser, os, device, referrer]
            
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

async def number_of_requests(mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    try:
        if mode == "month":
            query = "SELECT COUNT(*) FROM Log WHERE timestamp >= date_trunc('month', current_timestamp)"
        elif mode == "day":
            query = "SELECT COUNT(*) FROM Log WHERE timestamp >= date_trunc('day', current_timestamp)"
        elif mode == "hour":
            query = "SELECT COUNT(*) FROM Log WHERE timestamp >= date_trunc('hour', current_timestamp)"
        elif mode == "week":
            query = "SELECT COUNT(*) FROM Log WHERE timestamp >= date_trunc('week', current_timestamp)"
        elif mode == "year":
            query = "SELECT COUNT(*) FROM Log WHERE timestamp >= date_trunc('year', current_timestamp)"
        elif mode == "alltime":
            query = "SELECT COUNT(*) FROM Log"
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query).fetchone()
        
        return {
            "count": result[0]
        }

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_ip(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT ip, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY ip 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT ip, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY ip 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT ip, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY ip 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT ip, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY ip 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT ip, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY ip 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT ip, COUNT(*) as request_count 
                FROM Log 
                GROUP BY ip 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()        
        requests = [{"ip": row[0], "request_count": row[1]} for row in result]        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_method(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT method, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY method 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT method, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY method 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT method, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY method 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT method, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY method 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT method, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY method 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT method, COUNT(*) as request_count 
                FROM Log 
                GROUP BY method 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"method": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_city(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT city, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY city 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT city, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY city 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT city, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY city 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT city, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY city 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT city, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY city 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT city, COUNT(*) as request_count 
                FROM Log 
                GROUP BY city 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"city": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_country(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT country, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY country 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT country, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY country 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT country, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY country 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT country, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY country 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT country, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY country 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT country, COUNT(*) as request_count 
                FROM Log 
                GROUP BY country 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"country": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_coordinates(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT latitude, longitude, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY latitude, longitude 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT latitude, longitude, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY latitude, longitude 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT latitude, longitude, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY latitude, longitude 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT latitude, longitude, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY latitude, longitude 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT latitude, longitude, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY latitude, longitude 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT latitude, longitude, COUNT(*) as request_count 
                FROM Log 
                GROUP BY latitude, longitude 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"latitude": row[0], "longitude": row[1], "request_count": row[2]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_status_code(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT status_code, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY status_code 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT status_code, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY status_code 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT status_code, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY status_code 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT status_code, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY status_code 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT status_code, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY status_code 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT status_code, COUNT(*) as request_count 
                FROM Log 
                GROUP BY status_code 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"status_code": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_os(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT os, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY os 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT os, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY os 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT os, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY os 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT os, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY os 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT os, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY os 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT os, COUNT(*) as request_count 
                FROM Log 
                GROUP BY os 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"os": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_browser(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT browser, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY browser 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT browser, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY browser 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT browser, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY browser 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT browser, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY browser 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT browser, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY browser 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT browser, COUNT(*) as request_count 
                FROM Log 
                GROUP BY browser 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"browser": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_device(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT device, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY device 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT device, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY device 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT device, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY device 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT device, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY device 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT device, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY device 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT device, COUNT(*) as request_count 
                FROM Log 
                GROUP BY device 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"device": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_referrer(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT referrer, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY referrer 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT referrer, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY referrer 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT referrer, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY referrer 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT referrer, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY referrer 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT referrer, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY referrer 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT referrer, COUNT(*) as request_count 
                FROM Log 
                GROUP BY referrer 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"referrer": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}

async def requests_by_endpoint(n: int, mode: Literal["month", "day", "hour", "week", "year", "alltime"], db: duckdb.DuckDBPyConnection) -> Dict:
    try:
        if mode == "month":
            query = """
                SELECT endpoint, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('month', current_timestamp) 
                GROUP BY endpoint 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "day":
            query = """
                SELECT endpoint, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('day', current_timestamp) 
                GROUP BY endpoint 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "hour":
            query = """
                SELECT endpoint, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('hour', current_timestamp) 
                GROUP BY endpoint 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "week":
            query = """
                SELECT endpoint, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('week', current_timestamp) 
                GROUP BY endpoint 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "year":
            query = """
                SELECT endpoint, COUNT(*) as request_count 
                FROM Log 
                WHERE timestamp >= date_trunc('year', current_timestamp) 
                GROUP BY endpoint 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        elif mode == "alltime":
            query = """
                SELECT endpoint, COUNT(*) as request_count 
                FROM Log 
                GROUP BY endpoint 
                ORDER BY request_count DESC 
                LIMIT ?
            """
        else:
            logger.warning(f"Invalid mode: {mode}")
            return {"error": "Invalid mode specified"}

        result = db.execute(query, [n]).fetchall()
        requests = [{"endpoint": row[0], "request_count": row[1]} for row in result]
        
        return {"requests": requests}

    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}