from sqlmodel import select, Session
import ipaddress
import os
import geoip2.database
from dotenv import load_dotenv
from models import LocationData
from typing import Optional

load_dotenv()
server_path = os.getenv("SERVER_PATH")
maxmind_license_key = os.getenv("MAXMIND_GEOIP_LICENSE")
geoip_db_path = os.path.join(server_path, "GeoLite2-City.mmdb")

async def validate_ip(ip: str) -> bool:
    """
    Validates an IP string.

    args: ip (str)
    returns: bool
    """
    try:
        ip_obj = ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False

async def retrieve_geoloc(ip: str) -> Optional[LocationData]:
    """
    Retrieves IP location details like longitude, latitude, city, and country from a given IP.

    args: ip (str)
    returns: bool
    """
    if validate_ip(ip):
        try:
            with geoip2.database.Reader(geoip_db_path) as reader:
                response = reader.city(ip)

                return {
                    "city": response.city.name,
                    "country": response.country.name,
                    "latitude": response.location.latitude,
                    "longitude": response.location.longitude,
                }
        except Exception as e:
            print(f"Error retrieving geolocation: {e}")
            return None
    else:
        print("Invalid IP address")
        return None