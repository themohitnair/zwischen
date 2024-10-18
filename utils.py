from sqlmodel import select, Session
import ipaddress
import os
import geoip2.database
from geoip2.errors import AddressNotFoundError
from dotenv import load_dotenv
from models import LocationData
from typing import Optional

load_dotenv()
server_path = os.getenv("SERVER_PATH")
maxmind_license_key = os.getenv("MAXMIND_GEOIP_LICENSE")
geoip_db_path = os.path.join(server_path, "GeoLite2-City.mmdb")

def validate_ip(ip: str) -> bool:
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
    returns: Optional[LocationData]
    """
    if validate_ip(ip):
        try:
            with geoip2.database.Reader(geoip_db_path) as reader:
                response = reader.city(ip)

                return LocationData(
                    city=response.city.name if response.city.name else "Unknown",
                    country=response.country.name if response.country.name else "Unknown",
                    latitude=response.location.latitude if response.location.latitude else 0.0,
                    longitude=response.location.longitude if response.location.longitude else 0.0,
                )
        except AddressNotFoundError:
            print(f"IP address {ip} not found in the GeoLite2 database.")
            return None
        except Exception as e:
            print(f"Error retrieving geolocation: {e}")
            return None
    else:
        print("Invalid IP address")
        return None