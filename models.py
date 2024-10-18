from pydantic import BaseModel

class LocationData(BaseModel):
    city: str
    country: str
    latitude: str
    longitude: str
    isp: str