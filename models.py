from pydantic import BaseModel

class LocationData(BaseModel):
    city: str = "Unknown"
    country: str = "Unknown"
    latitude: float = 0.0
    longitude: float = 0.0