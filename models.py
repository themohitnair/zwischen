from pydantic import BaseModel

class LocationData(BaseModel):
    city: str = "Unknown"
    country: str = "Unknown"
    latitude: float = 0.0
    longitude: float = 0.0

class Log(BaseModel):
    id: int = Field(default=None, primary_key=True)
    timestamp: str
    method: str
    ip: str    
    city: str
    country: str
    latitude: float
    longitude: float
    url: str 
    status_code: int
    browser: str
    referrer: str