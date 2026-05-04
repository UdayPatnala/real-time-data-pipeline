import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    """Configuration settings for the Real-Time Data Pipeline."""
    CITY_NAME: str = os.getenv("CITY_NAME", "Bengaluru")
    LATITUDE: float = float(os.getenv("LATITUDE", 12.9716))
    LONGITUDE: float = float(os.getenv("LONGITUDE", 77.5946))
    POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL", 20))
    
    RAW_DATA_PATH: str = "data/raw_weather.csv"
    PROCESSED_DATA_PATH: str = "data/processed_weather.csv"
    
    # API Settings
    BASE_URL: str = "https://api.open-meteo.com/v1/forecast"
    
    @property
    def api_url(self) -> str:
        return (
            f"{self.BASE_URL}?latitude={self.LATITUDE}&longitude={self.LONGITUDE}"
            "&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
        )

config = Config()
