import csv
import os
import requests
from datetime import datetime, timezone
from typing import Dict, Any

from pipeline.config import config
from pipeline.utils.logger import setup_logger

logger = setup_logger("Ingestor")

class WeatherIngestor:
    """Handles the extraction of weather data from the Open-Meteo API."""
    
    def __init__(self):
        self.output_path = config.RAW_DATA_PATH
        self._ensure_output_dir()
        self._initialize_csv()

    def _ensure_output_dir(self) -> None:
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    def _initialize_csv(self) -> None:
        if not os.path.exists(self.output_path):
            with open(self.output_path, "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow([
                    "timestamp_utc",
                    "city",
                    "temperature_c",
                    "humidity_percent",
                    "wind_speed_kmh",
                ])
            logger.info(f"Initialized raw data storage at {self.output_path}")

    def fetch_current_weather(self) -> Dict[str, Any]:
        """Fetches the latest weather snapshot."""
        try:
            response = requests.get(config.api_url, timeout=15)
            response.raise_for_status()
            data = response.json().get("current", {})
            
            return {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "city": config.CITY_NAME,
                "temperature_c": data.get("temperature_2m"),
                "humidity_percent": data.get("relative_humidity_2m"),
                "wind_speed_kmh": data.get("wind_speed_10m"),
            }
        except requests.RequestException as e:
            logger.error(f"API Request failed: {e}")
            raise

    def save_data(self, data: Dict[str, Any]) -> None:
        """Appends the weather record to the CSV file."""
        with open(self.output_path, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=data.keys())
            writer.writerow(data)
        logger.info(f"Ingested record for {config.CITY_NAME}: {data['temperature_c']}°C")

    def run_once(self) -> None:
        """Executes a single ingestion cycle."""
        try:
            data = self.fetch_current_weather()
            self.save_data(data)
        except Exception as e:
            logger.error(f"Ingestion cycle failed: {e}")
