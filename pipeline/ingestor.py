import csv
import json
import os
import threading
import time
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pipeline.config import config
from pipeline.utils.logger import setup_logger

logger = setup_logger("Ingestor")

# Process/Thread-wide reentrant lock for raw data ingestion
_INGEST_LOCK = threading.RLock()


class WeatherIngestor:
    """Handles the extraction and thread-safe persistence of weather data."""

    def __init__(self, output_path: Optional[str] = None):
        self.output_path = output_path or config.RAW_DATA_PATH
        self._ensure_output_dir()
        self._initialize_csv()

    def _ensure_output_dir(self) -> None:
        dir_name = os.path.dirname(self.output_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

    def _initialize_csv(self) -> None:
        with _INGEST_LOCK:
            if not os.path.exists(self.output_path):
                dir_name = os.path.dirname(self.output_path) or "."
                temp_file = os.path.join(
                    dir_name, f".tmp_init_{os.getpid()}_{threading.get_ident()}.csv"
                )
                try:
                    with open(temp_file, "w", newline="", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        writer.writerow([
                            "timestamp_utc",
                            "city",
                            "temperature_c",
                            "humidity_percent",
                            "wind_speed_kmh",
                        ])
                    os.replace(temp_file, self.output_path)
                    logger.info(f"Initialized raw data storage at {self.output_path}")
                except Exception as e:
                    if os.path.exists(temp_file):
                        try:
                            os.remove(temp_file)
                        except OSError:
                            pass
                    logger.error(f"Failed to initialize CSV at {self.output_path}: {e}")
                    raise

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

    def save_data(
        self, data: Dict[str, Any], max_retries: int = 5, retry_delay: float = 0.05
    ) -> None:
        """Appends the weather record to the CSV file safely using thread locking and retries."""
        with _INGEST_LOCK:
            self._ensure_output_dir()
            if not os.path.exists(self.output_path) or os.path.getsize(self.output_path) == 0:
                self._initialize_csv()

            fieldnames = [
                "timestamp_utc",
                "city",
                "temperature_c",
                "humidity_percent",
                "wind_speed_kmh",
            ]

            for attempt in range(max_retries):
                try:
                    with open(self.output_path, "a", newline="", encoding="utf-8") as file:
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writerow(data)
                        file.flush()
                        os.fsync(file.fileno())
                    logger.info(
                        f"Ingested record for {data.get('city', config.CITY_NAME)}: {data.get('temperature_c')}°C"
                    )
                    return
                except (PermissionError, OSError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Failed to write data after {max_retries} attempts: {e}")
                        raise

    def save_data_json(self, data: Dict[str, Any], json_path: Optional[str] = None) -> None:
        """Appends record to a JSON stream file atomically."""
        target_path = json_path or self.output_path.replace(".csv", ".json")
        with _INGEST_LOCK:
            dir_name = os.path.dirname(target_path) or "."
            os.makedirs(dir_name, exist_ok=True)
            records = []
            if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
                try:
                    with open(target_path, "r", encoding="utf-8") as f:
                        records = json.load(f)
                except Exception:
                    records = []

            records.append(data)
            temp_path = os.path.join(
                dir_name, f".tmp_json_{os.getpid()}_{threading.get_ident()}.json"
            )
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)
            os.replace(temp_path, target_path)

    def run_once(self) -> None:
        """Executes a single ingestion cycle."""
        try:
            data = self.fetch_current_weather()
            self.save_data(data)
        except Exception as e:
            logger.error(f"Ingestion cycle failed: {e}")
