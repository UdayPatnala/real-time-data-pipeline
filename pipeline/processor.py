import os
import pandas as pd
from typing import Optional

from pipeline.config import config
from pipeline.utils.logger import setup_logger

logger = setup_logger("Processor")

class StreamProcessor:
    """Handles the transformation and metric computation of weather data."""
    
    def __init__(self):
        self.raw_path = config.RAW_DATA_PATH
        self.processed_path = config.PROCESSED_DATA_PATH

    def process(self) -> Optional[pd.DataFrame]:
        """Reads raw data, computes rolling metrics, and saves the result."""
        if not os.path.exists(self.raw_path):
            logger.warning("Raw data file not found. Skipping processing.")
            return None

        try:
            df = pd.read_csv(self.raw_path)
            if df.empty:
                return None

            # Type conversion and cleaning
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
            numeric_cols = ["temperature_c", "humidity_percent", "wind_speed_kmh"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.dropna(subset=["timestamp_utc"] + numeric_cols)
            df = df.sort_values("timestamp_utc")

            # Rolling window computations (5-point average)
            window_size = 5
            df["temp_rolling_avg"] = df["temperature_c"].rolling(window=window_size, min_periods=1).mean().round(2)
            df["humidity_rolling_avg"] = df["humidity_percent"].rolling(window=window_size, min_periods=1).mean().round(2)
            df["wind_rolling_avg"] = df["wind_speed_kmh"].rolling(window=window_size, min_periods=1).mean().round(2)

            # Save processed data
            df.to_csv(self.processed_path, index=False)
            logger.info(f"Processed {len(df)} records and updated {self.processed_path}")
            return df

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return None

    def run_once(self) -> None:
        """Executes a single processing cycle."""
        self.process()
