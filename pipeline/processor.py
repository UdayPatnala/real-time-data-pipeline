import os
import threading
import time
import pandas as pd
from typing import Optional

from pipeline.config import config
from pipeline.ingestor import _INGEST_LOCK
from pipeline.utils.logger import setup_logger

logger = setup_logger("Processor")

_PROCESS_LOCK = threading.RLock()


class StreamProcessor:
    """Handles transformation and metric computation of weather data with thread safety and atomic output writes."""

    def __init__(self, raw_path: Optional[str] = None, processed_path: Optional[str] = None):
        self.raw_path = raw_path or config.RAW_DATA_PATH
        self.processed_path = processed_path or config.PROCESSED_DATA_PATH

    def process(self, max_retries: int = 5, retry_delay: float = 0.05) -> Optional[pd.DataFrame]:
        """Reads raw data, computes rolling metrics, and atomically saves the result."""
        with _PROCESS_LOCK, _INGEST_LOCK:
            if not os.path.exists(self.raw_path):
                logger.warning(f"Raw data file not found at {self.raw_path}. Skipping processing.")
                return None

            df = None
            for attempt in range(max_retries):
                try:
                    df = pd.read_csv(self.raw_path)
                    break
                except (PermissionError, OSError, pd.errors.EmptyDataError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Failed to read raw data from {self.raw_path}: {e}")
                        return None

            if df is None or df.empty:
                return None

            # Type conversion and cleaning
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
            numeric_cols = ["temperature_c", "humidity_percent", "wind_speed_kmh"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.dropna(subset=["timestamp_utc"] + numeric_cols)
            if df.empty:
                return None

            df = df.sort_values("timestamp_utc")

            # Rolling window computations (5-point average)
            window_size = 5
            df["temp_rolling_avg"] = (
                df["temperature_c"].rolling(window=window_size, min_periods=1).mean().round(2)
            )
            df["humidity_rolling_avg"] = (
                df["humidity_percent"].rolling(window=window_size, min_periods=1).mean().round(2)
            )
            df["wind_rolling_avg"] = (
                df["wind_speed_kmh"].rolling(window=window_size, min_periods=1).mean().round(2)
            )

            # Save processed data atomically using a temporary file
            proc_dir = os.path.dirname(self.processed_path) or "."
            os.makedirs(proc_dir, exist_ok=True)
            temp_path = os.path.join(
                proc_dir, f".tmp_proc_{os.getpid()}_{threading.get_ident()}.csv"
            )

            try:
                df.to_csv(temp_path, index=False)
                os.replace(temp_path, self.processed_path)
                logger.info(f"Processed {len(df)} records and updated {self.processed_path}")
                return df
            except Exception as e:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass
                logger.error(f"Failed to save processed file to {self.processed_path}: {e}")
                return None

    def run_once(self) -> None:
        """Executes a single processing cycle."""
        self.process()
