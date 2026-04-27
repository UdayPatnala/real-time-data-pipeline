from __future__ import annotations

import logging
import os
import time

import pandas as pd

from config import POLL_INTERVAL_SECONDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

RAW_PATH = "data/raw_weather.csv"
PROCESSED_PATH = "data/processed_weather.csv"


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()
    transformed["timestamp_utc"] = pd.to_datetime(transformed["timestamp_utc"], errors="coerce")

    numeric_cols = ["temperature_c", "humidity_percent", "wind_speed_kmh"]
    for col in numeric_cols:
        transformed[col] = pd.to_numeric(transformed[col], errors="coerce")

    transformed = transformed.dropna(subset=["timestamp_utc"] + numeric_cols)
    transformed = transformed.sort_values("timestamp_utc")

    transformed["temp_rolling_avg_5"] = transformed["temperature_c"].rolling(window=5, min_periods=1).mean().round(2)
    transformed["humidity_rolling_avg_5"] = transformed["humidity_percent"].rolling(window=5, min_periods=1).mean().round(2)
    transformed["wind_rolling_avg_5"] = transformed["wind_speed_kmh"].rolling(window=5, min_periods=1).mean().round(2)

    return transformed


def main() -> None:
    os.makedirs("data", exist_ok=True)
    logger.info("Starting stream processor. Reading %s", RAW_PATH)

    while True:
        try:
            if os.path.exists(RAW_PATH):
                raw = pd.read_csv(RAW_PATH)
                if not raw.empty:
                    processed = compute_metrics(raw)
                    processed.to_csv(PROCESSED_PATH, index=False)
                    logger.info("Processed %d records", len(processed))
        except Exception as exc:  # noqa: BLE001
            logger.error("Processing failed: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
