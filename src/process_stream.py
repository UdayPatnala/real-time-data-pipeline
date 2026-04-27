from __future__ import annotations

import os
import time

import pandas as pd

from config import POLL_INTERVAL_SECONDS

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

    transformed["temp_rolling_avg_5"] = transformed["temperature_c"].rolling(window=5, min_periods=1).mean()
    transformed["humidity_rolling_avg_5"] = transformed["humidity_percent"].rolling(window=5, min_periods=1).mean()

    return transformed


def main() -> None:
    os.makedirs("data", exist_ok=True)
    print(f"Starting stream processor. Reading {RAW_PATH}")

    while True:
        try:
            if os.path.exists(RAW_PATH):
                raw = pd.read_csv(RAW_PATH)
                if not raw.empty:
                    processed = compute_metrics(raw)
                    processed.to_csv(PROCESSED_PATH, index=False)
                    print(f"Processed {len(processed)} records")
        except Exception as exc:  # noqa: BLE001
            print("Processing failed:", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
