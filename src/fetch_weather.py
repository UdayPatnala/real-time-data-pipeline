from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timezone

import requests

from config import CITY_NAME, LATITUDE, LONGITUDE, POLL_INTERVAL_SECONDS

OUTPUT_PATH = "data/raw_weather.csv"
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LATITUDE}&longitude={LONGITUDE}&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
)


def ensure_file(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([
                "timestamp_utc",
                "city",
                "temperature_c",
                "humidity_percent",
                "wind_speed_kmh",
            ])


def fetch_snapshot() -> dict:
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    data = response.json().get("current", {})

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "city": CITY_NAME,
        "temperature_c": data.get("temperature_2m"),
        "humidity_percent": data.get("relative_humidity_2m"),
        "wind_speed_kmh": data.get("wind_speed_10m"),
    }


def append_row(path: str, row: dict) -> None:
    with open(path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "timestamp_utc",
                "city",
                "temperature_c",
                "humidity_percent",
                "wind_speed_kmh",
            ],
        )
        writer.writerow(row)


def main() -> None:
    ensure_file(OUTPUT_PATH)
    print(f"Starting weather ingestion for {CITY_NAME}. Writing to {OUTPUT_PATH}")

    while True:
        try:
            row = fetch_snapshot()
            append_row(OUTPUT_PATH, row)
            print("Ingested:", row)
        except Exception as exc:  # noqa: BLE001
            print("Fetch failed:", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
