# Real-Time Weather Pipeline

A streaming-style data pipeline that continuously ingests live weather data from the Open-Meteo API, computes rolling metrics with Pandas, and visualizes trends in a self-refreshing Streamlit dashboard.

## Tech Stack

- **Python 3.10+**
- **Requests** — API data ingestion
- **Pandas** — stream processing & rolling aggregations
- **Streamlit + Plotly** — live dashboard
- **Open-Meteo API** — free weather data (no API key needed)

## Architecture

```
  Open-Meteo API
       │  every 20s
       ▼
  ┌──────────────────┐
  │  fetch_weather.py │ ──▶  data/raw_weather.csv
  └──────────────────┘
                                   │
                                   ▼
                         ┌────────────────────┐
                         │ process_stream.py   │ ──▶ data/processed_weather.csv
                         │ (rolling averages)  │
                         └────────────────────┘
                                   │
                                   ▼
                         ┌────────────────────┐
                         │   dashboard.py      │
                         │  (Streamlit live)   │
                         └────────────────────┘
```

## Setup & Run

```bash
pip install -r requirements.txt
```

Open **three terminals** and run:

```bash
# Terminal 1 — Ingestion
python src/fetch_weather.py

# Terminal 2 — Processing
python src/process_stream.py

# Terminal 3 — Dashboard
streamlit run dashboard.py
```

The dashboard auto-refreshes every 20 seconds.

## Configuration

Edit `src/config.py` to change the target city:

```python
CITY_NAME = "Bengaluru"
LATITUDE = 12.9716
LONGITUDE = 77.5946
POLL_INTERVAL_SECONDS = 20
```

## Metrics Computed

| Metric | Description |
|--------|-------------|
| `temperature_c` | Current temperature in Celsius |
| `humidity_percent` | Relative humidity % |
| `wind_speed_kmh` | Wind speed in km/h |
| `temp_rolling_avg_5` | 5-point rolling average of temperature |
| `humidity_rolling_avg_5` | 5-point rolling average of humidity |
| `wind_rolling_avg_5` | 5-point rolling average of wind speed |

## Project Structure

```
├── src/
│   ├── config.py               # City coordinates & poll interval
│   ├── fetch_weather.py        # Continuous API ingestion loop
│   └── process_stream.py       # Rolling metric computation
├── data/                        # CSV files (generated at runtime)
├── dashboard.py                # Streamlit live dashboard
├── requirements.txt
└── README.md
```

## Key Concepts

- **Continuous ingestion loop** with error recovery
- **Near real-time transformation** on append-only CSV
- **Rolling window aggregations** for trend smoothing
- **Live dashboard** with auto-refresh (no manual reload)

## License

MIT
