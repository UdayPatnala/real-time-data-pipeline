# Real-Time Data Pipeline

A streaming-style project that continuously ingests weather data, processes rolling metrics, and visualizes live trends.

## Stack

- Python
- Requests
- Pandas
- Streamlit

## Run

Open three terminals:

```bash
pip install -r requirements.txt
python src/fetch_weather.py
python src/process_stream.py
streamlit run dashboard.py
```

## What It Demonstrates

- Continuous ingestion loop
- Near real-time transformation
- Live dashboard updates
