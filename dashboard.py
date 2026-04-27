from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

DATA_PATH = Path("data/processed_weather.csv")

st.set_page_config(page_title="Real-Time Weather Dashboard", layout="wide")
st.title("Real-Time Data Pipeline Dashboard")

# Refresh every 20 seconds to simulate live monitoring.
st_autorefresh(interval=20_000, key="weather-refresh")

if not DATA_PATH.exists():
    st.info("No processed data yet. Start `python src/fetch_weather.py` and `python src/process_stream.py`.")
    st.stop()

df = pd.read_csv(DATA_PATH)
if df.empty:
    st.warning("Processed file is empty.")
    st.stop()

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")

latest = df.iloc[-1]
k1, k2, k3 = st.columns(3)
k1.metric("Latest Temperature (C)", f"{latest['temperature_c']:.1f}")
k2.metric("Latest Humidity (%)", f"{latest['humidity_percent']:.0f}")
k3.metric("Latest Wind (km/h)", f"{latest['wind_speed_kmh']:.1f}")

fig_temp = px.line(df, x="timestamp_utc", y=["temperature_c", "temp_rolling_avg_5"],
                   title="Temperature Trend")
st.plotly_chart(fig_temp, use_container_width=True)

fig_humidity = px.line(df, x="timestamp_utc", y=["humidity_percent", "humidity_rolling_avg_5"],
                       title="Humidity Trend")
st.plotly_chart(fig_humidity, use_container_width=True)

st.dataframe(df.tail(20), use_container_width=True)
