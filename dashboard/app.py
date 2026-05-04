import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pathlib import Path

# Configuration
PROCESSED_DATA_PATH = Path("data/processed_weather.csv")

st.set_page_config(
    page_title="SkyFlow | Real-Time Weather Insights",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Premium Look
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #161b22;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #30363d;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🌤️ SkyFlow: Real-Time Weather Pipeline")
st.markdown("---")

# Auto-refresh every 20 seconds
st_autorefresh(interval=20_000, key="data-refresh")

def load_data():
    if not PROCESSED_DATA_PATH.exists():
        return None
    df = pd.read_csv(PROCESSED_DATA_PATH)
    if df.empty:
        return None
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    return df

data = load_data()

if data is None:
    st.info("Waiting for data... Please ensure the Ingestor and Processor are running.")
    st.code("python main.py ingest\npython main.py process")
    st.stop()

# Header Metrics
latest = data.iloc[-1]
m1, m2, m3, m4 = st.columns(4)

m1.metric("Temperature", f"{latest['temperature_c']} °C", f"{latest['temperature_c'] - data.iloc[-2]['temperature_c']:.1f} °C" if len(data) > 1 else None)
m2.metric("Humidity", f"{latest['humidity_percent']} %", f"{latest['humidity_percent'] - data.iloc[-2]['humidity_percent']:.0f} %" if len(data) > 1 else None)
m3.metric("Wind Speed", f"{latest['wind_speed_kmh']} km/h", f"{latest['wind_speed_kmh'] - data.iloc[-2]['wind_speed_kmh']:.1f} km/h" if len(data) > 1 else None)
m4.metric("City", latest['city'])

st.markdown("### Trends & Analysis")

col1, col2 = st.columns(2)

with col1:
    fig_temp = px.line(
        data, 
        x="timestamp_utc", 
        y=["temperature_c", "temp_rolling_avg"],
        title="Temperature Trend (Real-time vs Rolling Avg)",
        template="plotly_dark",
        color_discrete_sequence=["#ff4b4b", "#00d4ff"]
    )
    fig_temp.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_temp, use_container_width=True)

with col2:
    fig_humidity = px.area(
        data, 
        x="timestamp_utc", 
        y="humidity_percent",
        title="Humidity Levels Over Time",
        template="plotly_dark",
        color_discrete_sequence=["#00ff9d"]
    )
    fig_humidity.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_humidity, use_container_width=True)

st.markdown("### Raw Stream Data")
st.dataframe(data.sort_values("timestamp_utc", ascending=False).head(10), use_container_width=True)
