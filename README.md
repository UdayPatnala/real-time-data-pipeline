# SkyFlow: Real-Time Weather Data Pipeline

SkyFlow is a professional-grade, end-to-end data pipeline designed to demonstrate real-time data ingestion, stream processing, and interactive visualization. It utilizes a modular architecture to fetch live weather data from the Open-Meteo API, process it using rolling window aggregations with Pandas, and serve insights via a premium Streamlit dashboard.

## 🏗️ Architecture

The project follows a decoupled producer-consumer pattern:

1.  **Ingestor**: Continuously polls the Open-Meteo API and appends raw data to an immutable storage (CSV).
2.  **Processor**: Monitors the raw data stream, performs cleaning, and computes real-time metrics (e.g., 5-point rolling averages).
3.  **Dashboard**: Provides a high-fidelity visual interface with auto-refreshing metrics and trend analysis.

## 🚀 Features

-   **Modular Design**: Clean separation of concerns (Ingestor, Processor, Dashboard).
-   **Robust Logging**: Structured logging for observability across all components.
-   **Environment Driven**: Configuration via `.env` for security and flexibility.
-   **Premium Visualization**: Interactive Plotly charts and custom-styled Streamlit UI.
-   **Type Safety**: Comprehensive use of Python type hints and dataclasses.

## 🛠️ Tech Stack

-   **Language**: Python 3.10+
-   **Data Ingestion**: Requests, Open-Meteo API
-   **Stream Processing**: Pandas (Rolling Windows)
-   **Visualization**: Streamlit, Plotly
-   **Configuration**: Dotenv

## 🚦 Getting Started

### 1. Setup Environment
```bash
pip install -r requirements.txt
cp .env.example .env
```

### 2. Run the Pipeline
Open three separate terminals to simulate the distributed nature of the pipeline:

```bash
# Terminal 1: Data Ingestion
python main.py ingest

# Terminal 2: Stream Processing
python main.py process

# Terminal 3: Live Dashboard
streamlit run dashboard/app.py
```

## 🧪 Testing
Run unit tests to ensure processing logic integrity:
```bash
python -m unittest discover tests
```

## 📂 Project Structure
```text
├── pipeline/             # Core logic package
│   ├── config.py         # Environment-based settings
│   ├── ingestor.py       # Data extraction logic
│   ├── processor.py      # Transformation & metrics
│   └── utils/            # Shared utilities (logging)
├── dashboard/            # Streamlit application
├── tests/                # Unit test suite
├── main.py               # Unified CLI entry point
├── requirements.txt      # Dependency manifest
└── .env.example          # Environment template
```

## 📝 License
MIT
