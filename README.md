# 📱 Telecom Anomaly Detection System

A production-ready anomaly detection system for telecommunications Call Data Records (CDR). This system identifies unusual patterns in subscriber behavior, ranging from potential fraud to technical malfunctions.

## 🚀 How it Works

The system operates as an automated pipeline that transforms raw CDR data into actionable security insights:

1.  **Data Ingestion**: Reads CSV files from the `data/input` directory or direct CLI arguments.
2.  **Validation & Cleaning**: Filters out invalid records, parses dates/times, and categorizes calls (e.g., voice vs. SMS, service vs. subscriber).
3.  **Multi-Layered Detection**: Parallel analysis of each record across multiple specialized detector modules.
4.  **Anomaly Aggregation**: Combines findings per subscriber and per record, calculating confidence scores and eliminating duplicates.
5.  **Reporting**: Generates multiple output formats for different stakeholders (analysts, systems, executives).

---

## 🔍 Core Detection Modules

The system uses 6 primary detection strategies:

| Detector | Logic | Use Case |
| :--- | :--- | :--- |
| **Duration** | Monitors for "infinite calls" or "flash calls" | Fraud, billing errors |
| **Frequency** | Tracks calls per minute/hour/day | Automated dialing, SMS flooding |
| **SMS** | Specifically analyzes SMS volume and rate | Spam, bulk-SMS marketing |
| **Location** | Calculates velocity between consecutive activities | Account sharing, travel anomalies |
| **IMEI** | Tracks hardware-to-subscriber changes | SIM swapping, device theft |
| **Time Pattern** | Identifies activity during unusual (night) hours | Targeted attacks, outlier behavior |

---

## 🛠 Architecture

The project follows a modular orchestrator-based architecture:

-   **`TelecomAnomalyDetector` (Core)**: The central brain that coordinates the reader, validator, and all detector modules.
-   **`TelecomDataReader`**: Handles high-performance batch reading of large CSV files.
-   **`DataValidator`**: Ensures MSISDN formats, geographical coordinates, and timestamps are valid.
-   **`ResultExporter`**: Formats findings into JSON (for APIs), CSV (for Excel), and Markdown (for reports).

---

## 📊 Data Requirements

The system expects CSV files with the following core columns:

-   `msisdn`: Subscriber identification (e.g., `233XXXXXXXXX`)
-   `usage_type`: `VOICE` or `SMS`
-   `duration`: Call length in seconds
-   `latitude` / `longitude`: Geographical coordinates of the activity
-   `date` / `time`: Activity timestamps (`YYYY-MM-DD` and `HH:MM:SS`)
-   `imei`: Device hardware identifier

---

## ⚙️ Configuration

Detection sensitivity can be tuned in `config/settings.py` or via `TelecomConfig` overrides:

```python
DEFAULT_THRESHOLDS = {
    'extreme_call_threshold': 10800.0,  # 3 hours
    'max_calls_per_hour': 60,           # >1 call/minute
    'max_location_change_speed': 1000.0,# km/h (impossible travel)
    'night_hours_start': 23,            # Start of "unusual" hours
}
```

---

## 💻 Installation & Usage

### Setup with uv
The project uses [uv](https://github.com/astral-sh/uv) for high-performance dependency management.

```bash
# Clone and enter the repository
git clone https://github.com/yourusername/telecom-anomaly-detection.git
cd telecom-anomaly-detection

# Sync dependencies and create virtual environment
uv sync
```

### Running the Analysis
Place your CSV files in `data/input/` and run using `uv run`:
```bash
uv run python -m telecom_anomaly.main
```

Or specify files directly:
```bash
uv run python -m telecom_anomaly.main data/input/records_2023_10.csv
```

### Development
```bash
# Run tests
uv run pytest

# Format code
uv run black .

# Lint code
uv run flake8 .
```

## 🌐 API (FastAPI)

The project includes a FastAPI-based REST API for programmatic access to the anomaly detection engine.

### Running the API
```bash
uv run python -m uvicorn telecom_anomaly.api:app --reload
```
The API will be available at `http://127.0.0.1:8000`.

### API Documentation
Interactive documentation is available at:
- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`

### Key Endpoints

#### `POST /analyze`
Upload a CDR CSV file for real-time analysis.

**Example using curl:**
```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/analyze' \
  -H 'accept: application/json' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@data/input/sample_cdr.csv'
```

**Response Example:**
```json
{
  "filename": "sample_cdr.csv",
  "statistics": {
    "total_rows": 1000,
    "rows_with_anomalies": 15,
    "total_anomalies_detected": 22,
    "anomaly_rate": 1.5,
    "unique_subscribers": 12,
    "anomaly_types": {
      "short_duration": 10,
      "impossible_speed": 5
    }
  },
  "anomalies": [
    {
      "row_index": 42,
      "column_name": "duration",
      "value": 1.2,
      "detection_method": "short_duration",
      "score": 2.5,
      "threshold": 3.0,
      "confidence": 0.75,
      "msisdn": "233123456789"
    }
  ]
}
```

#### `GET /health`
Returns the health status of the API.