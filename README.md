# Fabric RTI Demo — Real-Time Intelligence with IoT Sensors

**Industrial IoT sensor monitoring** demo on Microsoft Fabric, built entirely via APIs (Python + REST).

## Scenario

A company operates **3 sites** (Paris HQ, Lyon Factory, Marseille Warehouse) with **60 sensors** spread across **12 zones**. Sensors measure Temperature, Humidity, Pressure, CO2, and Vibration.

- **Referential data** (sites, zones, sensors) lives in a **Lakehouse** as Delta tables
- **Real-time telemetry** (readings, alerts) streams into an **Eventhouse** via KQL ingestion
- An **EventStream** connects the data flow from a Custom App source to the Eventhouse

## Architecture

```
[Data Injector] ──► [EventStream (Custom App)] ──► [Eventhouse / KQL Database]
     │                                                    │
     │                                              SensorReading table
     │                                              SensorAlert table
     │
     └──► [Lakehouse]
              │
         dim_sites (Delta)
         dim_zones (Delta)
         dim_sensors (Delta)
```

## Project Structure

```
Fabric RTI Demo/
├── README.md
├── requirements.txt
├── data/
│   └── raw/              # Generated CSV files
│       ├── dim_sites.csv
│       ├── dim_zones.csv
│       └── dim_sensors.csv
├── docs/
│   └── fabric_setup.md   # Manual portal steps
├── src/
│   ├── config.yaml       # All configuration
│   ├── helpers.py        # Shared: auth, polling, Kusto mgmt
│   ├── state.json        # Auto-generated deployment state (IDs)
│   ├── generate_data.py  # Generate referential CSVs
│   ├── deploy_all.py     # Master orchestrator
│   ├── deploy_workspace.py
│   ├── deploy_lakehouse.py
│   ├── deploy_eventhouse.py
│   ├── deploy_eventstream.py
│   └── inject_data.py    # Real-time data injector
└── tasks/
    └── todo.md
```

## Quick Start

### 1. Prerequisites

```powershell
$env:PATH = "C:\Users\cdroinat\AppData\Local\Programs\Python\Python312;C:\Users\cdroinat\AppData\Local\Programs\Python\Python312\Scripts;$env:PATH"
pip install -r requirements.txt
az login
az account set --subscription "9b51a6b4-ec1a-4101-a3af-266c89e87a52"
```

### 2. Deploy everything

```powershell
cd src
python deploy_all.py
```

This runs all steps in order:
1. **generate_data.py** — Creates dim_sites.csv, dim_zones.csv, dim_sensors.csv
2. **deploy_workspace.py** — Creates Fabric workspace "CDR - Fabric RTI Demo"
3. **deploy_lakehouse.py** — Creates Lakehouse + uploads CSVs to OneLake
4. **deploy_eventhouse.py** — Creates Eventhouse + KQL tables (SensorReading, SensorAlert)
5. **deploy_eventstream.py** — Creates EventStream item

### 3. Start injecting real-time data

```powershell
# Continuous (Ctrl+C to stop)
python inject_data.py

# Run for 5 minutes
python inject_data.py --duration 300

# Inject 1000 historical readings and stop
python inject_data.py --batch-only 1000
```

### 4. Portal configuration

After deployment, open the Fabric portal to:
- Configure EventStream: add Custom App source → Eventhouse destination
- Create a Notebook to convert CSVs to Delta tables in the Lakehouse
- Build KQL Dashboard with real-time tiles

## Sensor Types

| Type | Unit | Normal Range | Critical Range |
|------|------|-------------|----------------|
| Temperature | °C | 18–28 | 5–45 |
| Humidity | %RH | 30–60 | 10–90 |
| Pressure | hPa | 990–1025 | 950–1060 |
| CO2 | ppm | 400–1000 | 300–5000 |
| Vibration | mm/s | 0–4.5 | 0–11 |

## KQL Tables

### SensorReading
| Column | Type | Description |
|--------|------|-------------|
| ReadingId | string | Unique reading ID |
| SensorId | string | FK to dim_sensors |
| ZoneId | string | FK to dim_zones |
| SiteId | string | FK to dim_sites |
| Timestamp | datetime | UTC timestamp |
| SensorType | string | Temperature, Humidity, etc. |
| ReadingValue | real | Measured value |
| Unit | string | °C, %RH, hPa, ppm, mm/s |
| IsAnomaly | bool | True if outside normal range |
| QualityFlag | string | Good / Suspect |

### SensorAlert
| Column | Type | Description |
|--------|------|-------------|
| AlertId | string | Unique alert ID |
| SensorId | string | FK to dim_sensors |
| ZoneId | string | FK to dim_zones |
| SiteId | string | FK to dim_sites |
| Timestamp | datetime | UTC timestamp |
| SensorType | string | Sensor type |
| AlertType | string | AboveThreshold / BelowThreshold |
| Severity | string | Warning / Critical |
| ReadingValue | real | Value that triggered the alert |
| ThresholdValue | real | Threshold that was exceeded |
| Message | string | Human-readable alert message |

## Data Injector Behavior

The injector simulates realistic sensor data:
- **Daily cycles**: Temperature follows a sinusoidal pattern (cooler at night, warmer during day)
- **Gaussian noise**: Small random variations around the daily pattern
- **Anomalies**: 3% of readings are anomalous (outside normal range)
- **Alerts**: A subset of anomalies generate SensorAlert entries with severity levels

## Brain Knowledge

This project uses the RTI agent knowledge base at `../Github_Brain/agents/rti-kusto-agent/`.
