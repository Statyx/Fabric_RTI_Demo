"""Inject a large batch of sensor data and verify counts."""
import time
import random
import math
import uuid
import csv
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from helpers import load_config, load_state, get_kusto_token, kusto_mgmt

state = load_state()
config = load_config()
query_uri = state["query_service_uri"]
db_name = config["eventhouse_name"]
token = get_kusto_token(query_uri)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Load sensors
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
sensors = []
with open(DATA_DIR / "dim_sensors.csv", "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        sensors.append({
            "sensor_id": row["sensor_id"],
            "sensor_type": row["sensor_type"],
            "unit": row["unit"],
            "zone_id": row["zone_id"],
            "site_id": row["site_id"],
            "min_normal": float(row["min_normal"]),
            "max_normal": float(row["max_normal"]),
            "min_critical": float(row["min_critical"]),
            "max_critical": float(row["max_critical"]),
        })

print(f"Loaded {len(sensors)} sensors")

# Check counts before
for tbl in ["SensorReading", "SensorAlert"]:
    r = requests.post(f"{query_uri}/v1/rest/query", headers=headers,
        json={"db": db_name, "csl": f"{tbl} | count"}, timeout=30)
    cnt = r.json()["Tables"][0]["Rows"][0][0] if r.status_code == 200 else "?"
    print(f"  {tbl} BEFORE: {cnt}")

# Generate 2000 readings spread over the last 7 days
TARGET_READINGS = 2000
ANOMALY_PCT = 0.05
now = datetime.now(timezone.utc)
readings = []
alerts = []

print(f"\nGenerating {TARGET_READINGS} readings over 7 days...")
for i in range(TARGET_READINGS):
    sensor = random.choice(sensors)
    # Spread timestamps over the last 7 days
    ts = now - timedelta(seconds=random.randint(0, 7 * 86400))
    
    is_anomaly = random.random() < ANOMALY_PCT
    min_n, max_n = sensor["min_normal"], sensor["max_normal"]
    mid = (min_n + max_n) / 2
    spread = (max_n - min_n) / 2

    if is_anomaly:
        if random.random() < 0.5:
            value = random.uniform(sensor["min_critical"], min_n * 0.95)
        else:
            value = random.uniform(max_n * 1.05, sensor["max_critical"])
        quality = "Suspect"
    else:
        t_val = ts.timestamp()
        daily_phase = math.sin(2 * math.pi * (t_val % 86400) / 86400)
        noise = random.gauss(0, spread * 0.15)
        value = mid + daily_phase * spread * 0.4 + noise
        value = max(min_n * 0.98, min(max_n * 1.02, value))
        quality = "Good"

    reading = {
        "ReadingId": str(uuid.uuid4())[:12],
        "SensorId": sensor["sensor_id"],
        "ZoneId": sensor["zone_id"],
        "SiteId": sensor["site_id"],
        "Timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "SensorType": sensor["sensor_type"],
        "ReadingValue": round(value, 2),
        "Unit": sensor["unit"],
        "IsAnomaly": str(is_anomaly).lower(),
        "QualityFlag": quality,
    }
    readings.append(reading)

    if is_anomaly and random.random() < 0.8:
        v = reading["ReadingValue"]
        if v < min_n:
            alert_type = "BelowThreshold"
            threshold = min_n
            severity = "Critical" if v < sensor["min_critical"] * 1.1 else "Warning"
            msg = f"{sensor['sensor_type']} too low"
        else:
            alert_type = "AboveThreshold"
            threshold = max_n
            severity = "Critical" if v > sensor["max_critical"] * 0.9 else "Warning"
            msg = f"{sensor['sensor_type']} too high"

        alerts.append({
            "AlertId": str(uuid.uuid4())[:12],
            "SensorId": reading["SensorId"],
            "ZoneId": reading["ZoneId"],
            "SiteId": reading["SiteId"],
            "Timestamp": reading["Timestamp"],
            "SensorType": reading["SensorType"],
            "AlertType": alert_type,
            "Severity": severity,
            "ReadingValue": reading["ReadingValue"],
            "ThresholdValue": round(threshold, 2),
            "Message": msg,
        })

print(f"  Generated: {len(readings)} readings, {len(alerts)} alerts")

# Ingest in batches of 50
BATCH = 50
errors = 0
t0 = time.time()

print(f"\nIngesting readings ({len(readings)} rows, batch size {BATCH})...")
for i in range(0, len(readings), BATCH):
    batch = readings[i:i + BATCH]
    lines = []
    for r in batch:
        lines.append(",".join([
            r["ReadingId"], r["SensorId"], r["ZoneId"], r["SiteId"],
            r["Timestamp"], r["SensorType"], str(r["ReadingValue"]),
            r["Unit"], r["IsAnomaly"], r["QualityFlag"],
        ]))
    inline_data = "\n".join(lines)
    cmd = f".ingest inline into table SensorReading with (format='csv') <|\n{inline_data}"
    try:
        kusto_mgmt(query_uri, token, db_name, cmd)
        print(f"\r  Batch {i//BATCH + 1}/{(len(readings)-1)//BATCH + 1} OK ({i+len(batch)}/{len(readings)})", end="", flush=True)
    except Exception as e:
        errors += 1
        print(f"\n  ERROR batch {i//BATCH + 1}: {e}")

print(f"\nIngesting alerts ({len(alerts)} rows)...")
for i in range(0, len(alerts), BATCH):
    batch = alerts[i:i + BATCH]
    lines = []
    for a in batch:
        msg = a["Message"].replace('"', '""')
        lines.append(",".join([
            a["AlertId"], a["SensorId"], a["ZoneId"], a["SiteId"],
            a["Timestamp"], a["SensorType"], a["AlertType"],
            a["Severity"], str(a["ReadingValue"]),
            str(a["ThresholdValue"]), f'"{msg}"',
        ]))
    inline_data = "\n".join(lines)
    cmd = f".ingest inline into table SensorAlert with (format='csv') <|\n{inline_data}"
    try:
        kusto_mgmt(query_uri, token, db_name, cmd)
    except Exception as e:
        errors += 1
        print(f"  ERROR alert batch: {e}")

elapsed = time.time() - t0
print(f"\nDone in {elapsed:.1f}s ({errors} errors)")

# Wait for data to settle
time.sleep(5)

# Check counts after
for tbl in ["SensorReading", "SensorAlert"]:
    r = requests.post(f"{query_uri}/v1/rest/query", headers=headers,
        json={"db": db_name, "csl": f"{tbl} | count"}, timeout=30)
    cnt = r.json()["Tables"][0]["Rows"][0][0] if r.status_code == 200 else "?"
    print(f"  {tbl} AFTER: {cnt}")

# Show distribution
for q_label, q in [
    ("By Site", "SensorReading | summarize count() by SiteId | order by count_ desc"),
    ("By Type", "SensorReading | summarize count() by SensorType | order by count_ desc"),
    ("Anomalies", "SensorReading | summarize total=count(), anomalies=countif(IsAnomaly) | extend rate=round(100.0*anomalies/total, 1)"),
    ("Alerts", "SensorAlert | summarize count() by Severity"),
]:
    r = requests.post(f"{query_uri}/v1/rest/query", headers=headers,
        json={"db": db_name, "csl": q}, timeout=30)
    if r.status_code == 200:
        t = r.json()["Tables"][0]
        cols = [c["ColumnName"] for c in t["Columns"]]
        print(f"\n  {q_label}: {cols}")
        for row in t["Rows"]:
            print(f"    {row}")
