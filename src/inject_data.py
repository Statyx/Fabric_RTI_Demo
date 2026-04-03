#!/usr/bin/env python3
"""
Real-time sensor data injector.
Generates simulated IoT sensor readings and streams them into the
Eventhouse KQL tables via Kusto streaming ingestion.

Usage:
    python inject_data.py                     # Run continuously
    python inject_data.py --duration 300      # Run for 5 minutes
    python inject_data.py --batch-only 1000   # Inject 1000 readings and stop
"""

import argparse
import csv
import math
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from helpers import (
    load_config, load_state, get_kusto_token, kusto_streaming_ingest,
)

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data" / "raw"


def load_sensors() -> List[Dict]:
    """Load sensor definitions from the referential CSV."""
    sensors = []
    csv_path = DATA_DIR / "dim_sensors.csv"
    if not csv_path.exists():
        print("❌ dim_sensors.csv not found. Run generate_data.py first.")
        sys.exit(1)
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
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
    return sensors


def generate_reading(sensor: Dict, timestamp: datetime,
                     anomaly_pct: float) -> Dict:
    """Generate a single sensor reading with optional anomaly."""
    is_anomaly = random.random() < anomaly_pct
    min_n, max_n = sensor["min_normal"], sensor["max_normal"]
    mid = (min_n + max_n) / 2
    spread = (max_n - min_n) / 2

    if is_anomaly:
        # Anomaly: reading outside normal range
        if random.random() < 0.5:
            value = random.uniform(sensor["min_critical"], min_n * 0.95)
        else:
            value = random.uniform(max_n * 1.05, sensor["max_critical"])
        quality = "Suspect"
    else:
        # Normal: sinusoidal base + noise for realistic patterns
        t = timestamp.timestamp()
        # Daily cycle (temperature rises during day, drops at night)
        daily_phase = math.sin(2 * math.pi * (t % 86400) / 86400)
        # Small random walk
        noise = random.gauss(0, spread * 0.15)
        value = mid + daily_phase * spread * 0.4 + noise
        value = max(min_n * 0.98, min(max_n * 1.02, value))
        quality = "Good"

    return {
        "ReadingId": str(uuid.uuid4())[:12],
        "SensorId": sensor["sensor_id"],
        "ZoneId": sensor["zone_id"],
        "SiteId": sensor["site_id"],
        "Timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "SensorType": sensor["sensor_type"],
        "ReadingValue": round(value, 2),
        "Unit": sensor["unit"],
        "IsAnomaly": str(is_anomaly).lower(),
        "QualityFlag": quality,
    }


def generate_alert(reading: Dict, sensor: Dict) -> Dict:
    """Generate an alert from an anomalous reading."""
    value = reading["ReadingValue"]
    if value < sensor["min_normal"]:
        alert_type = "BelowThreshold"
        threshold = sensor["min_normal"]
        severity = "Critical" if value < sensor["min_critical"] * 1.1 else "Warning"
        msg = f"{sensor['sensor_type']} too low: {value} {sensor['unit']} (min: {threshold})"
    else:
        alert_type = "AboveThreshold"
        threshold = sensor["max_normal"]
        severity = "Critical" if value > sensor["max_critical"] * 0.9 else "Warning"
        msg = f"{sensor['sensor_type']} too high: {value} {sensor['unit']} (max: {threshold})"

    return {
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
    }


def _stream_csv_batched(lines: List[str], query_uri: str, kusto_token: str,
                        db_name: str, table_name: str, batch_size: int = 200):
    """Send CSV lines via streaming ingestion in batches with retry."""
    for i in range(0, len(lines), batch_size):
        batch = lines[i:i + batch_size]
        csv_payload = "\n".join(batch)
        for attempt in range(3):
            try:
                kusto_streaming_ingest(query_uri, kusto_token, db_name,
                                       table_name, csv_payload)
                break
            except Exception:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                else:
                    raise


def ingest_batch(readings: List[Dict], alerts: List[Dict],
                 query_uri: str, kusto_token: str, db_name: str):
    """Ingest a batch of readings and alerts via streaming ingestion API."""
    if readings:
        lines = []
        for r in readings:
            lines.append(",".join([
                r["ReadingId"], r["SensorId"], r["ZoneId"], r["SiteId"],
                r["Timestamp"], r["SensorType"], str(r["ReadingValue"]),
                r["Unit"], r["IsAnomaly"], r["QualityFlag"],
            ]))
        _stream_csv_batched(lines, query_uri, kusto_token, db_name,
                            "SensorReading")

    if alerts:
        lines = []
        for a in alerts:
            msg = a["Message"].replace('"', '""')
            lines.append(",".join([
                a["AlertId"], a["SensorId"], a["ZoneId"], a["SiteId"],
                a["Timestamp"], a["SensorType"], a["AlertType"],
                a["Severity"], str(a["ReadingValue"]),
                str(a["ThresholdValue"]), f'"{msg}"',
            ]))
        _stream_csv_batched(lines, query_uri, kusto_token, db_name,
                            "SensorAlert")


def run_continuous(sensors: List[Dict], config: dict,
                   query_uri: str, kusto_token: str,
                   db_name: str, duration: int = 0):
    """Run continuous data injection."""
    interval = config["streaming"]["interval_seconds"]
    anomaly_pct = config["streaming"]["anomaly_pct"]
    batch_size = config["streaming"]["batch_size"]
    alert_threshold = config["streaming"]["alert_threshold_pct"]

    total_readings = 0
    total_alerts = 0
    start_time = time.time()

    print(f"\n🔴 LIVE — Injecting data every {interval}s "
          f"({len(sensors)} sensors, batch size {batch_size})")
    if duration:
        print(f"   Will stop after {duration}s")
    print("   Press Ctrl+C to stop\n")

    try:
        while True:
            if duration and (time.time() - start_time) > duration:
                break

            now = datetime.now(timezone.utc)
            readings = []
            alerts = []

            # Pick a random subset of sensors for this batch
            batch_sensors = random.sample(
                sensors, min(batch_size, len(sensors))
            )

            for sensor in batch_sensors:
                reading = generate_reading(sensor, now, anomaly_pct)
                readings.append(reading)

                if reading["IsAnomaly"] == "true" and random.random() < alert_threshold / anomaly_pct:
                    alerts.append(generate_alert(reading, sensor))

            ingest_batch(readings, alerts, query_uri, kusto_token, db_name)
            total_readings += len(readings)
            total_alerts += len(alerts)

            elapsed = int(time.time() - start_time)
            print(f"\r  📊 {total_readings} readings, {total_alerts} alerts "
                  f"({elapsed}s elapsed)", end="", flush=True)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n⏹ Stopped by user.")

    elapsed = int(time.time() - start_time)
    print(f"\n✅ Injected {total_readings} readings and {total_alerts} alerts "
          f"in {elapsed}s")


def run_batch(sensors: List[Dict], config: dict,
              query_uri: str, kusto_token: str,
              db_name: str, count: int):
    """Inject a fixed number of readings and stop."""
    anomaly_pct = config["streaming"]["anomaly_pct"]
    readings = []
    alerts = []

    print(f"📦 Generating {count} readings...")
    now = datetime.now(timezone.utc)
    for i in range(count):
        sensor = random.choice(sensors)
        ts = datetime.fromtimestamp(
            now.timestamp() - (count - i) * 2, tz=timezone.utc
        )
        reading = generate_reading(sensor, ts, anomaly_pct)
        readings.append(reading)
        if reading["IsAnomaly"] == "true":
            alerts.append(generate_alert(reading, sensor))

    print(f"  📤 Ingesting {len(readings)} readings, {len(alerts)} alerts...")
    ingest_batch(readings, alerts, query_uri, kusto_token, db_name)
    print(f"✅ Done!")


def main():
    parser = argparse.ArgumentParser(description="IoT Sensor Data Injector")
    parser.add_argument("--duration", type=int, default=0,
                        help="Run for N seconds (0 = indefinite)")
    parser.add_argument("--batch-only", type=int, default=0,
                        help="Inject N readings and stop")
    args = parser.parse_args()

    config = load_config()
    state = load_state()

    query_uri = state.get("query_service_uri")
    if not query_uri:
        print("❌ Eventhouse not deployed yet. Run deploy_eventhouse.py first.")
        sys.exit(1)

    db_name = config["eventhouse_name"]
    sensors = load_sensors()

    print(f"🔌 Connecting to Kusto: {query_uri}")
    kusto_token = get_kusto_token(query_uri)

    if args.batch_only:
        run_batch(sensors, config, query_uri, kusto_token,
                  db_name, args.batch_only)
    else:
        run_continuous(sensors, config, query_uri, kusto_token,
                       db_name, args.duration)


if __name__ == "__main__":
    main()
