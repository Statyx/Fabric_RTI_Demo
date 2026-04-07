#!/usr/bin/env python3
"""
Deploy KQL Materialized Views — pre-aggregated statistics for fast dashboards.

Creates 5 materialized views over the Eventhouse KQL tables:

  1. mv_readings_hourly     — Hourly aggregates per SensorType + SiteId
  2. mv_readings_daily      — Daily aggregates per SensorType + SiteId
  3. mv_anomaly_rate_hourly — Hourly anomaly rate per SiteId
  4. mv_alerts_daily        — Daily alert counts per Severity + SiteId
  5. mv_quality_daily       — Daily quality rate per SensorType + SiteId

These views auto-update as new data arrives via streaming ingestion.
The KQL Dashboard and ad-hoc queries can query them instead of raw tables
for dramatically faster results at scale.

Requires: workspace, eventhouse, KQL database with SensorReading & SensorAlert.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from helpers import (
    load_config, load_state, save_state, get_kusto_token,
    kusto_mgmt, print_step,
)

# ── Materialized view definitions ────────────────────────────────

MATERIALIZED_VIEWS = [
    {
        "name": "mv_readings_hourly",
        "description": "Hourly reading statistics per sensor type and site",
        "source_table": "SensorReading",
        "query": """SensorReading
| summarize
    ReadingCount = count(),
    AvgValue = avg(ReadingValue),
    MinValue = min(ReadingValue),
    MaxValue = max(ReadingValue),
    AnomalyCount = countif(IsAnomaly == true),
    GoodQualityCount = countif(QualityFlag == "Good")
    by bin(Timestamp, 1h), SensorType, SiteId""",
    },
    {
        "name": "mv_readings_daily",
        "description": "Daily reading statistics per sensor type and site",
        "source_table": "SensorReading",
        "query": """SensorReading
| summarize
    ReadingCount = count(),
    AvgValue = avg(ReadingValue),
    MinValue = min(ReadingValue),
    MaxValue = max(ReadingValue),
    AnomalyCount = countif(IsAnomaly == true),
    GoodQualityCount = countif(QualityFlag == "Good")
    by bin(Timestamp, 1d), SensorType, SiteId""",
    },
    {
        "name": "mv_anomaly_rate_hourly",
        "description": "Hourly anomaly rate per site (for trend detection)",
        "source_table": "SensorReading",
        "query": """SensorReading
| summarize
    TotalReadings = count(),
    AnomalyCount = countif(IsAnomaly == true)
    by bin(Timestamp, 1h), SiteId""",
    },
    {
        "name": "mv_alerts_daily",
        "description": "Daily alert counts per severity and site",
        "source_table": "SensorAlert",
        "query": """SensorAlert
| summarize
    AlertCount = count(),
    AvgReading = avg(ReadingValue),
    AvgThreshold = avg(ThresholdValue)
    by bin(Timestamp, 1d), Severity, SiteId, SensorType""",
    },
    {
        "name": "mv_quality_daily",
        "description": "Daily quality rate per sensor type and site",
        "source_table": "SensorReading",
        "query": """SensorReading
| summarize
    TotalReadings = count(),
    GoodCount = countif(QualityFlag == "Good"),
    DegradedCount = countif(QualityFlag != "Good")
    by bin(Timestamp, 1d), SensorType, SiteId""",
    },
]


def deploy_materialized_views():
    print(f"\n{'='*60}")
    print("  DEPLOY KQL MATERIALIZED VIEWS")
    print(f"{'='*60}\n")

    config = load_config()
    state = load_state()

    query_uri = state.get("query_service_uri")
    if not query_uri:
        print("❌ Eventhouse not deployed. Run deploy_eventhouse.py first.")
        sys.exit(1)

    db_name = config["eventhouse_name"]
    kusto_token = get_kusto_token(query_uri)

    # Check existing materialized views
    print("  Checking existing materialized views...")
    try:
        result = kusto_mgmt(query_uri, kusto_token, db_name,
                            ".show materialized-views")
        existing = set()
        for table in result.get("Tables", []):
            for row in table.get("Rows", []):
                if row and len(row) > 0:
                    existing.add(row[0])
        print(f"  Found {len(existing)} existing views: {existing or 'none'}")
    except Exception:
        existing = set()
        print("  No existing views found")

    created = 0
    updated = 0
    failed = 0

    for i, mv in enumerate(MATERIALIZED_VIEWS, 1):
        name = mv["name"]
        query = mv["query"].strip()
        desc = mv["description"]

        print(f"\n  [{i}/{len(MATERIALIZED_VIEWS)}] {name}")
        print(f"      {desc}")

        if name in existing:
            # Drop and recreate (alter not supported for query changes)
            print(f"      Dropping existing view...")
            try:
                kusto_mgmt(query_uri, kusto_token, db_name,
                           f".drop materialized-view {name}")
                time.sleep(2)
            except Exception as e:
                print(f"      ⚠ Drop failed: {e}")

        # Create the materialized view
        cmd = (
            f".create materialized-view with (backfill=true) {name} on table {mv['source_table']}\n"
            "{\n"
            f"{query}\n"
            "}"
        )

        try:
            kusto_mgmt(query_uri, kusto_token, db_name, cmd)
            print(f"      ✅ Created (backfill started)")
            if name in existing:
                updated += 1
            else:
                created += 1
        except Exception as e:
            err_msg = str(e)
            if "already exists" in err_msg.lower():
                print(f"      ⚡ Already exists, skipping")
            else:
                print(f"      ❌ Failed: {err_msg[:300]}")
                failed += 1

    # Update state
    state["materialized_views"] = [mv["name"] for mv in MATERIALIZED_VIEWS]
    save_state(state)

    # Summary
    print(f"\n{'='*60}")
    print(f"  ✅ Materialized Views: {created} created, {updated} updated, {failed} failed")
    print(f"{'='*60}")
    print(f"  Views created:")
    for mv in MATERIALIZED_VIEWS:
        print(f"    • {mv['name']} — {mv['description']}")
    print(f"\n  Backfill: Views will process historical data in the background.")
    print(f"  New data: Auto-updated as streaming ingestion continues.")
    print(f"\n  Sample queries:")
    print(f"    mv_readings_hourly | where Timestamp > ago(24h) | order by Timestamp desc")
    print(f"    mv_anomaly_rate_hourly | where AnomalyRate > 5 | order by Timestamp desc")
    print(f"    mv_alerts_daily | summarize TotalAlerts = sum(AlertCount) by SiteId")


if __name__ == "__main__":
    deploy_materialized_views()
