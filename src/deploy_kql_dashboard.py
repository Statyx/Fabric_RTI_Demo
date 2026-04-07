#!/usr/bin/env python3
"""
Deploy KQL Dashboard — RTI_SensorDashboard
Creates a 6-tile real-time dashboard over the Eventhouse KQL database.

Tiles:
  Row 0:  [Reading Count (card)]  [Anomaly Rate (card)]  [Alert Count (card)]
  Row 5:  [Readings Over Time (line chart - full width)]
  Row 11: [Alerts by Severity (pie)]  [Top Alerting Sensors (table)]
  Row 17: [Anomaly Rate by Site (bar)]  [Latest Alerts (table)]

Requires: workspace, eventhouse, KQL database.
"""
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from helpers import (
    load_config, load_state, save_state, get_fabric_token,
    fabric_headers, find_item, b64encode_json, poll_operation,
)

API = "https://api.fabric.microsoft.com/v1"
DASHBOARD_NAME = "RTI_SensorDashboard"


def _id():
    return str(uuid.uuid4())


def build_dashboard_json(state: dict, config: dict) -> dict:
    """Build the RealTimeDashboard.json definition."""
    ds_id = _id()
    page1_id = _id()
    page2_id = _id()
    cluster = state["query_service_uri"]
    db_name = config.get("kql_database_name", "RTI_SensorTelemetry")

    # ── Data source ──────────────────────────────────────────────
    data_source = {
        "id": ds_id,
        "name": db_name,
        "clusterUri": cluster,
        "database": db_name,
        "kind": "manual-kusto",
        "scopeId": "KustoDatabaseResource",
    }

    tiles = []

    # ═══════════════════════════ PAGE 1: Overview ═══════════════
    # Row 0: three KPI cards
    tiles.append({
        "id": _id(), "title": "Total Readings",
        "query": "SensorReading\n| count",
        "layout": {"x": 0, "y": 0, "width": 8, "height": 5},
        "pageId": page1_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Anomaly Rate",
        "query": (
            "SensorReading\n"
            "| summarize Total = count(), Anomalies = countif(IsAnomaly == true)\n"
            "| project AnomalyRate = round(todouble(Anomalies) / Total * 100, 2)"
        ),
        "layout": {"x": 8, "y": 0, "width": 8, "height": 5},
        "pageId": page1_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Total Alerts",
        "query": "SensorAlert\n| count",
        "layout": {"x": 16, "y": 0, "width": 8, "height": 5},
        "pageId": page1_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })

    # Row 5: readings over time (line chart)
    tiles.append({
        "id": _id(), "title": "Readings Over Time (15 min)",
        "query": (
            "SensorReading\n"
            "| summarize AvgValue = round(avg(ReadingValue), 2) "
            "by bin(Timestamp, 15m), SensorType\n"
            "| order by Timestamp asc"
        ),
        "layout": {"x": 0, "y": 5, "width": 24, "height": 6},
        "pageId": page1_id, "dataSourceId": ds_id,
        "visualType": "line",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "seriesColumns": {"type": "infer"},
            "hideLegend": False, "hideTileTitle": False,
            "multipleYAxes": {
                "base": {"id": "-1", "columns": [], "yAxisScale": "linear"},
                "additional": [],
            },
        },
        "usedParamVariables": [],
    })

    # Row 11: alerts by severity (pie) + top alerting sensors (table)
    tiles.append({
        "id": _id(), "title": "Alerts by Severity",
        "query": (
            "SensorAlert\n"
            "| summarize Count = count() by Severity\n"
            "| order by Count desc"
        ),
        "layout": {"x": 0, "y": 11, "width": 8, "height": 6},
        "pageId": page1_id, "dataSourceId": ds_id,
        "visualType": "pie",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Top 10 Alerting Sensors",
        "query": (
            "SensorAlert\n"
            "| summarize AlertCount = count(), "
            "AvgReading = round(avg(ReadingValue), 2) "
            "by SensorId, SensorType\n"
            "| top 10 by AlertCount desc"
        ),
        "layout": {"x": 8, "y": 11, "width": 16, "height": 6},
        "pageId": page1_id, "dataSourceId": ds_id,
        "visualType": "table",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })

    # ═══════════════════════════ PAGE 2: Drilldown ══════════════
    # Row 0: anomaly rate by site (bar)
    tiles.append({
        "id": _id(), "title": "Anomaly Rate by Site",
        "query": (
            "SensorReading\n"
            "| summarize Total = count(), Anomalies = countif(IsAnomaly == true) "
            "by SiteId\n"
            "| project SiteId, AnomalyRate = round(todouble(Anomalies) / Total * 100, 2)\n"
            "| order by AnomalyRate desc"
        ),
        "layout": {"x": 0, "y": 0, "width": 12, "height": 6},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "bar",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })

    # Readings by sensor type heatmap (bar chart)
    tiles.append({
        "id": _id(), "title": "Readings by Sensor Type & Site",
        "query": (
            "SensorReading\n"
            "| summarize Count = count() by SiteId, SensorType\n"
            "| order by SiteId asc, SensorType asc"
        ),
        "layout": {"x": 12, "y": 0, "width": 12, "height": 6},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "bar",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "seriesColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })

    # Row 6: latest alerts table
    tiles.append({
        "id": _id(), "title": "Latest 50 Alerts",
        "query": (
            "SensorAlert\n"
            "| project Timestamp, SiteId, SensorType, Severity, "
            "AlertType, ReadingValue, ThresholdValue, Message\n"
            "| order by Timestamp desc\n"
            "| take 50"
        ),
        "layout": {"x": 0, "y": 6, "width": 24, "height": 7},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "table",
        "visualOptions": {
            "xColumn": {"type": "infer"}, "yColumns": {"type": "infer"},
            "hideTileTitle": False,
        },
        "usedParamVariables": [],
    })

    # Avg reading by sensor type (stat cards row)
    tiles.append({
        "id": _id(), "title": "Avg Temperature (°C)",
        "query": (
            "SensorReading\n"
            "| where SensorType == 'Temperature'\n"
            "| summarize AvgTemp = round(avg(ReadingValue), 1)"
        ),
        "layout": {"x": 0, "y": 13, "width": 5, "height": 4},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {"xColumn": {"type": "infer"}, "yColumns": {"type": "infer"}, "hideTileTitle": False},
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Avg Humidity (%RH)",
        "query": (
            "SensorReading\n"
            "| where SensorType == 'Humidity'\n"
            "| summarize AvgHumidity = round(avg(ReadingValue), 1)"
        ),
        "layout": {"x": 5, "y": 13, "width": 5, "height": 4},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {"xColumn": {"type": "infer"}, "yColumns": {"type": "infer"}, "hideTileTitle": False},
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Avg Pressure (hPa)",
        "query": (
            "SensorReading\n"
            "| where SensorType == 'Pressure'\n"
            "| summarize AvgPressure = round(avg(ReadingValue), 1)"
        ),
        "layout": {"x": 10, "y": 13, "width": 5, "height": 4},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {"xColumn": {"type": "infer"}, "yColumns": {"type": "infer"}, "hideTileTitle": False},
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Avg CO2 (ppm)",
        "query": (
            "SensorReading\n"
            "| where SensorType == 'CO2'\n"
            "| summarize AvgCO2 = round(avg(ReadingValue), 1)"
        ),
        "layout": {"x": 15, "y": 13, "width": 5, "height": 4},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {"xColumn": {"type": "infer"}, "yColumns": {"type": "infer"}, "hideTileTitle": False},
        "usedParamVariables": [],
    })
    tiles.append({
        "id": _id(), "title": "Avg Vibration (mm/s)",
        "query": (
            "SensorReading\n"
            "| where SensorType == 'Vibration'\n"
            "| summarize AvgVibration = round(avg(ReadingValue), 2)"
        ),
        "layout": {"x": 20, "y": 13, "width": 4, "height": 4},
        "pageId": page2_id, "dataSourceId": ds_id,
        "visualType": "stat",
        "visualOptions": {"xColumn": {"type": "infer"}, "yColumns": {"type": "infer"}, "hideTileTitle": False},
        "usedParamVariables": [],
    })

    # ── Assemble ─────────────────────────────────────────────────
    return {
        "$schema": "https://dataexplorer.azure.com/static/d/schema/20/dashboard.json",
        "schema_version": "20",
        "title": DASHBOARD_NAME,
        "autoRefresh": {
            "enabled": True,
            "defaultInterval": "30s",
            "minInterval": "30s",
        },
        "dataSources": [data_source],
        "pages": [
            {"id": page1_id, "name": "Sensor Overview"},
            {"id": page2_id, "name": "Drilldown & Alerts"},
        ],
        "tiles": tiles,
        "parameters": [],
    }


def deploy_kql_dashboard():
    """Deploy or update the KQL dashboard."""
    print(f"\n{'='*60}")
    print(f"  DEPLOY KQL DASHBOARD: {DASHBOARD_NAME}")
    print(f"{'='*60}\n")

    config = load_config()
    state = load_state()
    ws_id = state["workspace_id"]
    token = get_fabric_token()
    headers = fabric_headers(token)

    # Build the dashboard JSON
    print("  Building dashboard definition...")
    dash_json = build_dashboard_json(state, config)
    tiles_count = len(dash_json["tiles"])
    pages_count = len(dash_json["pages"])
    print(f"  📊 {pages_count} pages, {tiles_count} tiles")

    # Check if dashboard exists
    import requests
    dash_id = state.get("kql_dashboard_id")
    if not dash_id:
        try:
            existing = find_item(token, API, ws_id, DASHBOARD_NAME, "KQLDashboard")
            dash_id = existing["id"]
            print(f"  Found existing dashboard: {dash_id}")
        except RuntimeError:
            pass

    if not dash_id:
        # Create new
        print("  Creating KQL Dashboard item...")
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/items",
            headers=headers,
            json={
                "displayName": DASHBOARD_NAME,
                "type": "KQLDashboard",
                "description": "Real-time sensor monitoring dashboard — RTI Demo",
            },
        )
        if resp.status_code in (200, 201):
            dash_id = resp.json()["id"]
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            poll_operation(token, API, op_id)
            result = find_item(token, API, ws_id, DASHBOARD_NAME, "KQLDashboard")
            dash_id = result["id"]
        else:
            raise RuntimeError(f"Create dashboard failed ({resp.status_code}): {resp.text[:300]}")
        print(f"  ✅ Created: {dash_id}")
    else:
        print(f"  Updating existing: {dash_id}")

    # Upload definition
    print("  Uploading dashboard definition...")
    payload = b64encode_json(dash_json)
    body = {
        "definition": {
            "parts": [{
                "path": "RealTimeDashboard.json",
                "payload": payload,
                "payloadType": "InlineBase64",
            }]
        }
    }

    # Try type-specific endpoint, then generic
    for endpoint in [
        f"{API}/workspaces/{ws_id}/kqlDashboards/{dash_id}/updateDefinition",
        f"{API}/workspaces/{ws_id}/items/{dash_id}/updateDefinition",
    ]:
        resp = requests.post(endpoint, headers=headers, json=body)
        if resp.status_code in (200, 202):
            if resp.status_code == 202:
                op_id = resp.headers.get("x-ms-operation-id", "")
                if op_id:
                    poll_operation(token, API, op_id)
            print(f"  ✅ Definition uploaded ({resp.status_code})")
            break
    else:
        print(f"  ❌ Failed: {resp.status_code} — {resp.text[:300]}")

    # Save state
    state["kql_dashboard_id"] = dash_id
    save_state(state)
    print(f"\n  Dashboard ID: {dash_id}")
    print(f"  Pages: {pages_count} | Tiles: {tiles_count}")
    print(f"  Auto-refresh: 30s")


if __name__ == "__main__":
    deploy_kql_dashboard()
