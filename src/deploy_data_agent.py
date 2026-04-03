#!/usr/bin/env python3
"""
Deploy SensorAnalytics Data Agent in the Fabric RTI Demo workspace.

Creates a new Data Agent connected to SM_SensorAnalytics semantic model.
Includes instructions, 10 few-shot examples, and full element mapping.

Usage:
    python deploy_data_agent.py
"""
import base64
import json
import os
import sys
import time

import requests

from helpers import (
    load_state,
    get_fabric_token,
    fabric_headers,
    find_item,
    poll_operation,
)

API = "https://api.fabric.microsoft.com/v1"
AGENT_NAME = "SensorAnalytics_Agent"
MODEL_NAME = "SM_SensorAnalytics"
DATASOURCE_FOLDER = f"semantic-model-{MODEL_NAME}"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INSTRUCTIONS_PATH = os.path.join(SCRIPT_DIR, "agent_instructions.md")
FEWSHOTS_PATH = os.path.join(SCRIPT_DIR, "agent_fewshots.json")


def b64(obj):
    """Base64-encode a JSON object."""
    return base64.b64encode(
        json.dumps(obj, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")


# ──────────────────────────────────────────────
# Elements builder — from model.bim knowledge
# ──────────────────────────────────────────────

def build_elements():
    """Build the datasource elements array (tables → columns + measures)."""

    def _col(name, dtype, desc):
        return {
            "id": None, "display_name": name,
            "type": "semantic_model.column",
            "is_selected": True,
            "description": desc, "children": [],
        }

    def _meas(name, desc):
        return {
            "id": None, "display_name": name,
            "type": "semantic_model.measure",
            "is_selected": True,
            "description": desc, "children": [],
        }

    def _table(name, desc, children):
        return {
            "id": None, "display_name": name,
            "type": "semantic_model.table",
            "is_selected": True,
            "description": desc, "children": children,
        }

    elements = []

    # ── dim_sites ──
    elements.append(_table(
        "dim_sites",
        "Reference table of industrial sites (factories, warehouses, offices)",
        [
            _col("site_id", "string", "Unique site identifier (e.g. SITE_001)"),
            _col("site_name", "string", "Human-readable site name (e.g. Paris HQ)"),
            _col("region", "string", "Geographic region (e.g. Île-de-France)"),
            _col("country", "string", "Country"),
            _col("latitude", "double", "Site latitude"),
            _col("longitude", "double", "Site longitude"),
            _col("is_active", "boolean", "Whether the site is currently active"),
        ],
    ))

    # ── dim_zones ──
    elements.append(_table(
        "dim_zones",
        "Zones within a site: Production, Storage, Office, HVAC",
        [
            _col("zone_id", "string", "Unique zone identifier (e.g. SITE_001_Z01)"),
            _col("zone_name", "string", "Zone display name"),
            _col("zone_type", "string", "Zone category: Production, Storage, Office, or HVAC"),
            _col("site_id", "string", "Parent site FK"),
            _col("floor", "int64", "Floor number"),
            _col("area_sqm", "double", "Zone area in square meters"),
            _col("is_active", "boolean", "Whether the zone is currently active"),
        ],
    ))

    # ── dim_sensors ──
    elements.append(_table(
        "dim_sensors",
        "Sensor catalogue — each sensor has type (Temperature, Humidity, Pressure, CO2, Vibration), unit, and thresholds",
        [
            _col("sensor_id", "string", "Unique sensor identifier (e.g. SN_0001)"),
            _col("sensor_name", "string", "Sensor display name"),
            _col("sensor_type", "string", "Sensor category: Temperature, Humidity, Pressure, CO2, Vibration"),
            _col("unit", "string", "Measurement unit (°C, %RH, hPa, ppm, mm/s)"),
            _col("zone_id", "string", "Zone FK"),
            _col("site_id", "string", "Site FK"),
            _col("min_normal", "double", "Lower bound of normal range"),
            _col("max_normal", "double", "Upper bound of normal range"),
            _col("min_critical", "double", "Lower bound of critical range"),
            _col("max_critical", "double", "Upper bound of critical range"),
            _col("install_date", "string", "Sensor installation date"),
            _col("is_active", "boolean", "Whether the sensor is currently active"),
        ],
    ))

    # ── SensorReading (fact) ──
    elements.append(_table(
        "SensorReading",
        "Real-time sensor readings streamed via EventStream — one row per measurement",
        [
            _col("ReadingId", "string", "Unique reading identifier"),
            _col("SensorId", "string", "Sensor FK"),
            _col("ZoneId", "string", "Zone FK (denormalized)"),
            _col("SiteId", "string", "Site FK (denormalized)"),
            _col("Timestamp", "dateTime", "Reading timestamp (UTC)"),
            _col("SensorType", "string", "Sensor type at time of reading"),
            _col("ReadingValue", "double", "Measured value in sensor unit"),
            _col("Unit", "string", "Measurement unit"),
            _col("IsAnomaly", "boolean", "Whether this reading is anomalous"),
            _col("QualityFlag", "string", "Data quality: Good, Degraded, etc."),
            # Measures
            _meas("Total Readings", "Total count of sensor readings"),
            _meas("Avg Reading Value", "Average sensor reading value"),
            _meas("Max Reading Value", "Maximum sensor reading value"),
            _meas("Min Reading Value", "Minimum sensor reading value"),
            _meas("Anomaly Count", "Number of anomalous readings"),
            _meas("Anomaly Rate", "Percentage of readings that are anomalous"),
            _meas("Good Quality Rate", "Percentage with Good quality flag"),
        ],
    ))

    # ── SensorAlert (fact) ──
    elements.append(_table(
        "SensorAlert",
        "Alerts triggered when sensor readings exceed critical thresholds",
        [
            _col("AlertId", "string", "Unique alert identifier"),
            _col("SensorId", "string", "Sensor FK"),
            _col("ZoneId", "string", "Zone FK (denormalized)"),
            _col("SiteId", "string", "Site FK (denormalized)"),
            _col("Timestamp", "dateTime", "Alert timestamp (UTC)"),
            _col("SensorType", "string", "Sensor type that triggered the alert"),
            _col("AlertType", "string", "Alert classification (e.g. CRITICAL_HIGH)"),
            _col("Severity", "string", "Alert severity: WARNING or CRITICAL"),
            _col("ReadingValue", "double", "Actual reading that triggered the alert"),
            _col("ThresholdValue", "double", "Threshold that was exceeded"),
            _col("Message", "string", "Human-readable alert description"),
            # Measures
            _meas("Total Alerts", "Total number of sensor alerts"),
            _meas("Critical Alerts", "Number of CRITICAL severity alerts"),
            _meas("Warning Alerts", "Number of WARNING severity alerts"),
            _meas("Alert Rate", "Ratio of alerts to total readings"),
        ],
    ))

    return elements


# ──────────────────────────────────────────────
# Build definition parts
# ──────────────────────────────────────────────

def build_parts(instructions_text, fewshots, datasource):
    """Build the 8 definition parts (data_agent + publish_info + draft/published × 3)."""

    data_agent_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"
    }
    stage_config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json",
        "aiInstructions": instructions_text,
    }
    publish_info = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/publishInfo/1.0.0/schema.json",
        "description": f"SensorAnalytics Data Agent — Industrial IoT monitoring — Created {time.strftime('%Y-%m-%d')}",
    }

    sc = b64(stage_config)
    ds = b64(datasource)
    fs = b64(fewshots)

    return [
        {"path": "Files/Config/data_agent.json", "payload": b64(data_agent_json), "payloadType": "InlineBase64"},
        {"path": "Files/Config/publish_info.json", "payload": b64(publish_info), "payloadType": "InlineBase64"},
        # Draft
        {"path": "Files/Config/draft/stage_config.json", "payload": sc, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/draft/{DATASOURCE_FOLDER}/datasource.json", "payload": ds, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/draft/{DATASOURCE_FOLDER}/fewshots.json", "payload": fs, "payloadType": "InlineBase64"},
        # Published
        {"path": "Files/Config/published/stage_config.json", "payload": sc, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/published/{DATASOURCE_FOLDER}/datasource.json", "payload": ds, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/published/{DATASOURCE_FOLDER}/fewshots.json", "payload": fs, "payloadType": "InlineBase64"},
    ]


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    state = load_state()
    ws_id = state.get("workspace_id")
    model_id = state.get("semantic_model_id")

    if not ws_id or not model_id:
        print("❌ workspace_id and semantic_model_id required in state.json")
        sys.exit(1)

    print("=" * 60)
    print(f"  {AGENT_NAME} — Data Agent Deployment")
    print("=" * 60)

    token = get_fabric_token()
    headers = fabric_headers(token)

    # ── Load instructions ──
    print("\n📄 Loading files...")
    with open(INSTRUCTIONS_PATH, "r", encoding="utf-8") as f:
        instructions = f.read()
    print(f"  Instructions: {len(instructions)} chars")

    with open(FEWSHOTS_PATH, "r", encoding="utf-8") as f:
        fewshots = json.load(f)
    print(f"  Fewshots: {len(fewshots.get('fewShots', []))} examples")

    # ── Build datasource ──
    print("\n🔨 Building datasource & elements...")
    elements = build_elements()
    elem_count = len(elements)
    child_count = sum(len(e.get("children", [])) for e in elements)
    print(f"  Elements: {elem_count} tables, {child_count} cols/measures")

    datasource = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json",
        "artifactId": model_id,
        "workspaceId": ws_id,
        "displayName": MODEL_NAME,
        "type": "semantic_model",
        "elements": elements,
    }

    # ── Build parts ──
    parts = build_parts(instructions, fewshots, datasource)
    print(f"  Definition parts: {len(parts)}")

    # ── Create or update agent ──
    print(f"\n🚀 Deploying {AGENT_NAME}...")

    # Check if agent already exists
    existing_id = None
    try:
        existing = find_item(token, API, ws_id, AGENT_NAME, "DataAgent")
        existing_id = existing["id"]
        print(f"  📦 Updating existing agent: {existing_id}")
    except RuntimeError:
        print(f"  📦 Creating new agent: {AGENT_NAME}")

    if existing_id:
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/items/{existing_id}/updateDefinition",
            headers=headers,
            json={"definition": {"parts": parts}},
            timeout=60,
        )
    else:
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/items",
            headers=headers,
            json={
                "displayName": AGENT_NAME,
                "type": "DataAgent",
                "description": "Industrial IoT sensor analytics — readings, alerts, anomalies across sites and zones",
                "definition": {"parts": parts},
            },
            timeout=60,
        )

    if resp.status_code == 201:
        agent_id = resp.json()["id"]
        print(f"  ✅ Agent created: {agent_id}")
    elif resp.status_code in (200, 202):
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            print(f"  ⏳ Async operation: {op_id}")
            op = poll_operation(token, API, op_id)
            print(f"  ✅ Agent {'updated' if existing_id else 'created'}")
            if not existing_id:
                try:
                    result = requests.get(
                        f"{API}/operations/{op_id}/result",
                        headers=headers, timeout=30,
                    ).json()
                    agent_id = result.get("id", "unknown")
                except Exception:
                    agent_id = "unknown"
            else:
                agent_id = existing_id
        else:
            agent_id = existing_id or "unknown"
            print(f"  ✅ Agent {'updated' if existing_id else 'created'}")
    else:
        print(f"  ❌ FAILED ({resp.status_code}): {resp.text[:500]}")
        sys.exit(1)

    # Save to state
    state["data_agent_id"] = agent_id
    from helpers import save_state
    save_state(state)

    print(f"\n{'=' * 60}")
    print(f"  ✅ Data Agent deployed: {AGENT_NAME}")
    print(f"  ID:       {agent_id}")
    print(f"  Model:    {MODEL_NAME} ({model_id})")
    print(f"  Tables:   {elem_count} | Elements: {child_count}")
    print(f"  Fewshots: {len(fewshots.get('fewShots', []))}")
    print(f"  Portal:   https://app.fabric.microsoft.com/groups/{ws_id}/dataAgents/{agent_id}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
