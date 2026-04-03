#!/usr/bin/env python3
"""
Deploy Semantic Model SM_SensorAnalytics for the Fabric RTI Demo.

Steps:
  1. Create OneLake shortcuts in Lakehouse → Eventhouse KQL tables
  2. Build model.bim (star schema, DAX measures, AI-ready)
  3. Deploy via REST API (Direct Lake mode)

Prerequisites:
  - deploy_workspace.py, deploy_lakehouse.py, deploy_eventhouse.py already run
  - state.json has workspace_id, lakehouse_id, kql_database_id
"""

import base64
import json
import sys
import time
import uuid

import requests

from helpers import (
    load_config,
    load_state,
    save_state,
    get_fabric_token,
    fabric_headers,
    find_item,
    poll_operation,
)

API = "https://api.fabric.microsoft.com/v1"
MODEL_NAME = "SM_SensorAnalytics"

SQL_ENDPOINT = (
    "eenhbexk3uueboufjqpzd6vyqe-clsnzijnpmdeja4nmheuzlmkz4"
    ".datawarehouse.fabric.microsoft.com"
)
LAKEHOUSE_NAME = "LH_SensorReference"


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _tag():
    return str(uuid.uuid4())


def _col(name, dtype, desc, *, summarize_none=False, hidden=False,
         fmt=None, data_category=None):
    """Build a column definition."""
    c = {
        "name": name,
        "dataType": dtype,
        "sourceColumn": name,
        "description": desc,
        "lineageTag": _tag(),
        "annotations": [
            {"name": "SummarizationSetBy", "value": "Automatic"},
        ],
    }
    if summarize_none:
        c["summarizeBy"] = "none"
    if hidden:
        c["isHidden"] = True
    if fmt:
        c["formatString"] = fmt
    if data_category:
        c["dataCategory"] = data_category
    return c


def _partition(table_name):
    """Build a Direct Lake entity partition."""
    return {
        "name": table_name,
        "source": {
            "type": "entity",
            "entityName": table_name,
            "schemaName": "dbo",
            "expressionSource": "DatabaseQuery",
        },
    }


def _measure(name, expr, desc, *, fmt="#,0", folder=None):
    """Build a DAX measure definition."""
    m = {
        "name": name,
        "expression": expr,
        "description": desc,
        "formatString": fmt,
        "lineageTag": _tag(),
    }
    if folder:
        m["displayFolder"] = folder
    return m


# ──────────────────────────────────────────────
# Shortcuts  (Eventhouse → Lakehouse)
# ──────────────────────────────────────────────

def create_shortcut(headers, ws_id, lakehouse_id, table_name, kql_db_id):
    """Create a OneLake shortcut in the Lakehouse to a KQL table."""
    body = {
        "name": table_name,
        "path": "Tables",
        "target": {
            "oneLake": {
                "workspaceId": ws_id,
                "itemId": kql_db_id,
                "path": f"Tables/{table_name}",
            }
        },
    }
    resp = requests.post(
        f"{API}/workspaces/{ws_id}/items/{lakehouse_id}/shortcuts",
        headers=headers, json=body, timeout=30,
    )
    if resp.status_code in (200, 201):
        print(f"  ✅ Shortcut created: {table_name}")
        return True
    if resp.status_code == 409:
        print(f"  ✅ Shortcut already exists: {table_name}")
        return True
    print(f"  ⚠️  Shortcut failed ({resp.status_code}): {resp.text[:300]}")
    return False


# ──────────────────────────────────────────────
# model.bim builder
# ──────────────────────────────────────────────

def build_model_bim(include_facts):
    """Return the full model.bim dict."""

    tables = []
    relationships = []

    # ── dim_sites ────────────────────────────
    tables.append({
        "name": "dim_sites",
        "description": (
            "Reference table of industrial sites. "
            "Each row is a physical location (factory, warehouse, office)."
        ),
        "columns": [
            _col("site_id", "string",
                 "Unique site identifier (e.g. SITE_001)", summarize_none=True),
            _col("site_name", "string",
                 "Human-readable site name (e.g. Paris HQ)"),
            _col("region", "string",
                 "Geographic region (e.g. Île-de-France)",
                 data_category="StateOrProvince"),
            _col("country", "string", "Country", data_category="Country"),
            _col("latitude", "double", "Site latitude", fmt="#,0.0#"),
            _col("longitude", "double", "Site longitude", fmt="#,0.0#"),
            _col("is_active", "boolean", "Whether the site is currently active"),
        ],
        "partitions": [_partition("dim_sites")],
    })

    # ── dim_zones ────────────────────────────
    tables.append({
        "name": "dim_zones",
        "description": (
            "Zones within a site (Production, Storage, Office, HVAC). "
            "Each zone belongs to exactly one site."
        ),
        "columns": [
            _col("zone_id", "string",
                 "Unique zone identifier (e.g. SITE_001_Z01)", summarize_none=True),
            _col("zone_name", "string", "Zone display name"),
            _col("zone_type", "string",
                 "Zone category: Production, Storage, Office, or HVAC"),
            _col("site_id", "string", "Parent site FK", summarize_none=True, hidden=True),
            _col("floor", "int64", "Floor number", summarize_none=True),
            _col("area_sqm", "double", "Zone area in square meters", fmt="#,0.0"),
            _col("is_active", "boolean", "Whether the zone is currently active"),
        ],
        "partitions": [_partition("dim_zones")],
    })

    # ── dim_sensors ──────────────────────────
    tables.append({
        "name": "dim_sensors",
        "description": (
            "Sensor catalogue. Each sensor has a type (Temperature, Humidity, "
            "Pressure, CO2, Vibration), a unit, and normal/critical thresholds."
        ),
        "columns": [
            _col("sensor_id", "string",
                 "Unique sensor identifier (e.g. SN_0001)", summarize_none=True),
            _col("sensor_name", "string", "Sensor display name"),
            _col("sensor_type", "string",
                 "Sensor category: Temperature, Humidity, Pressure, CO2, Vibration"),
            _col("unit", "string", "Measurement unit (°C, %RH, hPa, ppm, mm/s)"),
            _col("zone_id", "string", "Zone FK", summarize_none=True, hidden=True),
            _col("site_id", "string", "Site FK", summarize_none=True, hidden=True),
            _col("min_normal", "double", "Lower bound of normal range", fmt="#,0.0#"),
            _col("max_normal", "double", "Upper bound of normal range", fmt="#,0.0#"),
            _col("min_critical", "double", "Lower bound of critical range", fmt="#,0.0#"),
            _col("max_critical", "double", "Upper bound of critical range", fmt="#,0.0#"),
            _col("install_date", "string", "Sensor installation date"),
            _col("is_active", "boolean", "Whether the sensor is currently active"),
        ],
        "partitions": [_partition("dim_sensors")],
    })

    # ── Dimension relationships ──────────────
    # dim_sites 1 → M dim_zones
    relationships.append({
        "name": "rel_sites_zones",
        "fromTable": "dim_zones",
        "fromColumn": "site_id",
        "toTable": "dim_sites",
        "toColumn": "site_id",
    })
    # dim_zones 1 → M dim_sensors
    relationships.append({
        "name": "rel_zones_sensors",
        "fromTable": "dim_sensors",
        "fromColumn": "zone_id",
        "toTable": "dim_zones",
        "toColumn": "zone_id",
    })

    # ── Fact tables (from Eventhouse shortcuts) ──
    if include_facts:
        # SensorReading
        tables.append({
            "name": "SensorReading",
            "description": (
                "Real-time sensor readings streamed via EventStream. "
                "Each row is a single measurement from one sensor at one point in time."
            ),
            "columns": [
                _col("ReadingId", "string", "Unique reading identifier", summarize_none=True),
                _col("SensorId", "string", "Sensor FK", summarize_none=True, hidden=True),
                _col("ZoneId", "string", "Zone FK (denormalized)", summarize_none=True, hidden=True),
                _col("SiteId", "string", "Site FK (denormalized)", summarize_none=True, hidden=True),
                _col("Timestamp", "dateTime", "Reading timestamp (UTC)", fmt="yyyy-MM-dd HH:mm:ss"),
                _col("SensorType", "string", "Sensor type at time of reading"),
                _col("ReadingValue", "double", "Measured value in sensor unit", fmt="#,0.00"),
                _col("Unit", "string", "Measurement unit"),
                _col("IsAnomaly", "boolean", "Whether this reading is anomalous"),
                _col("QualityFlag", "string", "Data quality: Good, Degraded, etc."),
            ],
            "measures": [
                _measure("Total Readings", "COUNTROWS(SensorReading)",
                         "Total number of sensor readings", folder="Counts"),
                _measure("Avg Reading Value",
                         "AVERAGE(SensorReading[ReadingValue])",
                         "Average sensor reading value",
                         fmt="#,0.00", folder="Statistics"),
                _measure("Max Reading Value",
                         "MAX(SensorReading[ReadingValue])",
                         "Maximum sensor reading value",
                         fmt="#,0.00", folder="Statistics"),
                _measure("Min Reading Value",
                         "MIN(SensorReading[ReadingValue])",
                         "Minimum sensor reading value",
                         fmt="#,0.00", folder="Statistics"),
                _measure("Anomaly Count",
                         "CALCULATE(COUNTROWS(SensorReading), SensorReading[IsAnomaly] = TRUE())",
                         "Number of anomalous readings", folder="Quality"),
                _measure("Anomaly Rate",
                         "DIVIDE([Anomaly Count], [Total Readings], 0)",
                         "Percentage of readings that are anomalous",
                         fmt="0.0%", folder="Quality"),
                _measure("Good Quality Rate",
                         'DIVIDE(CALCULATE(COUNTROWS(SensorReading), SensorReading[QualityFlag] = "Good"), [Total Readings], 0)',
                         "Percentage of readings with Good quality flag",
                         fmt="0.0%", folder="Quality"),
                _measure("Worst Zone Quality",
                         'VAR _QualityByZone = ADDCOLUMNS(VALUES(dim_zones[zone_type]), "ZoneQuality", CALCULATE([Good Quality Rate])) RETURN MINX(_QualityByZone, [ZoneQuality])',
                         "Lowest Good Quality Rate across all zone types (identifies worst zone)",
                         fmt="0.0%", folder="Quality"),
            ],
            "partitions": [_partition("SensorReading")],
        })

        # SensorAlert
        tables.append({
            "name": "SensorAlert",
            "description": (
                "Alerts triggered when sensor readings exceed critical thresholds. "
                "Each alert links to a sensor and includes severity and threshold details."
            ),
            "columns": [
                _col("AlertId", "string", "Unique alert identifier", summarize_none=True),
                _col("SensorId", "string", "Sensor FK", summarize_none=True, hidden=True),
                _col("ZoneId", "string", "Zone FK (denormalized)", summarize_none=True, hidden=True),
                _col("SiteId", "string", "Site FK (denormalized)", summarize_none=True, hidden=True),
                _col("Timestamp", "dateTime", "Alert timestamp (UTC)", fmt="yyyy-MM-dd HH:mm:ss"),
                _col("SensorType", "string", "Sensor type that triggered the alert"),
                _col("AlertType", "string", "Alert classification (e.g. CRITICAL_HIGH)"),
                _col("Severity", "string", "Alert severity: WARNING or CRITICAL"),
                _col("ReadingValue", "double", "Actual reading that triggered the alert", fmt="#,0.00"),
                _col("ThresholdValue", "double", "Threshold that was exceeded", fmt="#,0.00"),
                _col("Message", "string", "Human-readable alert description"),
            ],
            "measures": [
                _measure("Total Alerts", "COUNTROWS(SensorAlert)",
                         "Total number of sensor alerts", folder="Counts"),
                _measure("Critical Alerts",
                         'CALCULATE(COUNTROWS(SensorAlert), SensorAlert[Severity] = "CRITICAL")',
                         "Number of CRITICAL severity alerts", folder="Alerts"),
                _measure("Warning Alerts",
                         'CALCULATE(COUNTROWS(SensorAlert), SensorAlert[Severity] = "WARNING")',
                         "Number of WARNING severity alerts", folder="Alerts"),
                _measure("Alert Rate",
                         "DIVIDE([Total Alerts], [Total Readings], 0)",
                         "Ratio of alerts to total readings",
                         fmt="0.00%", folder="Quality"),
            ],
            "partitions": [_partition("SensorAlert")],
        })

        # Fact → Dim relationships
        relationships.append({
            "name": "rel_reading_sensor",
            "fromTable": "SensorReading",
            "fromColumn": "SensorId",
            "toTable": "dim_sensors",
            "toColumn": "sensor_id",
        })
        relationships.append({
            "name": "rel_alert_sensor",
            "fromTable": "SensorAlert",
            "fromColumn": "SensorId",
            "toTable": "dim_sensors",
            "toColumn": "sensor_id",
        })

    # ── Assemble model.bim ───────────────────
    # Add lineageTags to tables
    for t in tables:
        if "lineageTag" not in t:
            t["lineageTag"] = _tag()

    model_bim = {
        "compatibilityLevel": 1604,
        "model": {
            "name": MODEL_NAME,
            "defaultMode": "directLake",
            "defaultPowerBIDataSourceVersion": "PowerBI_V3",
            "discourageImplicitMeasures": True,
            "culture": "en-US",
            "sourceQueryCulture": "en-US",
            "tables": tables,
            "relationships": relationships,
            "expressions": [
                {
                    "name": "DatabaseQuery",
                    "kind": "m",
                    "lineageTag": _tag(),
                    "expression": [
                        "let",
                        f'    database = Sql.Database("{SQL_ENDPOINT}", "{LAKEHOUSE_NAME}")',
                        "in",
                        "    database",
                    ],
                }
            ],
            "annotations": [
                {"name": "PBI_QueryOrder", "value": "[]"},
            ],
        },
    }
    return model_bim


# ──────────────────────────────────────────────
# Deployment
# ──────────────────────────────────────────────

def deploy_model(model_bim, headers, ws_id, existing_id=None):
    """Create or update the semantic model via REST API."""
    bim_b64 = base64.b64encode(json.dumps(model_bim).encode()).decode()
    pbism_b64 = base64.b64encode(b'{"version": "1.0"}').decode()
    parts = [
        {"path": "definition.pbism", "payload": pbism_b64, "payloadType": "InlineBase64"},
        {"path": "model.bim", "payload": bim_b64, "payloadType": "InlineBase64"},
    ]

    if existing_id:
        print(f"  📦 Updating existing model: {existing_id}")
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/semanticModels/{existing_id}/updateDefinition",
            headers=headers, json={"definition": {"parts": parts}}, timeout=60,
        )
    else:
        print(f"  📦 Creating new model: {MODEL_NAME}")
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/items",
            headers=headers,
            json={
                "displayName": MODEL_NAME,
                "type": "SemanticModel",
                "description": "IoT sensor analytics — readings, alerts, anomaly tracking across sites and zones",
                "definition": {"parts": parts},
            },
            timeout=60,
        )

    if resp.status_code == 201:
        model_id = resp.json()["id"]
        print(f"  ✅ Model created: {model_id}")
        return model_id

    if resp.status_code in (200, 202):
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            print(f"  ⏳ Polling operation {op_id}...")
            for _ in range(60):
                time.sleep(5)
                op = requests.get(f"{API}/operations/{op_id}", headers=headers, timeout=30).json()
                status = op.get("status", "")
                if status == "Succeeded":
                    if existing_id:
                        print(f"  ✅ Model updated")
                        return existing_id
                    result = requests.get(
                        f"{API}/operations/{op_id}/result", headers=headers, timeout=30
                    ).json()
                    model_id = result.get("id", op_id)
                    print(f"  ✅ Model created: {model_id}")
                    return model_id
                if status in ("Failed", "Cancelled"):
                    raise RuntimeError(f"Deploy failed: {json.dumps(op.get('error', {}), indent=2)}")
            raise TimeoutError("Deploy did not complete in 5 minutes")
        if existing_id:
            print("  ✅ Model updated")
            return existing_id

    raise RuntimeError(f"Unexpected {resp.status_code}: {resp.text[:500]}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    state = load_state()
    ws_id = state.get("workspace_id")
    lakehouse_id = state.get("lakehouse_id")
    kql_db_id = state.get("kql_database_id")

    if not ws_id or not lakehouse_id:
        print("❌ workspace_id and lakehouse_id required in state.json")
        sys.exit(1)

    token = get_fabric_token()
    headers = fabric_headers(token)

    # ── Step 1: Enable OneLake availability & create shortcuts ──
    facts_available = False
    if kql_db_id:
        print("📎 Step 1/3 — Enabling OneLake availability on KQL database...")
        # Enable OneLake availability so KQL tables appear as Delta in OneLake
        patch_resp = requests.patch(
            f"{API}/workspaces/{ws_id}/kqlDatabases/{kql_db_id}",
            headers=headers, timeout=30,
            json={
                "properties": {
                    "oneLakeCachingPolicy": {"enabled": True},
                    "oneLakeStandardAvailability": {"enabled": True},
                }
            },
        )
        if patch_resp.status_code in (200, 202):
            print("  ✅ OneLake availability enabled")
            # Wait for tables to materialize
            print("  ⏳ Waiting 15s for Delta tables to materialize...")
            time.sleep(15)
        else:
            print(f"  ⚠️  OneLake availability patch: {patch_resp.status_code} — {patch_resp.text[:200]}")

        print("  📎 Creating shortcuts (Eventhouse → Lakehouse)...")
        ok_reading = create_shortcut(headers, ws_id, lakehouse_id, "SensorReading", kql_db_id)
        ok_alert = create_shortcut(headers, ws_id, lakehouse_id, "SensorAlert", kql_db_id)
        facts_available = ok_reading and ok_alert
        if not facts_available:
            print("  ⚠️  Shortcuts failed — model will include dimension tables only.")
            print("     To fix: enable OneLake availability on the KQL database,")
            print("     then re-run this script.")
    else:
        print("⚠️  Step 1/3 — No KQL database in state.json, skipping shortcuts.")

    # ── Step 2: Build model.bim ──
    print(f"\n🔨 Step 2/3 — Building model.bim (facts={'yes' if facts_available else 'no'})...")
    model_bim = build_model_bim(include_facts=facts_available)
    table_count = len(model_bim["model"]["tables"])
    measure_count = sum(len(t.get("measures", [])) for t in model_bim["model"]["tables"])
    rel_count = len(model_bim["model"]["relationships"])
    print(f"  📊 {table_count} tables, {measure_count} measures, {rel_count} relationships")

    # ── Step 3: Deploy ──
    print(f"\n🚀 Step 3/3 — Deploying {MODEL_NAME}...")
    try:
        existing = find_item(token, API, ws_id, MODEL_NAME, "SemanticModel")
        existing_id = existing["id"]
    except RuntimeError:
        existing_id = None

    model_id = deploy_model(model_bim, headers, ws_id, existing_id)
    state["semantic_model_id"] = model_id
    save_state(state)

    print(f"\n{'='*60}")
    print(f"✅ Semantic Model deployed: {MODEL_NAME}")
    print(f"   ID:     {model_id}")
    print(f"   Tables: {table_count} | Measures: {measure_count} | Rels: {rel_count}")
    print(f"   Mode:   Direct Lake")
    if not facts_available:
        print(f"\n   ⚠️  Fact tables not included (shortcut creation failed).")
        print(f"   To add SensorReading + SensorAlert:")
        print(f"   1. In Fabric portal → KQL Database → Settings → OneLake availability → ON")
        print(f"   2. Re-run: python deploy_semantic_model.py")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
