#!/usr/bin/env python3
"""
Deploy Power BI Report — RPT_SensorAnalytics
Creates a 2-page report over the SM_SensorAnalytics semantic model.

Page 1 — Overview:
  Row 0:  Title bar
  Row 1:  [Total Readings (card)]  [Anomaly Rate (card)]  [Total Alerts (card)]
  Row 2:  [Anomaly Rate by Zone (bar)]  [Readings per Site (bar)]
  Row 3:  [Good Quality Rate by Sensor Type (bar)]

Page 2 — Alerts:
  Row 0:  Title bar
  Row 1:  [Critical Alerts (card)]  [Warning Alerts (card)]  [Alert Rate (card)]
  Row 2:  [Alerts by Site (bar)]
  Row 3:  [Avg Reading by Site (bar)]

Requires: workspace, semantic_model.
Uses legacy PBIX format (report.json + definition.pbir).
"""
import json
import sys
import uuid
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from helpers import (
    load_config, load_state, save_state, get_fabric_token,
    fabric_headers, find_item, b64encode_json, poll_operation,
)

API = "https://api.fabric.microsoft.com/v1"
REPORT_NAME = "RPT_SensorAnalytics"


def _hex():
    return uuid.uuid4().hex[:16]


# ── Visual factory functions ─────────────────────────────────────

def _card(name, x, y, w, h, table, measure, title, z=1):
    alias = table[0].lower()
    return {
        "x": x, "y": y, "z": z, "width": w, "height": h,
        "config": json.dumps({
            "name": name,
            "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": z, "width": w, "height": h}}],
            "singleVisual": {
                "visualType": "cardVisual",
                "projections": {"Data": [{"queryRef": f"{table}.{measure}"}]},
                "prototypeQuery": {
                    "Version": 2,
                    "From": [{"Name": alias, "Entity": table, "Type": 0}],
                    "Select": [{
                        "Measure": {"Expression": {"SourceRef": {"Source": alias}}, "Property": measure},
                        "Name": f"{table}.{measure}", "NativeReferenceName": measure,
                    }],
                },
                "drillFilterOtherVisuals": True,
                "objects": {
                    "outline": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}],
                    "calloutValue": [{"properties": {"fontSize": {"expr": {"Literal": {"Value": "27D"}}}}}],
                    "categoryLabel": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}],
                },
                "vcObjects": {
                    "title": [{"properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": f"'{title}'"}}},
                    }}],
                    "background": [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}],
                    "border": [{"properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E0E0E0'"}}}}},
                        "radius": {"expr": {"Literal": {"Value": "4L"}}},
                    }}],
                },
            },
            "howCreated": "Copilot",
        }),
        "filters": "[]",
    }


def _bar(name, x, y, w, h, dim_table, dim_col, fact_table, measure, title, z=1):
    d_alias = dim_table[0].lower()
    f_alias = fact_table[0].lower()
    if d_alias == f_alias:
        f_alias = f_alias + "2"
    return {
        "x": x, "y": y, "z": z, "width": w, "height": h,
        "config": json.dumps({
            "name": name,
            "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": z, "width": w, "height": h}}],
            "singleVisual": {
                "visualType": "clusteredBarChart",
                "projections": {
                    "Category": [{"queryRef": f"{dim_table}.{dim_col}"}],
                    "Y": [{"queryRef": f"{fact_table}.{measure}"}],
                },
                "prototypeQuery": {
                    "Version": 2,
                    "From": [
                        {"Name": d_alias, "Entity": dim_table, "Type": 0},
                        {"Name": f_alias, "Entity": fact_table, "Type": 0},
                    ],
                    "Select": [
                        {
                            "Column": {"Expression": {"SourceRef": {"Source": d_alias}}, "Property": dim_col},
                            "Name": f"{dim_table}.{dim_col}", "NativeReferenceName": dim_col,
                        },
                        {
                            "Measure": {"Expression": {"SourceRef": {"Source": f_alias}}, "Property": measure},
                            "Name": f"{fact_table}.{measure}", "NativeReferenceName": measure,
                        },
                    ],
                    "OrderBy": [{
                        "Direction": 2,
                        "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": f_alias}}, "Property": measure}},
                    }],
                },
                "drillFilterOtherVisuals": True,
                "objects": {
                    "categoryAxis": [{"properties": {"showAxisTitle": {"expr": {"Literal": {"Value": "true"}}}}}],
                    "valueAxis": [{"properties": {"showAxisTitle": {"expr": {"Literal": {"Value": "true"}}}}}],
                },
                "vcObjects": {
                    "title": [{"properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": f"'{title}'"}}},
                    }}],
                },
            },
            "howCreated": "Copilot",
        }),
        "filters": "[]",
    }


def _textbox(name, x, y, w, h, text, font_size=20, z=0):
    return {
        "x": x, "y": y, "z": z, "width": w, "height": h,
        "config": json.dumps({
            "name": name,
            "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": z, "width": w, "height": h}}],
            "singleVisual": {
                "visualType": "textbox",
                "objects": {
                    "general": [{"properties": {
                        "paragraphs": [{"textRuns": [{
                            "value": text,
                            "textStyle": {"fontWeight": "bold", "fontSize": f"{font_size}px"},
                        }]}],
                    }}],
                },
            },
        }),
        "filters": "[]",
    }


# ── Build the report ─────────────────────────────────────────────

def build_report_json(state: dict, config: dict) -> dict:
    ws_name = config["workspace_name"]
    sm_name = "SM_SensorAnalytics"
    sm_id = state["semantic_model_id"]

    # ── Page 1: Sensor Overview ──────────────────────────────────
    p1_visuals = [
        _textbox("title1", 0, 0, 1280, 50, "Sensor Analytics — Overview", 22),
        # KPI cards
        _card("c_readings", 30, 60, 390, 120, "SensorReading", "Total Readings", "Total Readings", z=1),
        _card("c_anomaly", 450, 60, 390, 120, "SensorReading", "Anomaly Rate", "Anomaly Rate", z=2),
        _card("c_alerts", 870, 60, 380, 120, "SensorAlert", "Total Alerts", "Total Alerts", z=3),
        # Charts
        _bar("bar_zone_anomaly", 30, 200, 600, 240,
             "dim_zones", "zone_type", "SensorReading", "Anomaly Rate",
             "Anomaly Rate by Zone Type", z=4),
        _bar("bar_site_readings", 650, 200, 600, 240,
             "dim_sites", "site_name", "SensorReading", "Total Readings",
             "Readings per Site", z=5),
        _bar("bar_quality", 30, 460, 1220, 240,
             "dim_sensors", "sensor_type", "SensorReading", "Good Quality Rate",
             "Good Quality Rate by Sensor Type", z=6),
    ]

    # ── Page 2: Alerts & Details ─────────────────────────────────
    p2_visuals = [
        _textbox("title2", 0, 0, 1280, 50, "Sensor Analytics — Alerts", 22),
        _card("c_critical", 30, 60, 390, 120, "SensorAlert", "Critical Alerts", "Critical Alerts", z=1),
        _card("c_warning", 450, 60, 390, 120, "SensorAlert", "Warning Alerts", "Warning Alerts", z=2),
        _card("c_rate", 870, 60, 380, 120, "SensorAlert", "Alert Rate", "Alert Rate", z=3),
        _bar("bar_alerts_site", 30, 200, 1220, 240,
             "dim_sites", "site_name", "SensorAlert", "Total Alerts",
             "Total Alerts by Site", z=4),
        _bar("bar_avg_reading", 30, 460, 1220, 240,
             "dim_sites", "site_name", "SensorReading", "Avg Reading Value",
             "Avg Reading Value by Site", z=5),
    ]

    # ── Theme reference ──────────────────────────────────────────
    theme_name = "CY26SU02"
    report_config = {
        "version": "5.70",
        "themeCollection": {
            "baseTheme": {
                "name": theme_name,
                "version": {"visual": "2.6.0", "report": "3.1.0", "page": "2.3.0"},
                "type": 2,
            }
        },
        "activeSectionIndex": 0,
        "defaultDrillFilterOtherVisuals": True,
        "settings": {
            "useNewFilterPaneExperience": True,
            "allowChangeFilterTypes": True,
            "useStylableVisualContainerHeader": True,
            "exportDataMode": 1,
        },
    }

    report = {
        "config": json.dumps(report_config),
        "layoutOptimization": 0,
        "resourcePackages": [{
            "resourcePackage": {
                "name": "SharedResources", "type": 2,
                "items": [{"type": 202, "path": f"BaseThemes/{theme_name}.json", "name": theme_name}],
                "disabled": False,
            }
        }],
        "sections": [
            {
                "name": "SensorOverview",
                "displayName": "Sensor Overview",
                "displayOption": 1, "width": 1280, "height": 720,
                "config": json.dumps({"name": "SensorOverview"}),
                "filters": "[]",
                "visualContainers": p1_visuals,
            },
            {
                "name": "AlertsDetails",
                "displayName": "Alerts & Details",
                "displayOption": 1, "width": 1280, "height": 720,
                "config": json.dumps({"name": "AlertsDetails"}),
                "filters": "[]",
                "visualContainers": p2_visuals,
            },
        ],
        "theme": theme_name,
    }

    # ── definition.pbir ──────────────────────────────────────────
    conn_str = (
        f'Data Source="powerbi://api.powerbi.com/v1.0/myorg/{ws_name}";'
        f"initial catalog={sm_name};"
        f"integrated security=ClaimsToken;"
        f"semanticmodelid={sm_id}"
    )
    pbir = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {"byConnection": {"connectionString": conn_str}},
    }

    # ── Base theme (minimal) ─────────────────────────────────────
    base_theme = {
        "name": theme_name,
        "version": "5.70",
        "type": 2,
        "dataColors": [
            "#4E79A7", "#F28E2B", "#E15759", "#76B7B2",
            "#59A14F", "#EDC948", "#B07AA1", "#FF9DA7",
        ],
    }

    return report, pbir, base_theme, theme_name


def deploy_report():
    print(f"\n{'='*60}")
    print(f"  DEPLOY POWER BI REPORT: {REPORT_NAME}")
    print(f"{'='*60}\n")

    config = load_config()
    state = load_state()
    ws_id = state["workspace_id"]
    token = get_fabric_token()
    headers = fabric_headers(token)

    # Build report parts
    print("  Building report definition...")
    report_json, pbir, base_theme, theme_name = build_report_json(state, config)
    pages = len(report_json["sections"])
    visuals = sum(len(s["visualContainers"]) for s in report_json["sections"])
    print(f"  📊 {pages} pages, {visuals} visuals")

    # Encode parts
    parts = [
        {"path": "report.json", "payload": b64encode_json(report_json), "payloadType": "InlineBase64"},
        {"path": "definition.pbir", "payload": b64encode_json(pbir), "payloadType": "InlineBase64"},
        {
            "path": f"StaticResources/SharedResources/BaseThemes/{theme_name}.json",
            "payload": b64encode_json(base_theme),
            "payloadType": "InlineBase64",
        },
    ]

    # Check existing
    report_id = state.get("report_id")
    if not report_id:
        try:
            existing = find_item(token, API, ws_id, REPORT_NAME, "Report")
            report_id = existing["id"]
            print(f"  Found existing report: {report_id}")
        except RuntimeError:
            pass

    if report_id:
        # Update existing
        print(f"  Updating existing report: {report_id}")
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/reports/{report_id}/updateDefinition",
            headers=headers,
            json={"definition": {"parts": parts}},
        )
        if resp.status_code == 200:
            print("  ✅ Definition updated")
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            if op_id:
                poll_operation(token, API, op_id)
            print("  ✅ Definition updated (async)")
        else:
            print(f"  ❌ Update failed ({resp.status_code}): {resp.text[:300]}")
    else:
        # Create new
        print("  Creating report...")
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/items",
            headers=headers,
            json={
                "displayName": REPORT_NAME,
                "type": "Report",
                "description": "Sensor Analytics report — RTI Demo",
                "definition": {"parts": parts},
            },
        )
        if resp.status_code in (200, 201):
            report_id = resp.json()["id"]
            print(f"  ✅ Created: {report_id}")
        elif resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id", "")
            if op_id:
                poll_operation(token, API, op_id)
            result = find_item(token, API, ws_id, REPORT_NAME, "Report")
            report_id = result["id"]
            print(f"  ✅ Created: {report_id}")
        else:
            raise RuntimeError(f"Create report failed ({resp.status_code}): {resp.text[:300]}")

    state["report_id"] = report_id
    save_state(state)
    print(f"\n  Report ID: {report_id}")
    print(f"  Pages: {pages} | Visuals: {visuals}")
    print(f"  Model: SM_SensorAnalytics ({state['semantic_model_id']})")


if __name__ == "__main__":
    deploy_report()
