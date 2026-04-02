#!/usr/bin/env python3
"""
Step 2: Create the Lakehouse, upload referential CSV data, and convert to Delta tables.
"""

import base64
import csv
import sys
import time
from pathlib import Path

import requests

from helpers import (
    load_config, load_state, save_state,
    get_fabric_token, fabric_headers, create_fabric_item,
    find_item, print_step,
)

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data" / "raw"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


def get_storage_token() -> str:
    import subprocess
    return subprocess.check_output(
        ["az", "account", "get-access-token",
         "--resource", "https://storage.azure.com",
         "--query", "accessToken", "-o", "tsv"],
        shell=True
    ).decode().strip()


def upload_csv_to_onelake(workspace_id: str, lakehouse_id: str,
                          filename: str, storage_token: str):
    """Upload a CSV file to the Lakehouse Files/ folder via OneLake DFS API."""
    filepath = DATA_DIR / filename
    if not filepath.exists():
        print(f"  ⚠ {filename} not found, skipping")
        return

    with open(filepath, "rb") as f:
        data = f.read()

    # Step 1: Create file (empty)
    url = (f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}"
           f"/Files/{filename}?resource=file")
    headers = {
        "Authorization": f"Bearer {storage_token}",
        "Content-Type": "application/octet-stream",
    }
    resp = requests.put(url, headers=headers)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Create file failed ({resp.status_code}): {resp.text}")

    # Step 2: Append data
    url_append = (f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}"
                  f"/Files/{filename}?action=append&position=0")
    resp = requests.patch(url_append, headers=headers, data=data)
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Append failed ({resp.status_code}): {resp.text}")

    # Step 3: Flush
    url_flush = (f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}"
                 f"/Files/{filename}?action=flush&position={len(data)}")
    resp = requests.patch(url_flush, headers=headers)
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Flush failed ({resp.status_code}): {resp.text}")

    print(f"  📤 Uploaded {filename} ({len(data)} bytes)")


def main():
    config = load_config()
    state = load_state()
    api = config["fabric_api_base"]

    ws_id = state.get("workspace_id")
    if not ws_id:
        print("❌ Workspace not created yet. Run deploy_workspace.py first.")
        sys.exit(1)

    token = get_fabric_token()

    # --- Create Lakehouse ---
    if state.get("lakehouse_id"):
        print(f"✅ Lakehouse already exists: {state['lakehouse_id']}")
    else:
        print_step(1, 5, f"Creating Lakehouse: {config['lakehouse_name']}")
        lh = create_fabric_item(
            token, api, ws_id,
            config["lakehouse_name"], "Lakehouse",
            "Referential data for IoT sensors (sites, zones, sensors)"
        )
        state["lakehouse_id"] = lh["id"]
        save_state(state)
        print(f"✅ Lakehouse created: {lh['id']}")

    # --- Wait for SQL Endpoint ---
    print_step(2, 5, "Waiting for SQL Endpoint provisioning...")
    headers = fabric_headers(token)
    for attempt in range(20):
        resp = requests.get(
            f"{api}/workspaces/{ws_id}/items?type=SQLEndpoint",
            headers=headers
        )
        resp.raise_for_status()
        endpoints = [
            ep for ep in resp.json().get("value", [])
            if ep["displayName"] == config["lakehouse_name"]
        ]
        if endpoints:
            state["sql_endpoint_id"] = endpoints[0]["id"]
            save_state(state)
            print(f"✅ SQL Endpoint ready: {endpoints[0]['id']}")
            break
        print(f"  ⏳ Attempt {attempt+1}/20 — waiting 10s...")
        time.sleep(10)
    else:
        print("⚠ SQL Endpoint not ready yet — continue anyway, it will appear later.")

    # --- Upload CSVs ---
    print_step(3, 5, "Uploading referential CSVs to OneLake...")
    storage_token = get_storage_token()
    csv_files = ["dim_sites.csv", "dim_zones.csv", "dim_sensors.csv"]
    for csv_file in csv_files:
        upload_csv_to_onelake(ws_id, state["lakehouse_id"],
                              csv_file, storage_token)

    # --- Create / update NB_Setup_Lakehouse notebook ---
    print_step(4, 5, "Deploying CSV-to-Delta notebook...")
    nb_name = "NB_Setup_Lakehouse"
    nb_content = _build_setup_notebook()
    nb_b64 = base64.b64encode(nb_content.encode("utf-8")).decode("utf-8")
    nb_definition = {
        "parts": [
            {"path": "notebook-content.py", "payload": nb_b64,
             "payloadType": "InlineBase64"}
        ]
    }

    if state.get("notebook_setup_lakehouse_id"):
        nb_id = state["notebook_setup_lakehouse_id"]
        print(f"  Updating existing notebook {nb_id}...")
        resp = requests.post(
            f"{api}/workspaces/{ws_id}/notebooks/{nb_id}/updateDefinition",
            headers=headers, json={"definition": nb_definition}, timeout=60,
        )
        if resp.status_code == 202:
            _poll_location(resp, headers)
    else:
        try:
            nb_item = find_item(token, api, ws_id, nb_name, "Notebook")
            nb_id = nb_item["id"]
            print(f"  Found existing notebook {nb_id}, updating...")
            resp = requests.post(
                f"{api}/workspaces/{ws_id}/notebooks/{nb_id}/updateDefinition",
                headers=headers, json={"definition": nb_definition}, timeout=60,
            )
            if resp.status_code == 202:
                _poll_location(resp, headers)
        except RuntimeError:
            print(f"  Creating notebook {nb_name}...")
            body = {
                "displayName": nb_name,
                "type": "Notebook",
                "definition": nb_definition,
            }
            resp = requests.post(
                f"{api}/workspaces/{ws_id}/items",
                headers=headers, json=body, timeout=60,
            )
            if resp.status_code in (200, 201):
                nb_id = resp.json()["id"]
            elif resp.status_code == 202:
                nb_id = _poll_location(resp, headers)
                if not nb_id:
                    nb_item = find_item(token, api, ws_id, nb_name, "Notebook")
                    nb_id = nb_item["id"]
            else:
                raise RuntimeError(f"Create notebook failed ({resp.status_code}): {resp.text}")

        state["notebook_setup_lakehouse_id"] = nb_id
        save_state(state)

    print(f"  ✅ Notebook ready: {nb_id}")

    # --- Run the notebook to convert CSVs → Delta ---
    print_step(5, 5, "Running notebook to convert CSVs to Delta tables...")
    _run_notebook(api, ws_id, nb_id, headers)

    print("\n✅ Lakehouse deployed — Delta tables created.")
    print("   Tables: dim_sites, dim_zones, dim_sensors")


def _build_setup_notebook() -> str:
    """Generate the Fabric .py notebook content for CSV-to-Delta conversion.
    Uses relative paths — the notebook must be attached to the target Lakehouse."""
    return '''# Fabric notebook source


# MARKDOWN ********************

# # Setup Lakehouse — CSV to Delta Tables
#
# Auto-generated by deploy_lakehouse.py.
# Uses the default Lakehouse attached to this notebook.

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, DateType,
)

FILES = "Files"
TABLES = "Tables"

# CELL ********************

# --- dim_sites ---
schema_sites = StructType([
    StructField("site_id", StringType()),
    StructField("site_name", StringType()),
    StructField("region", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("is_active", BooleanType()),
])
df = spark.read.format("csv").option("header", "true").schema(schema_sites).load(f"{FILES}/dim_sites.csv")
df.write.format("delta").mode("overwrite").save(f"{TABLES}/dim_sites")
print(f"dim_sites: {df.count()} rows")

# CELL ********************

# --- dim_zones ---
schema_zones = StructType([
    StructField("zone_id", StringType()),
    StructField("zone_name", StringType()),
    StructField("zone_type", StringType()),
    StructField("site_id", StringType()),
    StructField("floor", IntegerType()),
    StructField("area_sqm", DoubleType()),
    StructField("is_active", BooleanType()),
])
df = spark.read.format("csv").option("header", "true").schema(schema_zones).load(f"{FILES}/dim_zones.csv")
df.write.format("delta").mode("overwrite").save(f"{TABLES}/dim_zones")
print(f"dim_zones: {df.count()} rows")

# CELL ********************

# --- dim_sensors ---
schema_sensors = StructType([
    StructField("sensor_id", StringType()),
    StructField("sensor_name", StringType()),
    StructField("sensor_type", StringType()),
    StructField("unit", StringType()),
    StructField("zone_id", StringType()),
    StructField("site_id", StringType()),
    StructField("min_normal", DoubleType()),
    StructField("max_normal", DoubleType()),
    StructField("min_critical", DoubleType()),
    StructField("max_critical", DoubleType()),
    StructField("install_date", DateType()),
    StructField("is_active", BooleanType()),
])
df = spark.read.format("csv").option("header", "true").schema(schema_sensors).load(f"{FILES}/dim_sensors.csv")
df.write.format("delta").mode("overwrite").save(f"{TABLES}/dim_sensors")
print(f"dim_sensors: {df.count()} rows")

# CELL ********************

# --- Verification ---
for t in ["dim_sites", "dim_zones", "dim_sensors"]:
    df = spark.read.format("delta").load(f"{TABLES}/{t}")
    print(f"{t}: {df.count()} rows, {len(df.columns)} columns")
print("All Delta tables ready.")
'''


def _poll_location(resp, headers, max_wait=180):
    """Poll an async 202 response until completion. Returns result ID if available."""
    op_url = resp.headers.get("Location")
    if not op_url:
        return None
    retry = int(resp.headers.get("Retry-After", "5"))
    time.sleep(retry)
    for _ in range(max_wait // 5):
        r = requests.get(op_url, headers=headers, timeout=30)
        data = r.json()
        status = data.get("status", "?")
        if status == "Succeeded":
            print(f"  ✅ Operation succeeded")
            return data.get("resourceId")
        if status in ("Failed", "Cancelled"):
            raise RuntimeError(f"Operation {status}: {data}")
        time.sleep(5)
    raise TimeoutError("Operation did not complete in time")


def _run_notebook(api: str, ws_id: str, nb_id: str, headers: dict,
                  max_wait: int = 600):
    """Trigger a Fabric notebook run and poll until complete."""
    url = f"{api}/workspaces/{ws_id}/items/{nb_id}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(url, headers=headers, timeout=60)

    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Run notebook failed ({resp.status_code}): {resp.text}")

    if resp.status_code == 200:
        print("  ✅ Notebook completed synchronously")
        return

    location = resp.headers.get("Location")
    retry = int(resp.headers.get("Retry-After", "10"))
    print(f"  ⏳ Notebook running (polling every {retry}s, max {max_wait}s)...")
    time.sleep(retry)

    for elapsed in range(0, max_wait, retry):
        r = requests.get(location, headers=headers, timeout=30)
        if r.status_code == 200:
            data = r.json()
            status = data.get("status", "?")
            print(f"  📊 Status: {status} ({elapsed + retry}s)")
            if status == "Completed":
                print("  ✅ Notebook run completed — Delta tables created")
                return
            if status in ("Failed", "Cancelled", "Deduped"):
                raise RuntimeError(f"Notebook run {status}: {data}")
        time.sleep(retry)

    raise TimeoutError(f"Notebook run did not complete in {max_wait}s")


if __name__ == "__main__":
    main()
