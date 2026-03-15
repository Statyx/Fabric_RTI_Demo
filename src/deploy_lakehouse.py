#!/usr/bin/env python3
"""
Step 2: Create the Lakehouse and upload referential CSV data as Delta tables.
"""

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
        print_step(1, 3, f"Creating Lakehouse: {config['lakehouse_name']}")
        lh = create_fabric_item(
            token, api, ws_id,
            config["lakehouse_name"], "Lakehouse",
            "Referential data for IoT sensors (sites, zones, sensors)"
        )
        state["lakehouse_id"] = lh["id"]
        save_state(state)
        print(f"✅ Lakehouse created: {lh['id']}")

    # --- Wait for SQL Endpoint ---
    print_step(2, 3, "Waiting for SQL Endpoint provisioning...")
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
    print_step(3, 3, "Uploading referential CSVs to OneLake...")
    storage_token = get_storage_token()
    csv_files = ["dim_sites.csv", "dim_zones.csv", "dim_sensors.csv"]
    for csv_file in csv_files:
        upload_csv_to_onelake(ws_id, state["lakehouse_id"],
                              csv_file, storage_token)

    print("\n✅ Lakehouse deployed with referential data.")
    print("   ⚠ You need to run a Spark notebook to convert CSVs to Delta tables.")
    print("   See docs/fabric_setup.md for instructions.")


if __name__ == "__main__":
    main()
