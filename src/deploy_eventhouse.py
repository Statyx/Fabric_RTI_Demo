#!/usr/bin/env python3
"""
Step 3: Create the Eventhouse + KQL Database, then create KQL tables.
"""

import sys
import time

import requests

from helpers import (
    load_config, load_state, save_state,
    get_fabric_token, get_kusto_token,
    fabric_headers, create_fabric_item,
    kusto_mgmt, print_step,
)


def wait_for_kql_database(token: str, api: str, ws_id: str,
                          eh_name: str, max_retries: int = 20) -> dict:
    """Wait for the auto-created KQL Database to be ready."""
    headers = fabric_headers(token)
    for attempt in range(max_retries):
        resp = requests.get(
            f"{api}/workspaces/{ws_id}/items?type=KQLDatabase",
            headers=headers
        )
        resp.raise_for_status()
        for db in resp.json().get("value", []):
            if db["displayName"] == eh_name:
                return db
        print(f"  ⏳ Attempt {attempt+1}/{max_retries} — KQL Database not ready, waiting 10s...")
        time.sleep(10)
    raise RuntimeError(f"KQL Database '{eh_name}' not provisioned after {max_retries * 10}s")


def get_query_service_uri(token: str, api: str, ws_id: str,
                          eventhouse_id: str) -> str:
    """Get the Kusto query service URI from the Eventhouse."""
    headers = fabric_headers(token)
    resp = requests.get(
        f"{api}/workspaces/{ws_id}/eventhouses/{eventhouse_id}",
        headers=headers
    )
    resp.raise_for_status()
    return resp.json()["properties"]["queryServiceUri"]


def create_kql_tables(config: dict, query_service_uri: str,
                      kusto_token: str, db_name: str):
    """Create KQL tables using .create-merge table commands."""
    for table_key, table_def in config["kql_tables"].items():
        table_name = table_def["name"]
        cols = ", ".join(
            f"{c['name']}:{c['type']}" for c in table_def["columns"]
        )
        cmd = f".create-merge table {table_name} ({cols})"
        print(f"  📋 Creating table: {table_name}")
        kusto_mgmt(query_service_uri, kusto_token, db_name, cmd)
        print(f"     ✅ {table_name} ready")


def main():
    config = load_config()
    state = load_state()
    api = config["fabric_api_base"]

    ws_id = state.get("workspace_id")
    if not ws_id:
        print("❌ Workspace not created yet. Run deploy_workspace.py first.")
        sys.exit(1)

    token = get_fabric_token()
    eh_name = config["eventhouse_name"]

    # --- Create Eventhouse ---
    if state.get("eventhouse_id"):
        print(f"✅ Eventhouse already exists: {state['eventhouse_id']}")
    else:
        print_step(1, 3, f"Creating Eventhouse: {eh_name}")
        eh = create_fabric_item(
            token, api, ws_id,
            eh_name, "Eventhouse",
            "Real-time sensor telemetry streaming data"
        )
        state["eventhouse_id"] = eh["id"]
        save_state(state)
        print(f"✅ Eventhouse created: {eh['id']}")

    # --- Wait for KQL Database ---
    print_step(2, 3, "Waiting for KQL Database...")
    kql_db = wait_for_kql_database(token, api, ws_id, eh_name)
    state["kql_database_id"] = kql_db["id"]

    # Get query service URI
    query_uri = get_query_service_uri(
        token, api, ws_id, state["eventhouse_id"]
    )
    state["query_service_uri"] = query_uri
    save_state(state)
    print(f"✅ KQL Database ready: {kql_db['id']}")
    print(f"   Query URI: {query_uri}")

    # --- Create KQL Tables ---
    print_step(3, 3, "Creating KQL tables...")
    kusto_token = get_kusto_token(query_uri)

    # Wait briefly for KQL DB to be fully operational
    print("  ⏳ Waiting 15s for KQL Database to be fully operational...")
    time.sleep(15)

    create_kql_tables(config, query_uri, kusto_token, eh_name)

    # Enable streaming ingestion on both tables
    for table_key, table_def in config["kql_tables"].items():
        table_name = table_def["name"]
        cmd = f".alter table {table_name} policy streamingingestion enable"
        try:
            kusto_mgmt(query_uri, kusto_token, eh_name, cmd)
            print(f"  🔄 Streaming ingestion enabled on {table_name}")
        except Exception as e:
            print(f"  ⚠ Could not enable streaming on {table_name}: {e}")

    print("\n✅ Eventhouse deployed with KQL tables.")


if __name__ == "__main__":
    main()
