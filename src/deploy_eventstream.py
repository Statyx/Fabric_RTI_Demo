#!/usr/bin/env python3
"""
Step 4: Create an EventStream that ingests Custom App data
and routes it to the Eventhouse KQL tables.
"""

import json
import sys
import time

import requests

from helpers import (
    load_config, load_state, save_state,
    get_fabric_token, fabric_headers, create_fabric_item,
    print_step,
)


def main():
    config = load_config()
    state = load_state()
    api = config["fabric_api_base"]

    ws_id = state.get("workspace_id")
    if not ws_id:
        print("❌ Workspace not created yet. Run deploy_workspace.py first.")
        sys.exit(1)

    eh_id = state.get("eventhouse_id")
    kql_db_id = state.get("kql_database_id")
    if not eh_id or not kql_db_id:
        print("❌ Eventhouse not created yet. Run deploy_eventhouse.py first.")
        sys.exit(1)

    token = get_fabric_token()
    es_name = config["eventstream_name"]

    # --- Create EventStream ---
    if state.get("eventstream_id"):
        print(f"✅ EventStream already exists: {state['eventstream_id']}")
    else:
        print_step(1, 2, f"Creating EventStream: {es_name}")
        es = create_fabric_item(
            token, api, ws_id,
            es_name, "Eventstream",
            "Ingests real-time sensor data from Custom App source"
        )
        state["eventstream_id"] = es["id"]
        save_state(state)
        print(f"✅ EventStream created: {es['id']}")

    # --- Configure Custom App source + KQL destination ---
    print_step(2, 2, "EventStream configuration note")
    print("""
  The EventStream has been created. To complete the wiring:

  Option A — Portal (recommended for demo):
    1. Open the EventStream in Fabric portal
    2. Add source → "Custom App" (this gives you an endpoint URL + key)
    3. Add destination → "Eventhouse" → select KQL Database
    4. Map to SensorReading and SensorAlert tables

  Option B — Use the data injector (inject_data.py):
    The injector script sends data directly to the Eventhouse
    using Kusto streaming ingestion, bypassing EventStream.
    This is simpler for automated demos.

  The Custom App endpoint (if configured) will be saved in state.json.
  You can retrieve it from the portal's EventStream → Custom App source.
""")

    # Try to get the EventStream details for the Custom App endpoint
    headers = fabric_headers(token)
    try:
        resp = requests.get(
            f"{api}/workspaces/{ws_id}/eventstreams/{state['eventstream_id']}",
            headers=headers
        )
        if resp.status_code == 200:
            es_detail = resp.json()
            print(f"  EventStream details: {json.dumps(es_detail, indent=2)}")
    except Exception:
        pass

    print("\n✅ EventStream created. Configure source/destination in portal.")


if __name__ == "__main__":
    main()
