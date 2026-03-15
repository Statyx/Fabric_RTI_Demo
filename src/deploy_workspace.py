#!/usr/bin/env python3
"""
Step 1: Create the Fabric workspace for RTI Demo.
"""

import sys
import requests
from helpers import (
    load_config, load_state, save_state,
    get_fabric_token, fabric_headers, print_step,
)


def main():
    config = load_config()
    state = load_state()
    api = config["fabric_api_base"]

    if state.get("workspace_id"):
        print(f"✅ Workspace already exists: {state['workspace_id']}")
        return

    print_step(1, 1, f"Creating workspace: {config['workspace_name']}")
    token = get_fabric_token()
    headers = fabric_headers(token)

    body = {
        "displayName": config["workspace_name"],
        "capacityId": config["capacity_id"],
    }
    resp = requests.post(f"{api}/workspaces", headers=headers, json=body)

    if resp.status_code in (200, 201):
        ws = resp.json()
        state["workspace_id"] = ws["id"]
        save_state(state)
        print(f"✅ Workspace created: {ws['id']}")
    elif resp.status_code == 409:
        # Already exists — find it
        print("  ⚠ Workspace already exists, looking it up...")
        resp2 = requests.get(f"{api}/workspaces", headers=headers)
        resp2.raise_for_status()
        for ws in resp2.json().get("value", []):
            if ws["displayName"] == config["workspace_name"]:
                state["workspace_id"] = ws["id"]
                save_state(state)
                print(f"✅ Found existing workspace: {ws['id']}")
                return
        print("❌ Workspace exists but could not find it")
        sys.exit(1)
    else:
        print(f"❌ Failed ({resp.status_code}): {resp.text}")
        sys.exit(1)


if __name__ == "__main__":
    main()
