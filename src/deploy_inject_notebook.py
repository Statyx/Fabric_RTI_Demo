#!/usr/bin/env python3
"""
Deploy NB_Inject_Data: convert local ipynb → Fabric .py, push, and run.

Uses notebook_utils for reliable conversion (fixes %pip, blank lines, .py format).
"""

import json
import sys
from pathlib import Path

from helpers import load_state, save_state, get_fabric_token
from notebook_utils import (
    ipynb_to_fabric_py, push_notebook, create_notebook,
    recreate_notebook, run_notebook,
)

SCRIPT_DIR = Path(__file__).parent


def main():
    state = load_state()
    ws_id = state["workspace_id"]
    ipynb_path = SCRIPT_DIR.parent / "notebooks" / "inject_data.ipynb"
    nb_name = "NB_Inject_Data"

    token = get_fabric_token()

    # Convert ipynb → Fabric .py (auto-fixes %pip, blank lines)
    print(f"📝 Converting {ipynb_path.name} → .py format...")
    py_content = ipynb_to_fabric_py(str(ipynb_path))
    print(f"  {len(py_content)} chars")

    nb_id = state.get("notebook_inject_data_id")

    if nb_id:
        # Update existing notebook
        print(f"📓 Updating {nb_name} ({nb_id})...")
        push_notebook(ws_id, nb_id, py_content, token)
        print(f"  ✅ Updated")
    else:
        # Create new notebook
        print(f"📓 Creating {nb_name}...")
        nb_id = create_notebook(ws_id, nb_name, py_content, token)
        state["notebook_inject_data_id"] = nb_id
        save_state(state)
        print(f"  ✅ Created: {nb_id}")

    # Run the notebook
    print(f"\n🚀 Running {nb_name}...")
    result = run_notebook(ws_id, nb_id, token)
    print(f"  ✅ Notebook completed!")


if __name__ == "__main__":
    main()
