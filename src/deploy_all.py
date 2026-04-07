#!/usr/bin/env python3
"""
Master deployment script — runs all steps in order.
Idempotent: skips steps that already completed (via state.json).
"""

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
STEPS = [
    ("generate_data.py",          "Generate referential CSV data"),
    ("deploy_workspace.py",       "Create Fabric workspace"),
    ("deploy_lakehouse.py",       "Create Lakehouse + upload CSVs"),
    ("deploy_eventhouse.py",      "Create Eventhouse + KQL tables"),
    ("deploy_eventstream.py",     "Create EventStream"),
    ("deploy_materialized_views.py", "Create KQL materialized views (5 pre-aggregations)"),
    ("deploy_semantic_model.py",  "Deploy Semantic Model (Direct Lake)"),
    ("deploy_data_agent.py",      "Deploy Data Agent (AI Skill)"),
    ("deploy_kql_dashboard.py",   "Deploy KQL Dashboard (2 pages, 14 tiles)"),
    ("deploy_report.py",          "Deploy Power BI Report (2 pages, 13 visuals)"),
]


def main():
    print("=" * 60)
    print("🚀 Fabric RTI Demo — Full Deployment")
    print("=" * 60)

    python = sys.executable

    for i, (script, description) in enumerate(STEPS, 1):
        print(f"\n{'='*60}")
        print(f"Step {i}/{len(STEPS)}: {description}")
        print(f"{'='*60}")

        script_path = SCRIPT_DIR / script
        result = subprocess.run(
            [python, str(script_path)],
            cwd=str(SCRIPT_DIR)
        )
        if result.returncode != 0:
            print(f"\n❌ Step {i} failed. Fix the issue and re-run deploy_all.py")
            sys.exit(1)

    print(f"\n{'='*60}")
    print("✅ All deployment steps completed!")
    print(f"{'='*60}")
    print("""
Next steps:
  1. Open Fabric portal → workspace "CDR - Fabric RTI Demo"
  2. Configure EventStream source (Custom App) + destination (Eventhouse)
  3. Run Notebook NB_Setup_Lakehouse to convert CSVs → Delta tables
  4. Run the data injector:
     python inject_data.py --duration 300    # 5 minutes of live data
     python inject_data.py                   # continuous
  5. Open RTI_SensorDashboard for real-time KQL tiles
  6. Open RPT_SensorAnalytics for Power BI report
  7. Chat with SensorAnalytics_Agent in Fabric portal
""")


if __name__ == "__main__":
    main()
