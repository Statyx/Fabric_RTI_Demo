# Copilot Instructions — Fabric RTI Demo

## Mandatory Testing Gate

Before running ANY `deploy_*.py` or artifact generator:
```bash
python -m pytest tests/test_smoke.py tests/test_report_visuals.py -v --tb=short
```
If ANY test fails → **STOP. Fix the code first. Do not proceed.**

If `tests/test_smoke.py` does not exist → create it first using the testing-agent template
from `Github_Brain/agents/testing-agent/instructions.md`.

## Report Visual Feedback Loop

The shared validator lives at `Github_Brain/agents/testing-agent/visual_validator.py`.
Run `test_report_visuals.py` before every report deployment.

## Project Context

- Python 3.12, Windows
- Real-time IoT streaming pipeline: EventStream → KQL Database → Semantic Model → Report
- 2-page Power BI report (legacy PBIX format)
- Data Agent with DAX fewshots
- Deployment order: workspace → lakehouse → eventhouse → eventstream → materialized views → model → report → agent

## Known Pitfalls

- EventStream destination itemId = KQL Database ID, NOT Eventhouse ID
- Kusto streaming policy must be enabled before ingestion
- prototypeQuery MANDATORY on every data visual
- Report config/filters must be stringified JSON
