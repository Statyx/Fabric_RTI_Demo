"""Diagnose Eventhouse row counts, date ranges, and policies."""
import json
import requests
from helpers import load_config, load_state, get_kusto_token

state = load_state()
config = load_config()
query_uri = state["query_service_uri"]
db_name = config["eventhouse_name"]
token = get_kusto_token(query_uri)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

queries = [
    ("SensorReading count", "SensorReading | count"),
    ("SensorAlert count", "SensorAlert | count"),
    ("Date range", "SensorReading | summarize min(Timestamp), max(Timestamp)"),
    ("By SiteId", "SensorReading | summarize count() by SiteId | order by count_ desc"),
    ("By SensorType", "SensorReading | summarize count() by SensorType | order by count_ desc"),
    ("Alerts by Severity", "SensorAlert | summarize count() by Severity"),
    ("Retention policy", ".show table SensorReading policy retention"),
    ("Caching policy", ".show table SensorReading policy caching"),
    ("Extents", ".show table SensorReading extents | summarize count(), sum(RowCount), min(MinCreatedOn), max(MaxCreatedOn)"),
]

with open("_diag_output.txt", "w") as f:
    for label, q in queries:
        f.write(f"\n--- {label}: {q}\n")
        endpoint = "/v1/rest/mgmt" if q.startswith(".") else "/v1/rest/query"
        body = {"db": db_name, "csl": q}
        try:
            r = requests.post(f"{query_uri}{endpoint}", headers=headers, json=body, timeout=30)
            if r.status_code == 200:
                data = r.json()
                tables = data.get("Tables", data.get("tables", []))
                if tables:
                    t = tables[0]
                    cols = [c.get("ColumnName", c.get("name", "?")) for c in t.get("Columns", t.get("columns", []))]
                    rows = t.get("Rows", t.get("rows", []))
                    f.write(f"  Columns: {cols}\n")
                    for row in rows[:10]:
                        f.write(f"  {row}\n")
            else:
                f.write(f"  ERROR {r.status_code}: {r.text[:300]}\n")
        except Exception as e:
            f.write(f"  EXCEPTION: {e}\n")
    f.write("\nDone.\n")

print("Output written to _diag_output.txt")
