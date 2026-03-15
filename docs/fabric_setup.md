# Fabric RTI Demo — Manual Portal Steps

## 1. Convert CSVs to Delta Tables (Notebook)

After deploy_lakehouse.py uploads CSVs to OneLake, you need a Spark notebook
to convert them to Delta tables.

### Create Notebook in Portal

1. Go to workspace "CDR - Fabric RTI Demo"
2. New → Notebook
3. Attach to the Lakehouse "LH_SensorReference"
4. Run this PySpark code:

```python
# Cell 1: Convert all CSVs to Delta tables
import os

csv_files = ["dim_sites", "dim_zones", "dim_sensors"]

for table_name in csv_files:
    csv_path = f"Files/{table_name}.csv"
    delta_path = f"Tables/{table_name}"

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"✅ {table_name}: {df.count()} rows → Delta")
```

## 2. EventStream Configuration

1. Open EventStream "ES_SensorIngestion"
2. **Add Source** → Custom App
   - This creates an endpoint URL + SAS key
   - Save the endpoint URL for the data injector
3. **Add Destination** → Eventhouse
   - Select KQL Database "EH_SensorTelemetry"
   - Map to table "SensorReading"

## 3. KQL Dashboard (Optional)

Create a Real-Time Dashboard with tiles:

### Suggested Tiles

1. **Live Reading Count** (last 5 min)
   ```kql
   SensorReading | where Timestamp > ago(5m) | count
   ```

2. **Anomaly Rate**
   ```kql
   SensorReading
   | where Timestamp > ago(1h)
   | summarize TotalReadings = count(), Anomalies = countif(IsAnomaly)
   | extend AnomalyRate = round(100.0 * Anomalies / TotalReadings, 2)
   ```

3. **Temperature by Site (time chart)**
   ```kql
   SensorReading
   | where SensorType == "Temperature" and Timestamp > ago(1h)
   | summarize AvgTemp = avg(ReadingValue) by bin(Timestamp, 1m), SiteId
   | render timechart
   ```

4. **Active Alerts**
   ```kql
   SensorAlert
   | where Timestamp > ago(30m)
   | summarize AlertCount = count() by Severity
   | render piechart
   ```

5. **Top Alerting Sensors**
   ```kql
   SensorAlert
   | where Timestamp > ago(1h)
   | summarize AlertCount = count() by SensorId, SensorType
   | top 10 by AlertCount desc
   ```

6. **Readings Heatmap by Zone**
   ```kql
   SensorReading
   | where Timestamp > ago(1h) and SensorType == "Temperature"
   | summarize AvgValue = avg(ReadingValue) by ZoneId
   | render columnchart
   ```
