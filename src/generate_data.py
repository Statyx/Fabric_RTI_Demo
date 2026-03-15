#!/usr/bin/env python3
"""
Generate referential CSV data for the IoT Sensor RTI demo.
Produces: dim_sites.csv, dim_zones.csv, dim_sensors.csv
"""

import csv
import uuid
import yaml
from pathlib import Path
from typing import Dict, List, Any

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "config.yaml"
DATA_DIR = SCRIPT_DIR.parent / "data" / "raw"


def load_config() -> Dict[str, Any]:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def generate_sites(config: Dict) -> List[Dict]:
    print("🏭 Generating sites...")
    sites = []
    for i in range(config["sites"]["count"]):
        sites.append({
            "site_id": f"SITE_{i+1:03d}",
            "site_name": config["sites"]["names"][i],
            "region": config["sites"]["regions"][i],
            "country": config["sites"]["countries"][i],
            "latitude": round(43.3 + i * 2.0, 4),
            "longitude": round(2.3 + i * 1.5, 4),
            "is_active": "true",
        })
    print(f"  ✓ {len(sites)} sites")
    return sites


def generate_zones(config: Dict, sites: List[Dict]) -> List[Dict]:
    print("📍 Generating zones...")
    zones = []
    zone_types = config["zones"]["types"]
    for site in sites:
        for j in range(config["zones"]["per_site"]):
            zone_type = zone_types[j % len(zone_types)]
            zones.append({
                "zone_id": f"{site['site_id']}_Z{j+1:02d}",
                "zone_name": f"{zone_type} - {site['site_name']}",
                "zone_type": zone_type,
                "site_id": site["site_id"],
                "floor": j // 2 + 1,
                "area_sqm": round(100 + j * 50 + len(site["site_name"]) * 3, 1),
                "is_active": "true",
            })
    print(f"  ✓ {len(zones)} zones")
    return zones


def generate_sensors(config: Dict, zones: List[Dict]) -> List[Dict]:
    print("📡 Generating sensors...")
    sensors = []
    sensor_types = config["sensors"]["types"]
    sensor_count = 0
    for zone in zones:
        for k in range(config["sensors"]["per_zone"]):
            stype = sensor_types[k % len(sensor_types)]
            sensor_count += 1
            sensors.append({
                "sensor_id": f"SN_{sensor_count:04d}",
                "sensor_name": f"{stype['name']} Sensor {sensor_count}",
                "sensor_type": stype["name"],
                "unit": stype["unit"],
                "zone_id": zone["zone_id"],
                "site_id": zone["site_id"],
                "min_normal": stype["min_normal"],
                "max_normal": stype["max_normal"],
                "min_critical": stype["min_critical"],
                "max_critical": stype["max_critical"],
                "install_date": "2024-06-15",
                "is_active": "true",
            })
    print(f"  ✓ {len(sensors)} sensors")
    return sensors


def write_csv(data: List[Dict], filepath: Path):
    if not data:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  📁 Written: {filepath.name} ({len(data)} rows)")


def main():
    print("=" * 60)
    print("🔧 RTI Demo — Referential Data Generator")
    print("=" * 60)

    config = load_config()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    sites = generate_sites(config)
    write_csv(sites, DATA_DIR / "dim_sites.csv")

    zones = generate_zones(config, sites)
    write_csv(zones, DATA_DIR / "dim_zones.csv")

    sensors = generate_sensors(config, zones)
    write_csv(sensors, DATA_DIR / "dim_sensors.csv")

    print(f"\n✅ Done! {len(sites)} sites, {len(zones)} zones, {len(sensors)} sensors")
    print(f"   Files in: {DATA_DIR}")


if __name__ == "__main__":
    main()
