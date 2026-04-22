#!/usr/bin/env python3
"""
Convert AOI CSV file to JSON format.

Usage:
    python csv_to_json.py aoi.csv aoi.json
"""

import csv
import json
import sys
from pathlib import Path
from typing import List, Dict, Any


def csv_to_json(csv_path: Path, json_path: Path) -> None:
    """
    Convert AOI CSV to JSON format.

    Args:
        csv_path: Path to input CSV file
        json_path: Path to output JSON file
    """
    aois = []

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    aoi = {
                        "id": int(row['id']),
                        "plant_name": row['plant_name'].strip(),
                        "bbox": {
                            "lower_left": [
                                float(row['LL_lon']),
                                float(row['LL_lat'])
                            ],
                            "upper_right": [
                                float(row['UR_lon']),
                                float(row['UR_lat'])
                            ]
                        }
                    }
                    aois.append(aoi)
                except (ValueError, KeyError) as e:
                    print(f"Warning: Skipping malformed row: {e}")
                    continue

        # Sort by ID
        aois.sort(key=lambda x: x['id'])

        # Create JSON structure
        json_data = {
            "metadata": {
                "source": str(csv_path),
                "total_rois": len(aois),
                "description": "AOI (Area of Interest) definitions for Sentinel-1 processing"
            },
            "rois": aois
        }

        # Write JSON file
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Converted {len(aois)} ROIs from {csv_path} to {json_path}")

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print("Usage: python csv_to_json.py <input.csv> <output.json>")
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    json_path = Path(sys.argv[2])

    csv_to_json(csv_path, json_path)


if __name__ == "__main__":
    main()
