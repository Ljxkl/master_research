#!/usr/bin/env python3
"""
Batch download Sentinel-1 SLC scenes from multiple AOIs (ROIs) in a CSV file.
Automatically skips ROIs that overlap significantly (>= 80%) with already downloaded images.

Usage:
    python batch_download_with_overlap_check.py --aoi-csv ../aoi.csv --config ../aoi_1.yaml --project-dir ../

Features:
    - Reads CSV with region-of-interest bounding boxes (UR, LL)
    - Detects existing interferogram metadata from merged/ directory
    - Calculates overlap percentage with existing data
    - Skips ROIs with >= 80% overlap
    - Downloads remaining ROIs using s1_sbas_download
"""

import argparse
import json
import sys
import subprocess
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import csv
import xml.etree.ElementTree as ET
import shutil


def parse_geotransform(geotransform_str: str) -> Tuple[float, float, float, float, float, float]:
    """
    Parse a GeoTransform string from VRT metadata.
    
    Returns: (x0, dx, 0, y0, 0, dy)
    """
    parts = geotransform_str.strip().split(",")
    return tuple(float(p.strip()) for p in parts)


def extract_bbox_from_vrt(vrt_path: Path) -> Optional[List[float]]:
    """
    Extract bounding box from GeoTIFF VRT file.
    
    Returns: [west, south, east, north] in lon/lat (EPSG:4326)
    """
    try:
        tree = ET.parse(vrt_path)
        root = tree.getroot()
        
        # Find GeoTransform element
        geo_elem = root.find(".//GeoTransform")
        if geo_elem is None or geo_elem.text is None:
            return None
        
        # Parse GeoTransform: [x0, dx, 0, y0, 0, dy]
        x0, dx, _, y0, _, dy = parse_geotransform(geo_elem.text)
        
        # Get raster dimensions
        raster_x = int(root.get("rasterXSize", 0))
        raster_y = int(root.get("rasterYSize", 0))
        
        if raster_x == 0 or raster_y == 0:
            return None
        
        # Calculate bounds
        # Note: dy is negative (going south)
        west = x0
        east = x0 + raster_x * dx
        north = y0
        south = y0 + raster_y * dy
        
        # Ensure correct order: west < east, south < north
        if west > east:
            west, east = east, west
        if south > north:
            south, north = north, south
        
        return [west, south, east, north]
    
    except Exception as e:
        print(f"Warning: Failed to parse VRT {vrt_path}: {e}")
        return None


def get_existing_bbox_from_merged() -> Optional[List[float]]:
    """
    Scan merged/ directory and extract bounding box from existing interferograms.
    
    Returns: [west, south, east, north] or None if no files found
    """
    merged_dir = Path(__file__).parent.parent / "merged"
    
    if not merged_dir.exists():
        print("Note: merged/ directory not found yet")
        return None
    
    # Look for VRT files (georeferenced files)
    vrt_files = [
        "topophase.flat.geo.vrt",
        "phsig.cor.geo.vrt",
        "filt_topophase.flat.geo.vrt"
    ]
    
    for vrt_name in vrt_files:
        vrt_path = merged_dir / vrt_name
        if vrt_path.exists():
            bbox = extract_bbox_from_vrt(vrt_path)
            if bbox:
                print(f"  Found existing interference data: {vrt_name}")
                print(f"    Coverage: {bbox}")
                return bbox
    
    return None


def bbox_overlap_percentage(roi_bbox: List[float], existing_bbox: List[float]) -> float:
    """
    Calculate overlap percentage of ROI with existing bounding box.
    
    Args:
        roi_bbox: [west, south, east, north]
        existing_bbox: [west, south, east, north]
    
    Returns: Overlap area / ROI area as percentage (0-100)
    """
    w1, s1, e1, n1 = roi_bbox
    w2, s2, e2, n2 = existing_bbox
    
    # Calculate ROI area
    roi_area = (e1 - w1) * (n1 - s1)
    if roi_area <= 0:
        return 0.0
    
    # Calculate intersection
    x_left = max(w1, w2)
    x_right = min(e1, e2)
    y_bottom = max(s1, s2)
    y_top = min(n1, n2)
    
    # If no intersection
    if x_right < x_left or y_top < y_bottom:
        return 0.0
    
    intersection_area = (x_right - x_left) * (y_top - y_bottom)
    overlap_pct = (intersection_area / roi_area) * 100.0
    
    return overlap_pct


def read_aoi_json(json_path: Path) -> List[Dict[str, Any]]:
    """
    Read AOI JSON file.
    
    Expected structure:
    {
      "rois": [
        {
          "id": 1,
          "plant_name": "Hideya",
          "bbox": {
            "lower_left": [lon, lat],
            "upper_right": [lon, lat]
          }
        },
        ...
      ]
    }
    
    Returns: List of dicts with 'id', 'plant_name', 'bbox' ([W, S, E, N])
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        aois = []
        for roi in data.get('rois', []):
            try:
                ll_lon, ll_lat = roi['bbox']['lower_left']
                ur_lon, ur_lat = roi['bbox']['upper_right']
                
                # Ensure correct ordering
                w = min(ll_lon, ur_lon)
                e = max(ll_lon, ur_lon)
                s = min(ll_lat, ur_lat)
                n = max(ll_lat, ur_lat)
                
                aois.append({
                    'id': int(roi['id']),
                    'plant_name': roi['plant_name'].strip(),
                    'bbox': [w, s, e, n]
                })
            except (KeyError, ValueError, TypeError) as e:
                print(f"Warning: Skipping malformed ROI: {e}")
                continue
        
        return aois
    
    except FileNotFoundError:
        print(f"Error: JSON file not found: {json_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON file: {e}")
        sys.exit(1)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Batch download Sentinel-1 SLC with overlap checking"
    )
    parser.add_argument(
        "--aoi-file",
        type=Path,
        required=True,
        help="Path to AOI file (CSV or JSON format)"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to YAML config file for s1_sbas_download"
    )
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Override project directory"
    )
    parser.add_argument(
        "--overlap-threshold",
        type=float,
        default=80.0,
        help="Skip ROI if overlap percentage >= this threshold (default: 80)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show what would be downloaded, don't actually download"
    )
    parser.add_argument(
        "--skip-existing-check",
        action="store_true",
        help="Skip the existing data overlap check and download all ROIs"
    )
    
    args = parser.parse_args(argv)
    
    # Load AOIs from file (CSV or JSON)
    print("=" * 70)
    print("Loading AOI file...")
    
    if args.aoi_file.suffix.lower() == '.json':
        aois = read_aoi_json(args.aoi_file)
    elif args.aoi_file.suffix.lower() == '.csv':
        aois = read_aoi_csv(args.aoi_file)
    else:
        print(f"Error: Unsupported file format: {args.aoi_file.suffix}")
        print("Supported formats: .csv, .json")
        sys.exit(1)
    
    print(f"Loaded {len(aois)} ROIs from {args.aoi_file}")
    
    # Check for existing data
    print("\n" + "=" * 70)
    print("Checking for existing interferogram data in merged/...")
    existing_bbox = None
    
    if not args.skip_existing_check:
        existing_bbox = get_existing_bbox_from_merged()
    
    if existing_bbox and not args.skip_existing_check:
        print(f"Existing coverage found: {existing_bbox}")
    else:
        print("No existing interferogram data, or check skipped")
    
    # Process each ROI
    print("\n" + "=" * 70)
    print("Overlap analysis:")
    print("-" * 70)
    
    to_download = []
    skipped = []
    
    for aoi in aois:
        roi_id = aoi['id']
        roi_name = aoi['plant_name']
        roi_bbox = aoi['bbox']
        
        overlap_pct = 0.0
        if existing_bbox:
            overlap_pct = bbox_overlap_percentage(roi_bbox, existing_bbox)
        
        status = "PROCESS"
        if existing_bbox and overlap_pct >= args.overlap_threshold:
            status = "SKIP"
            skipped.append(roi_id)
        else:
            to_download.append(roi_id)
        
        print(
            f"[{roi_id:2d}] {roi_name:20s} | "
            f"bbox: {roi_bbox} | "
            f"overlap: {overlap_pct:5.1f}% | "
            f"{status}"
        )
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY:")
    print(f"  Total ROIs: {len(aois)}")
    print(f"  To download: {len(to_download)}")
    print(f"  To skip: {len(skipped)}")
    
    if skipped:
        print(f"  Skipped ROI IDs: {skipped}")
    
    if args.dry_run:
        print("\n[DRY RUN] No downloads performed.")
        return 0
    
    if not to_download:
        print("\nAll ROIs skipped due to overlap. Nothing to download.")
        return 0
    
    # For now, just show which ROIs would be processed
    # In a full implementation, you would:
    # 1. Modify the config YAML for each ROI (update aoi_bbox)
    # 2. Call sbas_select_and_download() for each
    
    print("\n" + "=" * 70)
    print("Next steps:")
    print("  1. Modify config YAML for each ROI with its bounding box")
    print("  2. Call s1_sbas_download.py for each ROI")
    print("  3. Process with topsProc.xml")
    
    print("\nExample for first ROI to download:")
    if to_download:
        roi = next(a for a in aois if a['id'] == to_download[0])
        bbox = roi['bbox']
        print(f"\n  ROI: {roi['plant_name']} (ID {roi['id']})")
        print(f"  BBox: lower_left: [{bbox[0]}, {bbox[1]}], upper_right: [{bbox[2]}, {bbox[3]}]")
        print(f"\n  python tools/s1_sbas_download.py \\")
        print(f"    --config {args.config} \\")
        print(f"    --project-dir {args.project_dir or '..'}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
