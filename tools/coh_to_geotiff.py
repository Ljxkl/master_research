#!/usr/bin/env python3
"""
Convert ISCE2 coherence images to GeoTIFF format.
"""
import argparse
import sys
from pathlib import Path
import subprocess

try:
    from osgeo import gdal
except ImportError:
    print("ERROR: GDAL Python bindings not found. Install with: pip install GDAL", file=sys.stderr)
    sys.exit(1)

def coh_to_geotiff(coh_file, output_file=None, scale=255):
    """
    Convert ISCE2 coherence file to GeoTIFF.
    
    Args:
        coh_file: Path to .cor or .vrt file
        output_file: Output GeoTIFF path (auto-generated if None)
        scale: Scale factor (0-1 to 0-255 or 0-1000, etc.)
    """
    coh_path = Path(coh_file)
    
    # Find corresponding .vrt file
    if coh_path.suffix == '.cor':
        vrt_file = coh_path.with_suffix('.cor.vrt')
    elif coh_path.suffix == '.vrt':
        vrt_file = coh_path
    else:
        print(f"ERROR: Expected .cor or .vrt file, got: {coh_path}", file=sys.stderr)
        return False
    
    if not vrt_file.exists():
        print(f"ERROR: VRT file not found: {vrt_file}", file=sys.stderr)
        return False
    
    # Generate output filename
    if output_file is None:
        output_file = coh_path.parent / f"{coh_path.stem}.tif"
    
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Converting: {vrt_file}")
    print(f"Output: {output_path}")
    
    try:
        # Open source dataset
        src_ds = gdal.Open(str(vrt_file))
        if src_ds is None:
            print(f"ERROR: Cannot open {vrt_file}", file=sys.stderr)
            return False
        
        # Get band
        band = src_ds.GetRasterBand(1)
        
        # Create output dataset
        driver = gdal.GetDriverByName('GTiff')
        options = ['COMPRESS=LZW', 'TILED=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256']
        
        dst_ds = driver.CreateCopy(str(output_path), src_ds, options=options)
        if dst_ds is None:
            print(f"ERROR: Cannot create output file: {output_path}", file=sys.stderr)
            return False
        
        # Close datasets
        band = None
        src_ds = None
        dst_ds = None
        
        print(f"✓ Successfully created: {output_path}")
        return True
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Convert ISCE2 coherence images to GeoTIFF format"
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Input .cor or .vrt file"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output GeoTIFF file (auto-generated if not specified)"
    )
    parser.add_argument(
        "-d", "--directory",
        help="Directory containing multiple .cor files (batch mode)"
    )
    
    args = parser.parse_args()
    
    # Batch mode
    if args.directory:
        input_dir = Path(args.directory)
        cor_files = list(input_dir.glob("*.cor")) + list(input_dir.glob("*.vrt"))
        
        if not cor_files:
            print(f"ERROR: No .cor or .vrt files found in {input_dir}", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found {len(cor_files)} files to convert")
        success_count = 0
        
        for cor_file in cor_files:
            if coh_to_geotiff(cor_file):
                success_count += 1
            print()
        
        print(f"Converted {success_count}/{len(cor_files)} files")
        sys.exit(0 if success_count == len(cor_files) else 1)
    
    # Single file mode
    if not coh_to_geotiff(args.input, args.output):
        sys.exit(1)

if __name__ == "__main__":
    main()
