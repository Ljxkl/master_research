#!/bin/bash
# batch_download_orbits.sh

DATA_DIR="/home/jmrsl/research/data"
OUTPUT_DIR="${DATA_DIR}/orbits"

mkdir -p "$OUTPUT_DIR"

# .SAFE ディレクトリまたは .zip ファイルを処理
for file in "$DATA_DIR"/*.SAFE "$DATA_DIR"/*.zip; do
    [ -e "$file" ] || continue
    echo "Processing: $(basename $file)"
    python tools/orbit.py -i "$file" -o "$OUTPUT_DIR"
done

echo "All orbit files downloaded to: $OUTPUT_DIR"