#!/bin/bash
# Helper script to run batch download with overlap checking
# 使用方法: ./check_and_download_rois.sh [--dry-run] [--skip-check] [--threshold 80]

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_DIR="${SCRIPT_DIR}"

# デフォルト値
DRY_RUN=""
SKIP_CHECK=""
THRESHOLD="80"

# 引数パース
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --skip-check)
            SKIP_CHECK="--skip-existing-check"
            shift
            ;;
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "ROI 重複チェック + Sentinel-1 ダウンロード"
echo "=========================================="
echo "Project directory: $PROJECT_DIR"
echo "Overlap threshold: $THRESHOLD%"
[[ -n "$DRY_RUN" ]] && echo "Mode: DRY RUN (確認のみ)"
[[ -n "$SKIP_CHECK" ]] && echo "Mode: SKIP EXISTING CHECK（かぶり確認なし）"
echo ""

# Python スクリプトの実行
python3 "${PROJECT_DIR}/tools/batch_download_with_overlap_check.py" \
    --aoi-file "${PROJECT_DIR}/aoi.json" \
    --config "${PROJECT_DIR}/conf/aoi_1.yaml" \
    --project-dir "${PROJECT_DIR}" \
    --overlap-threshold "${THRESHOLD}" \
    ${DRY_RUN} \
    ${SKIP_CHECK}

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "✓ 処理完了"
else
    echo ""
    echo "✗ エラーが発生しました（終了コード: $exit_code）"
fi

exit $exit_code
