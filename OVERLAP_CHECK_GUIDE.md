# 重複チェック付きSentinel-1シーン一括ダウンロード

## 概要

`aoi.csv` に リストアップされた複数の ROI（関心領域）に対して、Sentinel-1 SLC（Single Look Complex）シーンを自動でダウンロードするツールです。

**主な機能：**
- ✅ CSV ファイルから ROI のバウンディングボックスを読み込む
- ✅ `merged/` フォルダから既にダウンロード済みのシーンを自動検出
- ✅ 各 ROI と既存データの重複率を計算
- ✅ 重複率 ≥ 80% の ROI は**自動的にスキップ**
- ✅ 重複がない ROI のみをダウンロード対象として提示

---

## セットアップ

### 修正事項

aoi.csv に **1 行の修正**が必要でした：

```diff
- 5,Nantan135.483326,35.092747,135.496074,35.102199
+ 5,Nantan,135.483326,35.092747,135.496074,35.102199
```

（コンマの欠落を修正）

---

## CSV → JSON 変換

CSV ファイルを JSON 形式に変換できます：

```bash
python tools/csv_to_json.py aoi.csv aoi.json
```

**JSON 構造例：**
```json
{
  "metadata": {
    "source": "aoi.csv",
    "total_rois": 99,
    "description": "AOI definitions for Sentinel-1 processing"
  },
  "rois": [
    {
      "id": 1,
      "plant_name": "Hideya",
      "bbox": {
        "lower_left": [134.366044, 34.98955],
        "upper_right": [134.375079, 34.995826]
      }
    }
  ]
}
```

## 実行方法

### 1. CSV ファイルを使用する場合

```bash
python tools/batch_download_with_overlap_check.py \
  --aoi-file aoi.csv \
  --config conf/aoi_1.yaml \
  --overlap-threshold 80 \
  --dry-run
```

### 2. JSON ファイルを使用する場合（推奨）

```bash
python tools/batch_download_with_overlap_check.py \
  --aoi-file aoi.json \
  --config conf/aoi_1.yaml \
  --overlap-threshold 80 \
  --dry-run
```

**出力例：**
```
Total ROIs: 99
To download: 76
To skip: 23
Skipped ROI IDs: [3, 9, 13, 18, 21, 28, 43, 52, 67, 68, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 99, 100]
```

### 2. 重複チェックなしで全て処理したい場合

```bash
python tools/batch_download_with_overlap_check.py \
  --aoi-csv aoi.csv \
  --config conf/aoi_1.yaml \
  --skip-existing-check \
  --dry-run
```

### 3. 重複率の閾値を変更したい場合

```bash
python tools/batch_download_with_overlap_check.py \
  --aoi-csv aoi.csv \
  --config conf/aoi_1.yaml \
  --overlap-threshold 50 \
  --dry-run
```

---

## 処理結果の説明

### 現在のテスト実行結果

**既存のカバレッジ範囲：**
```
[137.8, 34.708, 140.912, 36.581]  (西, 南, 東, 北の経緯度)
```

**分析結果：**
- 総 ROI 数：**99 個**
- ダウンロード対象：**76 個**（重複率 < 80%）
- スキップ対象：**23 個**（100% 重複）

### スキップされて ROI（既存カバレッジ内に完全に含まれる）

| ID  | ROI名 | 重複率 |
|-----|-------|--------|
| 3   | Ohmachi | 100% |
| 9   | Kasumigaura | 100% |
| 13  | Tsutiura | 100% |
| 18  | Katsuura | 100% |
| 21  | Katsuura | 100% |
| 28  | Sano | 100% |
| 43  | Mobara | 100% |
| 52  | Taura | 100% |
| 67  | Kasama | 100% |
| 68  | Yasunaka | 100% |
| 82  | Kamogawa | 100% |
| 83  | Kushihama | 100% |
| 84  | Onjyuku | 100% |
| 85  | FS_Japan | 100% |
| 86  | Yoshizawa | 100% |
| 87  | NRE-23 | 100% |
| 88  | SGET_Kisarazu | 100% |
| 89  | Tyounan | 100% |
| 90  | Hannou | 100% |
| 91  | Naka | 100% |
| 92  | Ushiku | 100% |
| 99  | CSKasama | 100% |
| 100 | Namegata | 100% |

---

## アルゴリズム

### 重複率の計算

```python
def bbox_overlap_percentage(roi_bbox, existing_bbox):
    # 重複エリアを計算
    intersection_area = (東 - 西) × (北 - 南)
    
    # ROI の面積を計算
    roi_area = (ROI_東 - ROI_西) × (ROI_北 - ROI_南)
    
    # 重複率を計算
    overlap_pct = (intersection_area / roi_area) × 100%
    
    # 80% 以上なら SKIP
    if overlap_pct >= 80:
        return SKIP
    else:
        return PROCESS
```

### バウンディングボックスの抽出方法

`merged/*.vrt` ファイルから GeoTransform を読み込み：

```xml
<GeoTransform>137.8, 0.000277..., 0.0, 36.580..., 0.0, -0.000277...</GeoTransform>
```

これを使用して、ピクセル座標を地理座標に変換：
- 経度：137.8 + (ピクセルX × 0.000277)
- 緯度：36.580 + (ピクセルY × (-0.000277))

---

## 使用技術

- **Python 3.6+**
- 標準ライブラリのみ（外部依存なし）
- XML パーサ（メタデータ読み込み）
- CSV 読み込み・処理

---

## トラブルシューティング

### Q: スクリプトを実行するとエラーが出る

**A:** CSV ファイルの形式を確認してください：
```bash
head -n 10 aoi.csv
```

期待される形式：
```
id,plant_name,LL_lon,LL_lat,UR_lon,UR_lat
1,Hideya,134.366044,34.989550,134.375079,34.995826
...
```

### Q: 既存データが検出されない

**A:** `merged/` フォルダに `.vrt` ファイルが必要です。確認：
```bash
ls -la merged/*.vrt
```

### Q: 重複率の計算がおかしい

**A:** 座標系が一致しているか確認（EPSG:4326 経度・緯度）

---

## 次のステップ（今後の実装予定）

1. **YAML 設定ファイルの自動生成**
   - 各 ROI の bbox を config に自動設定

2. **s1_sbas_download.py の自動実行**
   - 各 ROI ごとにダウンロード処理を逐次実行

3. **ダウンロード履歴ログの記録**
   - どの ROI をいつダウンロードしたかを記録

4. **並列処理**
   - 複数 ROI の同時ダウンロード

---

## ファイル一覧

```
tools/
├── batch_download_with_overlap_check.py  ← メインスクリプト（CSV/JSON対応）
├── csv_to_json.py                        ← CSV → JSON 変換ツール
├── s1_sbas_download.py                   ← Sentinel-1 ダウンロード実行
└── orbit.py

aoi.csv                                    ← ROI 定義（CSV形式）
aoi.json                                   ← ROI 定義（JSON形式）
conf/
├── aoi_1.yaml                            ← ダウンロード設定
check_and_download_rois.sh                ← 実行用シェルスクリプト
OVERLAP_CHECK_GUIDE.md                    ← このドキュメント
```

## 質問・フィードバック

スクリプトに改善が必要な場合は、以下をお知らせください：

- 追加したい機能
- エラーメッセージ
- 処理時間の改善要望
