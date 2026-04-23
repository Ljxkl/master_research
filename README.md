# Sentinel-1 InSAR パイプライン

太陽光発電所 AOI を対象に Sentinel-1 SLC を取得し、ISCE2 topsApp で干渉処理を行う
パイプライン。

## ディレクトリ構造

```
master_research/
├── aoi.csv                            # AOI 一覧 (pipeline 入力)
├── conf/grouper.yaml                  # グルーパー設定
├── tools/
│   ├── s1_aoi_grouper.py              # ①  AOI をグループ化
│   ├── s1_download_from_groups.py     # ②  SLC をダウンロード
│   ├── s1_isce2_pipeline.py           # ③  orbit + DEM + ISCE2 入力生成
│   ├── s1_isce2_run.py                # ④  topsApp.py 実行
│   └── orbit.py                       # ③ が内部で呼ぶ
├── .state/                            # グルーパー結果 (gitignore)
└── data/                              # パイプライン出力 (gitignore)
    └── group_XX/
        ├── data/                      # SLC .zip
        ├── orbit/                     # EOF
        ├── dem/                       # DEM
        ├── input-file/<pair>/         # ISCE2 入力 XML
        └── isce2/<pair>/              # topsApp 出力
```

## 前提環境

ISCE2 を含む conda 環境 (名前例: `isce2`) を作成しておく:

```bash
conda create -n isce2 -c conda-forge isce2 pandas asf_search pyyaml requests -y
```

`~/.netrc` に Earthdata 資格情報を書く (ダウンロードに必須):

```
machine urs.earthdata.nasa.gov login <user> password <pass>
```

```bash
chmod 600 ~/.netrc
```

## 実行 (全 4 ステップ)

すべて `master_research/` を cwd にして実行する。

```bash
conda activate isce2
```

### ① グループ化

```bash
python tools/s1_aoi_grouper.py conf/grouper.yaml
```

出力: `.state/aoi_grouping_result.json`

### ② ダウンロード

特定グループのみ:

```bash
python tools/s1_download_from_groups.py \
  .state/aoi_grouping_result.json \
  --out-dir data --groups 0
```

- `--groups '0,2-4'` でカンマ / 範囲指定
- `--processes N` で並列 DL (デフォルト 4)
- `--dry-run` で取得予定のみ表示

### ③ ISCE2 入力生成

orbit EOF 取得、DEM stitch、ペア毎の `reference.xml` / `secondary.xml` / `topsApp.xml` を生成 (相対パス)。

```bash
python tools/s1_isce2_pipeline.py \
  .state/aoi_grouping_result.json \
  --root data --groups 0
```

- 各グループは同じ (path, frame) なので、日付順で連続ペア化 (track 混在なし)
- DEM は group bbox を整数度に丸めて `dem.py -a stitch` を呼出、既存なら skip
- 相対パスで XML を生成 → ディレクトリごと他マシンに移しても動作

### ④ topsApp 実行

```bash
python tools/s1_isce2_run.py --root data --groups 0
```

- 各 `isce2/<pair>/` を cwd に topsApp.py を実行
- DEM を pair dir に自動 symlink
- `--workers N` で並列実行 (デフォルト 2)
- `--dry-run`, `--pairs`, `--stop-on-error` 等あり

## 設定ファイル (`conf/grouper.yaml`)

| キー | 説明 |
|---|---|
| `aoi_csv` | AOI CSV パス (デフォルト `aoi.csv`) |
| `date_start` / `date_end` | 検索期間 |
| `orbit_direction` | `ASCENDING` / `DESCENDING` |
| `platform` | `A` / `B` / `C` / `all` / リスト |
| `max_scenes_per_group` | グループあたり上限 (先頭から 12 日間隔) |
| `dry_run` | true で JSON サマリ出力のみ |

## グルーピングの仕様

1. AOI ごとに ASF へ問い合わせ、**AOI を完全に包含する** SLC を抽出
2. それらを `(path_number, flight_direction, frame_number)` ごとに InSAR 可能スタックへ分類 (scene 数 < 2 のスタックは除外)
3. 全 AOI を最小本数のスタックでカバーするよう greedy set cover で採用スタックを決定 → 採用 1 スタック = 1 グループ
4. グループ内 AOI は同じ track + frame なので scene 集合も同一 (共通 DL 可能)

InSAR 成立スタックが無い AOI は警告付きでスキップ。

## 典型的なトラブルシュート

- **0 シーン / スタックなし**: `orbit_direction` を反転 (日本では DESC が有利な場合が多い)。また AOI が scene 境界にあると完全包含 scene がなくなる
- **GDAL: DEM .vrt not found**: DEM symlink が作られていない (`s1_isce2_run.py` が自動処理、古い input-file を再生成)
- **dem.py / topsApp.py が見つからない**: `conda activate isce2` していない
- **No space left on device**: `data/group_*/isce2/<pair>/` の中間成果物、不要な `data/group_*/dem/` を削除

## ライセンス

内部研究用。
