"""
ISCE2 topsApp 用の入力ファイル自動生成パイプライン

s1_aoi_grouper.py の結果 JSON と s1_download_from_groups.py の出力先を読み、
各グループに対して:
  1. 各 SLC の EOF オービットを取得 (tools/orbit.py)
  2. グループ bbox から DEM を作成 (dem.py -a stitch)
  3. 連続ペア (N-1 個) ごとに reference.xml / secondary.xml / topsApp.xml を生成

topsApp.py の実行自体は行いません (入力だけ準備)。

使い方:
    python s1_isce2_pipeline.py <grouping_result.json> \
        --root /path/to/data_root [--groups 0,2-3] [--dry-run]
"""

import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple


def _rel(target: Path, base: Path) -> str:
    return os.path.relpath(str(target), str(base))

TOOLS_DIR = Path(__file__).resolve().parent
ORBIT_PY = TOOLS_DIR / "orbit.py"

DEFAULT_DEM_URL = "https://step.esa.int/auxdata/dem/SRTMGL1/"

DEFAULT_SWATHS = "[1, 2, 3]"
DEFAULT_RANGE_LOOKS = 9
DEFAULT_AZIMUTH_LOOKS = 3
DEFAULT_DO_UNWRAP = "False"


def _scene_date(scene_id: str) -> str:
    for part in scene_id.split("_"):
        if len(part) >= 15 and part[8:9] == "T" and part[:8].isdigit():
            return part[:8]
    raise ValueError(f"日付抽出に失敗: {scene_id}")


def _scene_track(scene_id: str) -> str:
    """取得時刻 HHMM をトラック識別子として返す (同じ track なら同じ HHMM)"""
    for part in scene_id.split("_"):
        if len(part) >= 15 and part[8:9] == "T":
            return part[9:13]
    return "0000"


def _find_slc(data_dir: Path, scene_id: str) -> Optional[Path]:
    for cand in (data_dir / f"{scene_id}.zip", data_dir / f"{scene_id}.SAFE"):
        if cand.exists():
            return cand
    return None


def _consecutive_pairs(scene_ids: List[str]) -> List[Tuple[str, str]]:
    """トラックごとに分けて、トラック内で日付順の隣接ペアを作る"""
    from collections import defaultdict
    by_track = defaultdict(list)
    for sid in scene_ids:
        by_track[_scene_track(sid)].append(sid)
    pairs = []
    for track, ids in sorted(by_track.items()):
        ids.sort(key=lambda s: (_scene_date(s), s))
        pairs.extend(list(zip(ids[:-1], ids[1:])))
    return pairs


def _ensure_orbits(data_dir: Path, orbit_dir: Path,
                   slc_paths: List[Path], dry_run: bool) -> None:
    orbit_dir.mkdir(parents=True, exist_ok=True)
    existing = {p.name for p in orbit_dir.glob("*.EOF")}
    for slc in slc_paths:
        if any(slc.stem.split("_")[-4][:8] in n for n in existing):
            print(f"    orbit: skip ({slc.name}) 既存あり")
            continue
        cmd = [sys.executable, str(ORBIT_PY), "-i", str(slc), "-o", str(orbit_dir)]
        print(f"    orbit: {' '.join(cmd)}")
        if dry_run:
            continue
        subprocess.run(cmd, check=True)
        existing = {p.name for p in orbit_dir.glob("*.EOF")}


def _dem_bbox_int(bbox: dict) -> Tuple[int, int, int, int]:
    w, s = bbox["lower_left"]
    e, n = bbox["upper_right"]
    south = int(math.floor(s))
    north = int(math.ceil(n))
    west = int(math.floor(w))
    east = int(math.ceil(e))
    if north == south:
        north = south + 1
    if east == west:
        east = west + 1
    return south, north, west, east


def _dem_stem(south: int, north: int, west: int, east: int) -> str:
    def ns(x: int) -> str:
        return f"{'S' if x < 0 else 'N'}{abs(x):02d}"

    def we(x: int) -> str:
        return f"{'W' if x < 0 else 'E'}{abs(x):03d}"

    return f"demLat_{ns(south)}_{ns(north)}_Lon_{we(west)}_{we(east)}"


def _find_dem(dem_dir: Path, stem: str) -> Optional[Path]:
    """既存 DEM を検索。.dem.wgs84 優先、無ければ .dem を使う。.xml が揃っていること"""
    for suffix in (".dem.wgs84", ".dem"):
        main = dem_dir / f"{stem}{suffix}"
        if main.exists() and (dem_dir / f"{stem}{suffix}.xml").exists():
            return main
    return None


def _ensure_dem(dem_dir: Path, bbox: dict, dem_url: str,
                dry_run: bool) -> Optional[Path]:
    south, north, west, east = _dem_bbox_int(bbox)
    dem_dir.mkdir(parents=True, exist_ok=True)
    stem = _dem_stem(south, north, west, east)

    existing = _find_dem(dem_dir, stem)
    if existing:
        print(f"    dem : skip ({existing.name}) 既存あり")
        return existing

    if shutil.which("dem.py") is None and not dry_run:
        print("    ⚠️  dem.py が PATH にありません (ISCE2 env が未アクティブ)")
        print(f"       手動で: dem.py -a stitch -b {south} {north} {west} {east} "
              f"-s 1 -r -u {dem_url}")
        return None

    cmd = [
        "dem.py", "-a", "stitch",
        "-b", str(south), str(north), str(west), str(east),
        "-s", "1", "-r", "-u", dem_url,
    ]
    print(f"    dem : (cwd={dem_dir}) {' '.join(cmd)}")
    if dry_run:
        return dem_dir / f"{stem}.dem"
    subprocess.run(cmd, cwd=str(dem_dir), check=True)
    found = _find_dem(dem_dir, stem)
    if found is None:
        print(f"    ⚠️  DEM 生成後に {stem}.dem[.wgs84] が見つかりません")
    return found


def _roi_string(bbox: dict) -> str:
    w, s = bbox["lower_left"]
    e, n = bbox["upper_right"]
    return f"[{s}, {n}, {w}, {e}]"


_PAIR_TAG_RE = re.compile(r"^\d{8}_\d{8}$")


def _pair_tag(ref_id: str, sec_id: str) -> str:
    return f"{_scene_date(ref_id)}_{_scene_date(sec_id)}"


def _write_component_xml(path: Path, safe_rel: str, orbit_rel: str,
                         output_name: str) -> None:
    content = (
        f'<component name="{output_name}">\n'
        f'    <property name="orbit directory">{orbit_rel}</property>\n'
        f'    <property name="output directory">{output_name}</property>\n'
        f'    <property name="safe">{safe_rel}</property>\n'
        f'</component>\n'
    )
    path.write_text(content, encoding="utf-8")


def _write_topsapp_xml(path: Path, ref_rel: str, sec_rel: str,
                       roi: str, dem_rel: Optional[str],
                       swaths: str, range_looks: int,
                       azimuth_looks: int, do_unwrap: str) -> None:
    dem_prop = (f'    <property name="demFilename">{dem_rel}</property>\n'
                if dem_rel else "")
    content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<topsApp>\n'
        '  <component name="topsinsar">\n'
        '    <property name="Sensor name">SENTINEL1</property>\n'
        f'    <component name="reference">\n'
        f'      <catalog>{ref_rel}</catalog>\n'
        f'      <property name="output directory">reference</property>\n'
        f'    </component>\n'
        f'    <component name="secondary">\n'
        f'      <catalog>{sec_rel}</catalog>\n'
        f'      <property name="output directory">secondary</property>\n'
        f'    </component>\n'
        f'    <property name="swaths">{swaths}</property>\n'
        f'    <property name="region of interest">{roi}</property>\n'
        f'    <property name="do unwrap">{do_unwrap}</property>\n'
        f'    <property name="range looks">{range_looks}</property>\n'
        f'    <property name="azimuth looks">{azimuth_looks}</property>\n'
        f'{dem_prop}'
        '  </component>\n'
        '</topsApp>\n'
    )
    path.write_text(content, encoding="utf-8")


def _parse_group_ids(spec: str) -> set:
    ids = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            ids.update(range(int(a), int(b) + 1))
        else:
            ids.add(int(part))
    return ids


def build_group(group: dict, root: Path, dem_url: str,
                dry_run: bool) -> None:
    gid = group["group_id"]
    group_root = root / f"group_{gid:02d}"
    data_dir = group_root / "data"
    orbit_dir = group_root / "orbit"
    dem_dir = group_root / "dem"
    input_root = group_root / "input-file"
    isce2_root = group_root / "isce2"

    print("-" * 70)
    print(f"グループ {gid}  ({', '.join(group.get('aoi_names', []))})")
    print(f"  root: {group_root}")

    scene_ids: List[str] = group.get("scene_ids", [])
    if len(scene_ids) < 2:
        print(f"  スキップ: ペア化にはシーン >= 2 必要 (現 {len(scene_ids)})")
        return

    # ローカルSLC 解決
    slc_map = {}
    missing = []
    for sid in scene_ids:
        p = _find_slc(data_dir, sid)
        if p is None:
            missing.append(sid)
        else:
            slc_map[sid] = p
    if missing:
        print(f"  ⚠️  data/ に見つからない SLC ({len(missing)}):")
        for m in missing[:3]:
            print(f"     - {m}")
        if not slc_map:
            return
    usable = [s for s in scene_ids if s in slc_map]
    pairs = _consecutive_pairs(usable)
    print(f"  ペア数: {len(pairs)} (入力 {len(usable)})")

    _ensure_orbits(data_dir, orbit_dir, list(slc_map.values()), dry_run)

    bbox = group["bbox"]
    if isinstance(bbox, list):
        bbox = {"lower_left": bbox[:2], "upper_right": bbox[2:]}
    dem_path = _ensure_dem(dem_dir, bbox, dem_url, dry_run)
    roi = _roi_string(bbox)

    for ref_id, sec_id in pairs:
        tag = _pair_tag(ref_id, sec_id)
        pair_input = input_root / tag
        pair_output = isce2_root / tag
        pair_input.mkdir(parents=True, exist_ok=True)
        pair_output.mkdir(parents=True, exist_ok=True)

        ref_xml = pair_input / "reference.xml"
        sec_xml = pair_input / "secondary.xml"
        tops_xml = pair_input / "topsApp.xml"

        print(f"    pair {tag}: {ref_xml.relative_to(group_root)}")
        if dry_run:
            continue

        # 実行 CWD = pair_output (isce2/<tag>/) からの相対パス
        ref_safe_rel = _rel(slc_map[ref_id], pair_output)
        sec_safe_rel = _rel(slc_map[sec_id], pair_output)
        orbit_rel = _rel(orbit_dir, pair_output)
        dem_rel = _rel(dem_path, pair_output) if dem_path else None
        ref_xml_rel = _rel(ref_xml, pair_output)
        sec_xml_rel = _rel(sec_xml, pair_output)

        _write_component_xml(ref_xml, ref_safe_rel, orbit_rel, "reference")
        _write_component_xml(sec_xml, sec_safe_rel, orbit_rel, "secondary")
        _write_topsapp_xml(
            tops_xml, ref_xml_rel, sec_xml_rel, roi, dem_rel,
            DEFAULT_SWATHS, DEFAULT_RANGE_LOOKS, DEFAULT_AZIMUTH_LOOKS,
            DEFAULT_DO_UNWRAP,
        )


def main():
    parser = argparse.ArgumentParser(
        description="s1_aoi_grouper.py 結果から ISCE2 topsApp 入力を生成"
    )
    parser.add_argument("grouping_json", type=Path,
                        help="s1_aoi_grouper.py の出力 JSON")
    parser.add_argument("--root", type=Path, required=True,
                        help="group_XX/ が並ぶルート (s1_download_from_groups.py の --out-dir と同じ)")
    parser.add_argument("--groups", type=str, default=None,
                        help="対象 group_id (例: '0,2-3')")
    parser.add_argument("--dem-url", type=str, default=DEFAULT_DEM_URL,
                        help=f"dem.py -u に渡す URL (デフォルト: {DEFAULT_DEM_URL})")
    parser.add_argument("--dry-run", action="store_true",
                        help="コマンドと生成先を表示のみ")
    args = parser.parse_args()

    if not args.grouping_json.exists():
        print(f"❌ JSON が見つかりません: {args.grouping_json}")
        return 1
    args.root = args.root.resolve()
    if not args.root.exists():
        print(f"❌ --root が存在しません: {args.root}")
        return 1

    with open(args.grouping_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    groups = data.get("groups", [])
    if args.groups:
        ids = _parse_group_ids(args.groups)
        groups = [g for g in groups if g["group_id"] in ids]
    if not groups:
        print("❌ 対象グループがありません")
        return 1

    print("=" * 70)
    print("🛠  ISCE2 パイプライン (入力生成のみ)")
    print("=" * 70)
    print(f"入力 : {args.grouping_json}")
    print(f"root : {args.root}")
    print(f"対象 : {[g['group_id'] for g in groups]}  dry-run: {args.dry_run}")
    print()

    for g in groups:
        build_group(g, args.root, args.dem_url, args.dry_run)

    print()
    print("=" * 70)
    print("✓ 完了")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    exit(main())
