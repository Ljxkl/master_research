"""
各グループの input-file/<pair>/topsApp.xml を実行。
出力は isce2/<pair>/ 配下に作られる。

前提:
    conda activate isce2  (topsApp.py が PATH に居ること)

使い方:
    python s1_isce2_run.py --root data [--groups 0,2-3] [--dry-run]
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional


GROUP_RE = re.compile(r"^group_(\d+)$")
PAIR_RE = re.compile(r"^\d{8}_\d{8}$")


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


def _find_group_dirs(root: Path, filter_ids: Optional[set]) -> List[Path]:
    dirs = []
    for p in sorted(root.iterdir()):
        m = GROUP_RE.match(p.name)
        if not m:
            continue
        gid = int(m.group(1))
        if filter_ids is not None and gid not in filter_ids:
            continue
        dirs.append(p)
    return dirs


def _find_pairs(group_dir: Path) -> List[str]:
    inp = group_dir / "input-file"
    if not inp.is_dir():
        return []
    pairs = []
    for p in sorted(inp.iterdir()):
        if p.is_dir() and PAIR_RE.match(p.name) and (p / "topsApp.xml").exists():
            pairs.append(p.name)
    return pairs


def _is_done(pair_out_dir: Path) -> bool:
    """干渉処理が完了しているかの簡易判定"""
    return (pair_out_dir / "merged" / "topophase.flat").exists() or \
           (pair_out_dir / "merged" / "filt_topophase.flat").exists()


def _symlink_dem(topsapp_xml: Path, work_dir: Path) -> None:
    """topsApp.xml の demFilename を読んで、DEM 一式を work_dir に symlink"""
    tree = ET.parse(topsapp_xml)
    root = tree.getroot()
    dem_path_str = None
    for prop in root.iter("property"):
        if prop.attrib.get("name") == "demFilename":
            dem_path_str = prop.text.strip()
            break
    if not dem_path_str:
        return
    dem_path = Path(dem_path_str)
    if not dem_path.is_absolute():
        dem_path = (work_dir / dem_path).resolve()
    dem_dir = dem_path.parent
    stem = dem_path.name
    for sidecar in dem_dir.glob(f"{stem}*"):
        link = work_dir / sidecar.name
        if link.exists() or link.is_symlink():
            continue
        os.symlink(sidecar, link)


def run_pair(group_dir: Path, pair: str, dry_run: bool,
             skip_existing: bool) -> bool:
    input_xml = group_dir / "input-file" / pair / "topsApp.xml"
    out_dir = group_dir / "isce2" / pair

    if skip_existing and _is_done(out_dir):
        print(f"    ⏭  {pair}: 既存完了 (skip)")
        return True

    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "topsApp.log"

    cmd = ["topsApp.py", str(input_xml)]
    print(f"    ▶  {pair}: cwd={out_dir.relative_to(group_dir.parent)}")
    print(f"        {' '.join(cmd)}")

    if dry_run:
        return True

    _symlink_dem(input_xml, out_dir)

    with open(log_path, "w") as lf:
        result = subprocess.run(
            cmd, cwd=str(out_dir),
            stdout=lf, stderr=subprocess.STDOUT,
        )

    if result.returncode == 0:
        print(f"    ✓  {pair} 完了")
        return True
    print(f"    ✗  {pair} 失敗 (exit={result.returncode}, log: {log_path})")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="ISCE2 topsApp.py を各ペアに対して実行"
    )
    parser.add_argument("--root", type=Path, required=True,
                        help="group_XX/ が並ぶルート")
    parser.add_argument("--groups", type=str, default=None,
                        help="対象 group_id (例: '0,2-3')")
    parser.add_argument("--pairs", type=str, default=None,
                        help="対象ペア (カンマ区切り、例: '20240101_20240113')")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="merged/ があるペアはスキップ (デフォルト ON)")
    parser.add_argument("--no-skip-existing", dest="skip_existing",
                        action="store_false")
    parser.add_argument("--stop-on-error", action="store_true",
                        help="失敗したら後続を実行しない")
    parser.add_argument("--dry-run", action="store_true",
                        help="実行コマンドを表示のみ")
    args = parser.parse_args()

    args.root = args.root.resolve()
    if not args.root.exists():
        print(f"❌ root が存在しません: {args.root}")
        return 1

    if shutil.which("topsApp.py") is None and not args.dry_run:
        print("❌ topsApp.py が PATH にありません。`conda activate isce2` を確認")
        return 1

    filter_ids = _parse_group_ids(args.groups) if args.groups else None
    pair_filter = set(p.strip() for p in args.pairs.split(",")) if args.pairs else None

    groups = _find_group_dirs(args.root, filter_ids)
    if not groups:
        print("❌ 対象グループなし")
        return 1

    print("=" * 70)
    print("🛰  ISCE2 topsApp 実行")
    print("=" * 70)
    print(f"root : {args.root}")
    print(f"対象 グループ: {[g.name for g in groups]}")
    print(f"dry-run: {args.dry_run}  skip-existing: {args.skip_existing}")
    print()

    total_ok = 0
    total_fail = 0

    for g in groups:
        pairs = _find_pairs(g)
        if pair_filter:
            pairs = [p for p in pairs if p in pair_filter]
        print("-" * 70)
        print(f"{g.name}  ペア数: {len(pairs)}")
        if not pairs:
            continue

        for p in pairs:
            ok = run_pair(g, p, args.dry_run, args.skip_existing)
            if ok:
                total_ok += 1
            else:
                total_fail += 1
                if args.stop_on_error:
                    print("\n⚠️  stop-on-error: 中断")
                    break
        else:
            continue
        break  # stop-on-error で内側 break から伝搬

    print()
    print("=" * 70)
    print(f"✓ 成功: {total_ok}  ✗ 失敗: {total_fail}")
    print("=" * 70)
    return 0 if total_fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
