"""
s1_aoi_grouper.py の出力 JSON を読んで Sentinel-1 SLC をダウンロードする。

使い方:
    python s1_download_from_groups.py <grouping_result.json> \
        --out-dir /path/to/data [--processes 4] [--skip-existing] [--dry-run]
"""

import argparse
import json
import netrc
from pathlib import Path
from typing import List, Optional, Tuple

import asf_search as asf


def netrc_creds(host: str = "urs.earthdata.nasa.gov",
                netrc_path: Optional[Path] = None) -> Tuple[str, str]:
    p = netrc_path or (Path.home() / ".netrc")
    auth = netrc.netrc(str(p)).authenticators(host)
    if not auth:
        raise FileNotFoundError(
            f"Earthdata creds not found in {p} for host '{host}'.\n"
            f"~/.netrc に以下を追加:\n"
            f"  machine {host} login <user> password <pass>\n"
            f"そして chmod 600 ~/.netrc"
        )
    login, _, password = auth
    if not login or not password:
        raise RuntimeError(f"Invalid .netrc entry for {host}")
    return login, password


def build_session(netrc_path: Optional[Path] = None) -> asf.ASFSession:
    user, pw = netrc_creds(netrc_path=netrc_path)
    sess = asf.ASFSession()
    sess.auth_with_creds(user, pw)
    return sess


GROUP_SUBDIRS = ["data", "input-file", "isce2", "orbit", "dem"]


def _group_dirname(group: dict) -> str:
    return f"group_{group['group_id']:02d}"


def _ensure_group_layout(group_root: Path) -> Path:
    """グループ直下に data/ input-file/ isce2/ orbit/ を作り、data/ を返す"""
    for sub in GROUP_SUBDIRS:
        (group_root / sub).mkdir(parents=True, exist_ok=True)
    return group_root / "data"


def _already_downloaded(scene_id: str, data_dir: Path) -> bool:
    return (data_dir / f"{scene_id}.zip").exists() or \
           (data_dir / f"{scene_id}.SAFE").exists()


def _parse_group_ids(spec: str) -> set:
    """'0,2,5-7' のような指定を set に展開"""
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


def download_groups(grouping_json: Path,
                    out_dir: Path,
                    processes: int = 1,
                    skip_existing: bool = True,
                    dry_run: bool = False,
                    group_ids: Optional[set] = None) -> int:
    with open(grouping_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_groups = data.get("groups", [])
    if not all_groups:
        print("⚠️  グループが空です")
        return 1

    if group_ids is not None:
        groups = [g for g in all_groups if g["group_id"] in group_ids]
        missing = group_ids - {g["group_id"] for g in all_groups}
        if missing:
            print(f"⚠️  JSON に存在しない group_id: {sorted(missing)}")
        if not groups:
            print("❌ 指定された group_id が1つも一致しません")
            return 1
    else:
        groups = all_groups

    print("=" * 70)
    print(f"📥 Sentinel-1 SLC ダウンロード")
    print("=" * 70)
    print(f"入力 : {grouping_json}")
    print(f"出力 : {out_dir}")
    if group_ids is not None:
        print(f"グループ数 : {len(groups)} / {len(all_groups)} "
              f"(指定: {sorted(g['group_id'] for g in groups)})")
    else:
        print(f"グループ数 : {len(groups)}")
    print(f"並列数 : {processes}  既存スキップ : {skip_existing}  dry-run : {dry_run}")
    print()

    session = None if dry_run else build_session()

    total_downloaded = 0
    total_skipped = 0
    total_missing = 0

    for group in groups:
        gid = group["group_id"]
        group_root = out_dir / _group_dirname(group)
        data_dir = group_root / "data"
        scene_ids: List[str] = group.get("scene_ids", [])

        print("-" * 70)
        print(f"グループ {gid}  ({', '.join(group.get('aoi_names', []))})")
        print(f"  出力: {group_root}  (data/ input-file/ isce2/ orbit/)")
        print(f"  対象シーン: {len(scene_ids)}")

        if not scene_ids:
            print("  (スキップ: シーン 0)")
            continue

        if skip_existing:
            todo = [s for s in scene_ids if not _already_downloaded(s, data_dir)]
            skipped = len(scene_ids) - len(todo)
        else:
            todo = list(scene_ids)
            skipped = 0

        total_skipped += skipped
        if skipped:
            print(f"  既存スキップ: {skipped}")

        if not todo:
            print("  全てダウンロード済み")
            continue

        if dry_run:
            for s in todo:
                print(f"    - {s}")
            total_missing += len(todo)
            continue

        _ensure_group_layout(group_root)

        print(f"  検索中: {len(todo)} granule ...", flush=True)
        results = asf.granule_search(granule_list=todo)
        found = {r.properties.get("sceneName") for r in results}
        missing = [s for s in todo if s not in found]
        if missing:
            print(f"  ⚠️  ASF で未検出 ({len(missing)}): {missing[:3]}...")
            total_missing += len(missing)

        if len(results) == 0:
            continue

        print(f"  ダウンロード中: {len(results)} granule → {data_dir}", flush=True)
        results.download(path=str(data_dir), session=session, processes=processes)
        total_downloaded += len(results)

    print()
    print("=" * 70)
    print(f"✓ 完了  ダウンロード: {total_downloaded}  既存: {total_skipped}  未検出: {total_missing}")
    print("=" * 70)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="s1_aoi_grouper.py の結果 JSON から Sentinel-1 SLC をダウンロード"
    )
    parser.add_argument(
        "grouping_json",
        type=Path,
        help="s1_aoi_grouper.py の出力 JSON (例: .state/aoi_grouping_result.json)"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="ダウンロード先ルートディレクトリ (グループごとにサブディレクトリ作成)"
    )
    parser.add_argument("--processes", type=int, default=4,
                        help="並列ダウンロード数 (デフォルト 4)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="既存ファイルがあればスキップ (デフォルト ON)")
    parser.add_argument("--no-skip-existing", dest="skip_existing",
                        action="store_false",
                        help="既存チェックを無効化")
    parser.add_argument("--dry-run", action="store_true",
                        help="ダウンロード対象を表示のみ、実際の取得はしない")
    parser.add_argument("--groups", type=str, default=None,
                        help="対象 group_id をカンマ/範囲で指定 (例: '0,2,5-7')")
    args = parser.parse_args()

    group_ids = _parse_group_ids(args.groups) if args.groups else None

    if not args.grouping_json.exists():
        print(f"❌ JSON が見つかりません: {args.grouping_json}")
        return 1

    return download_groups(
        grouping_json=args.grouping_json,
        out_dir=args.out_dir,
        processes=args.processes,
        skip_existing=args.skip_existing,
        dry_run=args.dry_run,
        group_ids=group_ids,
    )


if __name__ == "__main__":
    exit(main())
