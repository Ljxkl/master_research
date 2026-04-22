"""
Sentinel-1シーン範囲ベースのAOIグループ化ツール

複数のAOIに対して、Sentinel-1 SLC シーンの取得範囲に基づいて
自動的にグループ化します。

シーンの重複度が100%のAOI同士を同じグループに割当てます。
"""

import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Set, Optional, Iterable
from datetime import datetime
import asf_search as asf
from shapely.geometry import shape, box


_PLATFORM_ALIASES = {
    'A': 'Sentinel-1A', 'a': 'Sentinel-1A', 'S1A': 'Sentinel-1A',
    'B': 'Sentinel-1B', 'b': 'Sentinel-1B', 'S1B': 'Sentinel-1B',
    'C': 'Sentinel-1C', 'c': 'Sentinel-1C', 'S1C': 'Sentinel-1C',
}
_ALL_PLATFORMS = ['Sentinel-1A', 'Sentinel-1B', 'Sentinel-1C']


def _normalize_platform(platform) -> List[str]:
    """'A' / 'all' / ['A','C'] 等を asf_search の platform 値リストに変換"""
    if platform is None:
        return _ALL_PLATFORMS
    if isinstance(platform, str):
        if platform.lower() == 'all':
            return _ALL_PLATFORMS
        return [_PLATFORM_ALIASES.get(platform, platform)]
    if isinstance(platform, Iterable):
        resolved = []
        for p in platform:
            if isinstance(p, str) and p.lower() == 'all':
                return _ALL_PLATFORMS
            resolved.append(_PLATFORM_ALIASES.get(p, p))
        return resolved
    raise ValueError(f"platform の形式が不正: {platform!r}")


def _extract_scene_date(scene_id: str) -> str:
    """S1 シーン名から取得日 (YYYYMMDD) を抽出。失敗時は空文字。"""
    for part in scene_id.split('_'):
        if len(part) >= 15 and part[8:9] == 'T' and part[:8].isdigit():
            return part[:8]
    return ''


def _thin_scenes(scene_ids: Iterable[str], max_count: Optional[int]) -> List[str]:
    """日付順で先頭から max_count 枚を取る (S1A の 12日周期を活用)"""
    ids = sorted(scene_ids, key=lambda s: (_extract_scene_date(s), s))
    if max_count is None:
        return ids
    if max_count <= 0:
        return []
    return ids[:max_count]


class S1SceneBasedGrouper:
    """Sentinel-1シーン範囲に基づいてAOIをグループ化"""
    
    def __init__(self, 
                 scene_overlap_threshold: float = 1.0,
                 min_common_scenes: int = 1):
        """
        scene_overlap_threshold: AOIペア間で何%シーンが重なれば同じグループか (0.0~1.0)
                                 デフォルト: 1.0 (100% 完全一致)
        min_common_scenes: 最小共通シーン数
        """
        self.overlap_threshold = scene_overlap_threshold
        self.min_common_scenes = min_common_scenes
        self.groups = []
    
    def search_scenes_for_aoi(self, bbox, date_start, date_end,
                               orbit_direction='ASCENDING',
                               platforms: Optional[List[str]] = None):
        """
        AOIを **完全に含む** Sentinel-1 SLC シーンのみ返す。

        Args:
            bbox: [W, S, E, N] 形式のバウンディングボックス
            date_start: 検索開始日 (YYYY-MM-DD)
            date_end: 検索終了日 (YYYY-MM-DD)
            orbit_direction: 'ASCENDING' or 'DESCENDING'
            platforms: 'Sentinel-1A' 等のリスト。None なら全機

        戻り値: シーンID のセット (AOI を包含するもののみ)
        """
        try:
            wkt = self._bbox_to_wkt(bbox)
            aoi_poly = box(bbox[0], bbox[1], bbox[2], bbox[3])

            kwargs = dict(
                dataset=asf.DATASET.SENTINEL1,
                intersectsWith=wkt,
                start=f"{date_start}T00:00:00Z",
                end=f"{date_end}T23:59:59Z",
                beamMode="IW",
                processingLevel="SLC",
                flightDirection=orbit_direction,
                maxResults=5000,
            )
            if platforms:
                kwargs['platform'] = platforms
            results = asf.geo_search(**kwargs)

            scene_ids = set()
            dropped_partial = 0
            for r in results:
                scene_poly = shape(r.geometry)
                if not scene_poly.contains(aoi_poly):
                    dropped_partial += 1
                    continue
                props = r.properties
                scene_id = (
                    props.get('sceneName') or
                    props.get('fileID') or
                    props.get('productName')
                )
                if scene_id:
                    scene_ids.add(scene_id)

            self._last_dropped_partial = dropped_partial
            return scene_ids

        except Exception as e:
            print(f"⚠️  Error searching scenes: {e}")
            return set()
    
    def _bbox_to_wkt(self, bbox):
        """BBox [W, S, E, N] → WKT Polygon"""
        w, s, e, n = bbox
        return f"POLYGON(({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))"
    
    def grouping(self, aoi_df, date_start, date_end, orbit_direction='ASCENDING',
                 platform=None, max_scenes_per_group: Optional[int] = None):
        """
        全AOIをシーン範囲ベースでグループ化

        重複度100%のAOI同士を同じグループに割当てます。

        Args:
            aoi_df: AOI の DataFrame (カラム: plant_name, LL_lon, LL_lat, UR_lon, UR_lat)
            date_start: 検索開始日
            date_end: 検索終了日
            orbit_direction: 'ASCENDING' or 'DESCENDING'
            platform: 'A'/'B'/'C'/'all' またはリスト。None なら全機
            max_scenes_per_group: グループごとの最大シーン数 (None なら無制限)

        戻り値: グループのリスト
        """
        platforms = _normalize_platform(platform)
        print("="*70)
        print("📡 Sentinel-1 AOI グループ化")
        print("="*70)
        
        print("\n📡 ステップ1: 各AOIでSentinel-1 SLCシーンを検索中...")
        print("-"*70)
        
        # ステップ1: 各AOIのシーンを検索
        aoi_scenes: Dict[int, Set[str]] = {}
        aoi_metadata = {}
        
        for idx, row in aoi_df.iterrows():
            bbox = [row['LL_lon'], row['LL_lat'], row['UR_lon'], row['UR_lat']]
            plant_name = row['plant_name']
            
            print(f"  [{idx+1:2d}/{len(aoi_df)}] AOI {idx} ({plant_name})", end=' ... ')
            
            scenes = self.search_scenes_for_aoi(
                bbox, date_start, date_end, orbit_direction, platforms
            )
            dropped = getattr(self, '_last_dropped_partial', 0)

            aoi_scenes[idx] = scenes
            aoi_metadata[idx] = {
                'plant_name': plant_name,
                'bbox': bbox,
                'scene_count': len(scenes)
            }

            extra = f" (部分一致 {dropped} 枚除外)" if dropped else ""
            warn = "  ⚠️ 候補0" if len(scenes) == 0 else ""
            print(f"✓ {len(scenes)} シーン{extra}{warn}")
        
        print(f"\n✓ シーン検索完了\n")
        
        # ステップ2: 候補シーン集合でハッシュグルーピング
        print("📊 ステップ2: 候補シーン集合でハッシュグルーピング...")
        print("-"*70)

        from collections import defaultdict
        buckets: Dict[frozenset, List[int]] = defaultdict(list)
        for i in range(len(aoi_df)):
            buckets[frozenset(aoi_scenes[i])].append(i)

        empty_aois = buckets.pop(frozenset(), [])
        if empty_aois:
            print(f"  ⚠️  候補シーン 0 の AOI が {len(empty_aois)} 個: グループ化対象外")
            for i in empty_aois:
                print(f"     - AOI {i} ({aoi_metadata[i]['plant_name']})")

        print(f"  有効グループ数: {len(buckets)}\n")

        # グループオブジェクトを構築 (候補数が多い順に安定 ID 付与)
        sorted_groups = sorted(
            buckets.items(),
            key=lambda kv: (-len(kv[0]), kv[1][0])  # scene数 desc、tie は最小 AOI index
        )
        for group_id, (scene_set, aoi_indices) in enumerate(sorted_groups):
            aoi_indices = sorted(aoi_indices)

            # bucket 内の AOI は同一の候補集合を持つ
            group_scenes = set(scene_set)
            full_count = len(group_scenes)
            scene_ids = _thin_scenes(group_scenes, max_scenes_per_group)
            thinned = max_scenes_per_group is not None and full_count > len(scene_ids)

            # グループを包含するBBox を計算
            group_bbox = self._compute_group_bbox(aoi_df, aoi_indices)

            plant_names = [aoi_metadata[i]['plant_name'] for i in aoi_indices]

            group_obj = {
                'group_id': group_id,
                'aoi_indices': aoi_indices,
                'plant_names': plant_names,
                'bbox': group_bbox,
                'scene_ids': scene_ids,
                'scene_count': len(scene_ids),
                'scene_count_full': full_count,
                'thinned': thinned,
            }
            
            self.groups.append(group_obj)
            
            print(f"\n グループ {group_id}:")
            print(f"   AOI インデックス: {aoi_indices}")
            print(f"   発電所: {', '.join(plant_names)}")
            print(f"   BBox: W={group_bbox[0]:.6f}, S={group_bbox[1]:.6f}, "
                  f"E={group_bbox[2]:.6f}, N={group_bbox[3]:.6f}")
            if thinned:
                print(f"   シーン数: {len(scene_ids)} (全 {full_count} から間引き)")
            else:
                print(f"   シーン数: {len(scene_ids)}")
        
        print("\n" + "="*70)
        print(f"✓ グループ化完了: {len(self.groups)} グループ")
        print("="*70 + "\n")
        
        return self.groups
    
    def _compute_group_bbox(self, aoi_df, aoi_indices):
        """グループ内のすべてのAOIを包含するBBox を計算"""
        lls_lon = [aoi_df.iloc[i]['LL_lon'] for i in aoi_indices]
        lls_lat = [aoi_df.iloc[i]['LL_lat'] for i in aoi_indices]
        urs_lon = [aoi_df.iloc[i]['UR_lon'] for i in aoi_indices]
        urs_lat = [aoi_df.iloc[i]['UR_lat'] for i in aoi_indices]
        
        return [
            min(lls_lon),
            min(lls_lat),
            max(urs_lon),
            max(urs_lat)
        ]
    
    def get_groups(self) -> List[Dict]:
        """グループを取得"""
        return self.groups
    
    def save_grouping_result(self, output_path: Path, aoi_df, date_start: str, 
                             date_end: str, orbit_direction: str = 'ASCENDING'):
        """
        グループ化結果（メタデータ含む）をJSONファイルに保存
        
        Args:
            output_path: 出力JSONファイルのパス
            aoi_df: AOI の DataFrame
            date_start: 検索開始日
            date_end: 検索終了日
            orbit_direction: オービット方向
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # グループごとの詳細情報を構築
        group_details = []
        for group in self.groups:
            detail = {
                'group_id': group['group_id'],
                'aoi_count': len(group['aoi_indices']),
                'aoi_indices': group['aoi_indices'],
                'aoi_names': group['plant_names'],
                'bbox': {
                    'lower_left': [group['bbox'][0], group['bbox'][1]],
                    'upper_right': [group['bbox'][2], group['bbox'][3]]
                },
                'scene_count': group['scene_count'],
                'scene_ids': group['scene_ids']
            }
            group_details.append(detail)
        
        result = {
            'generated_at': datetime.now().isoformat(),
            'parameters': {
                'date_start': date_start,
                'date_end': date_end,
                'orbit_direction': orbit_direction,
                'overlap_threshold': self.overlap_threshold
            },
            'summary': {
                'total_aois': len(aoi_df),
                'total_groups': len(self.groups),
                'total_scenes': sum(g['scene_count'] for g in self.groups)
            },
            'groups': group_details
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        return output_path
    
    def print_dry_run_summary(self):
        """Dry-run結果のサマリーを表示"""
        print("\n" + "="*70)
        print("📋 ドライラン結果サマリー")
        print("="*70 + "\n")
        
        total_aois = sum(len(g['aoi_indices']) for g in self.groups)
        total_scenes = sum(g['scene_count'] for g in self.groups)
        
        print(f"総AOI数: {total_aois}")
        print(f"グループ数: {len(self.groups)}")
        print(f"総シーン数: {total_scenes}\n")
        
        print("-"*70)
        print("グループ別詳細:")
        print("-"*70)
        
        for group in self.groups:
            print(f"\n【グループ {group['group_id']}】")
            print(f"  含まれるAOI: {len(group['aoi_indices'])} 個")
            for aoi_idx, plant_name in zip(group['aoi_indices'], group['plant_names']):
                print(f"    • [{aoi_idx:2d}] {plant_name}")
            print(f"  BBox: W={group['bbox'][0]:.6f}, S={group['bbox'][1]:.6f}, "
                  f"E={group['bbox'][2]:.6f}, N={group['bbox'][3]:.6f}")
            print(f"  シーン数: {group['scene_count']}")
            print(f"  取得予定シーン:")
            for scene_id in group['scene_ids'][:5]:  # 最初の5つだけ表示
                print(f"    - {scene_id}")
            if len(group['scene_ids']) > 5:
                print(f"    ... 他 {len(group['scene_ids']) - 5} シーン")
        
        print("\n" + "="*70)
        print(f"✓ これらの {len(self.groups)} グループを処理します")
        print("="*70 + "\n")
    
    def save_group_configs(self, output_dir: Path, date_start: str, date_end: str, 
                           orbit_direction: str = 'ASCENDING'):
        """各グループに対応するYAML設定ファイルを生成"""
        try:
            import yaml
        except ImportError:
            print("⚠️  PyYAML is required to save configs. Install with: pip install pyyaml")
            return
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n📝 グループ設定ファイルを生成中...\n")
        
        for group in self.groups:
            yaml_config = {
                'aoi_bbox': {
                    'lower_left': [group['bbox'][0], group['bbox'][1]],
                    'upper_right': [group['bbox'][2], group['bbox'][3]]
                },
                'date_start': date_start,
                'date_end': date_end,
                'orbit_direction': orbit_direction,
                'aoi_indices': group['aoi_indices'],
                'aoi_names': group['plant_names'],
                'comment': f"Auto-generated group of {len(group['plant_names'])} AOI(s)"
            }
            
            config_filename = f"aoi_group_{group['group_id']}.yaml"
            config_path = output_dir / config_filename
            
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_config, f, default_flow_style=False, allow_unicode=True)
            
            print(f" ✓ {config_filename}")
        
        print(f"\n✓ 設定ファイル生成完了 ({output_dir})\n")


def _load_config(config_path: Path) -> dict:
    import yaml
    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    required = ['aoi_csv', 'date_start', 'date_end', 'orbit_direction']
    missing = [k for k in required if cfg.get(k) is None]
    if missing:
        raise ValueError(f"config に必須キーがありません: {missing}")

    cfg.setdefault('overlap_threshold', 1.0)
    cfg.setdefault('min_common_scenes', 1)
    cfg.setdefault('max_scenes_per_group', None)
    cfg.setdefault('platform', None)
    cfg.setdefault('dry_run', True)
    cfg.setdefault('result_json', None)
    cfg.setdefault('output_dir', None)

    if cfg['orbit_direction'] not in ('ASCENDING', 'DESCENDING'):
        raise ValueError(
            f"orbit_direction は ASCENDING/DESCENDING: {cfg['orbit_direction']}"
        )

    cfg['aoi_csv'] = Path(cfg['aoi_csv'])
    if cfg['result_json'] is not None:
        cfg['result_json'] = Path(cfg['result_json'])
    if cfg['output_dir'] is not None:
        cfg['output_dir'] = Path(cfg['output_dir'])

    return cfg


def main():
    """メイン処理"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Sentinel-1シーン範囲ベースでAOIをグループ化"
    )
    parser.add_argument(
        'config',
        type=Path,
        help='YAML 設定ファイルのパス (例: conf/grouper.yaml)'
    )
    args = parser.parse_args()

    if not args.config.exists():
        print(f"❌ 設定ファイルが見つかりません: {args.config}")
        return 1

    cfg = _load_config(args.config)
    print(f"📂 設定を読み込み: {args.config}\n")

    if not cfg['aoi_csv'].exists():
        print(f"❌ AOI CSV ファイルが見つかりません: {cfg['aoi_csv']}")
        return 1

    print(f"📂 AOI CSV を読み込み中: {cfg['aoi_csv']}\n")
    aoi_df = pd.read_csv(cfg['aoi_csv'])
    print(f"✓ {len(aoi_df)} 個のAOIを読み込みました\n")

    grouper = S1SceneBasedGrouper(
        scene_overlap_threshold=cfg['overlap_threshold'],
        min_common_scenes=cfg['min_common_scenes']
    )

    grouper.grouping(
        aoi_df,
        date_start=cfg['date_start'],
        date_end=cfg['date_end'],
        orbit_direction=cfg['orbit_direction'],
        platform=cfg['platform'],
        max_scenes_per_group=cfg['max_scenes_per_group'],
    )

    if cfg['dry_run']:
        grouper.print_dry_run_summary()

        result_json = cfg['result_json']
        if result_json is None:
            state_dir = cfg['aoi_csv'].parent / '.state'
            state_dir.mkdir(parents=True, exist_ok=True)
            result_json = state_dir / 'aoi_grouping_result.json'

        result_path = grouper.save_grouping_result(
            result_json,
            aoi_df,
            cfg['date_start'],
            cfg['date_end'],
            cfg['orbit_direction']
        )
        print(f"📄 グループ化結果を保存しました:")
        print(f"   {result_path}\n")
        return 0

    if cfg['output_dir']:
        grouper.save_group_configs(
            cfg['output_dir'],
            cfg['date_start'],
            cfg['date_end'],
            cfg['orbit_direction']
        )

    return 0


if __name__ == '__main__':
    exit(main())
