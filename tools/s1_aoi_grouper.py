"""
Sentinel-1シーン範囲ベースのAOIグループ化ツール

複数のAOIに対して、Sentinel-1 SLC シーンの取得範囲に基づいて
自動的にグループ化します。

シーンの重複度が100%のAOI同士を同じグループに割当てます。
"""

import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
from datetime import datetime
import asf_search as asf


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
                               orbit_direction='ASCENDING'):
        """
        AOIに対してSentinel-1 SLCシーンを検索
        
        Args:
            bbox: [W, S, E, N] 形式のバウンディングボックス
            date_start: 検索開始日 (YYYY-MM-DD)
            date_end: 検索終了日 (YYYY-MM-DD)
            orbit_direction: 'ASCENDING' or 'DESCENDING'
        
        戻り値: シーンID のセット
        """
        try:
            wkt = self._bbox_to_wkt(bbox)
            
            results = asf.geo_search(
                dataset=asf.DATASET.SENTINEL1,
                intersectsWith=wkt,
                start=f"{date_start}T00:00:00Z",
                end=f"{date_end}T23:59:59Z",
                beamMode="IW",
                processingLevel="SLC",
                flightDirection=orbit_direction,
                maxResults=5000,
            )
            
            scene_ids = set()
            for r in results:
                props = r.properties
                scene_id = (
                    props.get('sceneName') or 
                    props.get('fileID') or 
                    props.get('productName')
                )
                if scene_id:
                    scene_ids.add(scene_id)
            
            return scene_ids
        
        except Exception as e:
            print(f"⚠️  Error searching scenes: {e}")
            return set()
    
    def _bbox_to_wkt(self, bbox):
        """BBox [W, S, E, N] → WKT Polygon"""
        w, s, e, n = bbox
        return f"POLYGON(({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))"
    
    def grouping(self, aoi_df, date_start, date_end, orbit_direction='ASCENDING'):
        """
        全AOIをシーン範囲ベースでグループ化
        
        重複度100%のAOI同士を同じグループに割当てます。
        
        Args:
            aoi_df: AOI の DataFrame (カラム: plant_name, LL_lon, LL_lat, UR_lon, UR_lat)
            date_start: 検索開始日
            date_end: 検索終了日
            orbit_direction: 'ASCENDING' or 'DESCENDING'
        
        戻り値: グループのリスト
        """
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
                bbox, date_start, date_end, orbit_direction
            )
            
            aoi_scenes[idx] = scenes
            aoi_metadata[idx] = {
                'plant_name': plant_name,
                'bbox': bbox,
                'scene_count': len(scenes)
            }
            
            print(f"✓ {len(scenes)} シーン")
        
        print(f"\n✓ シーン検索完了\n")
        
        # ステップ2: AOI間のシーン重複度を計算
        print("📊 ステップ2: AOI間の重複度を計算中...")
        print("-"*70)
        
        # Union-Find のような動的グルーピング
        parent = list(range(len(aoi_df)))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # すべてのAOIペアを比較
        merge_count = 0
        for i in range(len(aoi_df)):
            for j in range(i + 1, len(aoi_df)):
                scenes_i = aoi_scenes[i]
                scenes_j = aoi_scenes[j]
                
                # 共通シーン数
                common = len(scenes_i & scenes_j)
                
                # 重複度を計算
                if len(scenes_i) == 0 or len(scenes_j) == 0:
                    overlap = 0.0
                else:
                    # シーン数が同じかつ全て共通 = 100%
                    if len(scenes_i) == len(scenes_j) == common:
                        overlap = 1.0
                    else:
                        # 最小値基準の重複度
                        min_scenes = min(len(scenes_i), len(scenes_j))
                        overlap = common / min_scenes if min_scenes > 0 else 0.0
                
                plant_i = aoi_metadata[i]['plant_name']
                plant_j = aoi_metadata[j]['plant_name']
                
                # 100% 重複かつ最小シーン数を満たせば同グループ
                if common >= self.min_common_scenes and overlap >= self.overlap_threshold:
                    status = "✓ 統合"
                    union(i, j)
                    merge_count += 1
                else:
                    status = "✗ 分離"
                
                print(f"  {status}  {plant_i:15s} ↔ {plant_j:15s}  "
                      f"重複度={overlap:5.1%}  共通={common:3d}/{min(len(scenes_i), len(scenes_j)):3d}")
        
        print(f"\n✓ 重複度計算完了 ({merge_count} ペアが統合)\n")
        
        # ステップ3: グループを確定
        print("🔗 ステップ3: グループを確定中...")
        print("-"*70)
        
        groups_dict: Dict[int, List[int]] = {}
        for i in range(len(aoi_df)):
            root = find(i)
            if root not in groups_dict:
                groups_dict[root] = []
            groups_dict[root].append(i)
        
        # グループオブジェクトを構築
        for group_id, (root, aoi_indices) in enumerate(sorted(groups_dict.items())):
            aoi_indices.sort()
            
            # グループ内シーンの和集合
            group_scenes = set()
            for idx in aoi_indices:
                group_scenes.update(aoi_scenes[idx])
            
            # グループを包含するBBox を計算
            group_bbox = self._compute_group_bbox(aoi_df, aoi_indices)
            
            plant_names = [aoi_metadata[i]['plant_name'] for i in aoi_indices]
            
            group_obj = {
                'group_id': group_id,
                'aoi_indices': aoi_indices,
                'plant_names': plant_names,
                'bbox': group_bbox,
                'scene_ids': sorted(list(group_scenes)),
                'scene_count': len(group_scenes),
            }
            
            self.groups.append(group_obj)
            
            print(f"\n グループ {group_id}:")
            print(f"   AOI インデックス: {aoi_indices}")
            print(f"   発電所: {', '.join(plant_names)}")
            print(f"   BBox: W={group_bbox[0]:.6f}, S={group_bbox[1]:.6f}, "
                  f"E={group_bbox[2]:.6f}, N={group_bbox[3]:.6f}")
            print(f"   シーン数: {len(group_scenes)}")
        
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


def main():
    """メイン処理"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Sentinel-1シーン範囲ベースでAOIをグループ化"
    )
    parser.add_argument(
        '--aoi-csv',
        type=Path,
        default=Path(__file__).parent.parent / 'aoi.csv',
        help='AOI CSV ファイルのパス'
    )
    parser.add_argument(
        '--date-start',
        type=str,
        default='2023-01-01',
        help='検索開始日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--date-end',
        type=str,
        default='2025-12-31',
        help='検索終了日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--orbit-direction',
        type=str,
        default='ASCENDING',
        choices=['ASCENDING', 'DESCENDING'],
        help='オービット方向'
    )
    parser.add_argument(
        '--overlap-threshold',
        type=float,
        default=1.0,
        help='重複度閾値 (0.0~1.0, デフォルト: 1.0 = 100%)'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='グループ設定ファイルの出力ディレクトリ'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='ドライラン: グループ化結果を生成して表示し、JSONで保存する（ダウンロードしない）'
    )
    parser.add_argument(
        '--result-json',
        type=Path,
        default=None,
        help='グループ化結果のJSON出力ファイルパス（指定なしで自動生成）'
    )
    
    args = parser.parse_args()
    
    if not args.aoi_csv.exists():
        print(f"❌ AOI CSV ファイルが見つかりません: {args.aoi_csv}")
        return 1
    
    # AOI CSV を読み込み
    print(f"📂 AOI CSV を読み込み中: {args.aoi_csv}\n")

    aoi_df = pd.read_csv(args.aoi_csv)

    print(f"✓ {len(aoi_df)} 個のAOIを読み込みました\n")
    
    # グルーパーを初期化
    grouper = S1SceneBasedGrouper(
        scene_overlap_threshold=args.overlap_threshold,
        min_common_scenes=1
    )
    
    # グループ化を実行
    groups = grouper.grouping(
        aoi_df,
        date_start=args.date_start,
        date_end=args.date_end,
        orbit_direction=args.orbit_direction
    )
    
    # Dry-run または Dry-run + 設定出力
    if args.dry_run:
        # ドライラン結果を表示
        grouper.print_dry_run_summary()
        
        # JSON結果を保存
        if args.result_json is None:
            # デフォルトパスを生成
            state_dir = args.aoi_csv.parent / '.state'
            state_dir.mkdir(parents=True, exist_ok=True)
            args.result_json = state_dir / 'aoi_grouping_result.json'
        
        result_path = grouper.save_grouping_result(
            args.result_json,
            aoi_df,
            args.date_start,
            args.date_end,
            args.orbit_direction
        )
        
        print(f"📄 グループ化結果を保存しました:")
        print(f"   {result_path}\n")
        
        return 0
    
    # オプション: 設定ファイルを出力
    if args.output_dir:
        grouper.save_group_configs(
            args.output_dir,
            args.date_start,
            args.date_end,
            args.orbit_direction
        )
    
    return 0


if __name__ == '__main__':
    exit(main())
