from __future__ import annotations

import json
import netrc
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import asf_search as asf


# ============================================================
# Helpers
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_orbit_direction(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    t = x.strip().upper()
    if t in {"ASC", "ASCENDING"}:
        return "ASCENDING"
    if t in {"DESC", "DESCENDING"}:
        return "DESCENDING"
    if t in {"BOTH"}:
        return None
    raise ValueError(f"orbit_direction must be ASC/DESC/BOTH (got: {x})")


def normalize_aoi_bbox(aoi_spec: Any) -> List[float]:
    """
    Convert various AOI bbox formats to [W, S, E, N] (standard format).
    
    Supported input formats:
      - List/tuple: [W, S, E, N]
      - Dict: {"lower_left": [lon, lat], "upper_right": [lon, lat]}
    """
    if isinstance(aoi_spec, (list, tuple)) and len(aoi_spec) == 4:
        return [float(v) for v in aoi_spec]
    
    if isinstance(aoi_spec, dict):
        lower_left = aoi_spec.get("lower_left")
        upper_right = aoi_spec.get("upper_right")
        if lower_left and upper_right and len(lower_left) == 2 and len(upper_right) == 2:
            w, s = float(lower_left[0]), float(lower_left[1])
            e, n = float(upper_right[0]), float(upper_right[1])
            return [w, s, e, n]
        raise ValueError(
            "Dict format requires both 'lower_left' and 'upper_right' keys "
            "with [lon, lat] coordinate pairs"
        )
    
    raise ValueError(
        f"aoi_bbox must be [W, S, E, N] list or dict with "
        f"{{\"lower_left\": [lon, lat], \"upper_right\": [lon, lat]}}, got: {aoi_spec}"
    )


def parse_time(s: str) -> datetime:
    # ASF properties['startTime'] is usually "...Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def netrc_creds(
    host: str = "urs.earthdata.nasa.gov",
    netrc_path: Optional[Path] = None
) -> Tuple[str, str]:
    p = netrc_path or (Path.home() / ".netrc")
    auth = netrc.netrc(str(p)).authenticators(host)
    if not auth:
        raise FileNotFoundError(
            f"Earthdata creds not found in {p} for host '{host}'.\n"
            f"Create ~/.netrc:\n"
            f"  machine {host} login <user> password <pass>\n"
            f"and run: chmod 600 ~/.netrc"
        )
    login, _, password = auth
    if not login or not password:
        raise RuntimeError(f"Invalid .netrc entry for {host} in {p}")
    return login, password


def build_session(netrc_path: Optional[Path] = None) -> asf.ASFSession:
    user, pw = netrc_creds(netrc_path=netrc_path)
    sess = asf.ASFSession()
    sess.auth_with_creds(user, pw)
    return sess


def safe_prop(props: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in props and props[k] is not None:
            return props[k]
    return default


def _geom_bbox(geom: Dict[str, Any]) -> Optional[List[float]]:
    """Return [W,S,E,N] bbox from a GeoJSON geometry dict."""
    if not geom or not isinstance(geom, dict):
        return None
    coords = geom.get("coordinates")
    if coords is None:
        return None

    def walk(x):
        if isinstance(x, (list, tuple)):
            if len(x) == 2 and all(isinstance(v, (int, float)) for v in x):
                yield float(x[0]), float(x[1])
            else:
                for y in x:
                    yield from walk(y)

    xs: List[float] = []
    ys: List[float] = []
    for lon, lat in walk(coords):
        xs.append(lon)
        ys.append(lat)

    if not xs:
        return None
    return [min(xs), min(ys), max(xs), max(ys)]


def _scene_geometry(r: Any) -> Optional[Dict[str, Any]]:
    """Best-effort extraction of GeoJSON geometry from an ASF result object."""
    geom = getattr(r, "geometry", None)
    if isinstance(geom, dict) and "coordinates" in geom:
        return geom

    gj = getattr(r, "geojson", None)
    if callable(gj):
        try:
            feature = gj()
            if isinstance(feature, dict):
                g = feature.get("geometry")
                if isinstance(g, dict) and "coordinates" in g:
                    return g
        except Exception:
            return None

    return None


def bbox_union(bboxes: Iterable[List[float]]) -> Optional[List[float]]:
    b = [bb for bb in bboxes if bb and len(bb) == 4]
    if not b:
        return None
    return [
        min(bb[0] for bb in b),
        min(bb[1] for bb in b),
        max(bb[2] for bb in b),
        max(bb[3] for bb in b),
    ]


def bbox_from_infos(infos: Iterable["SceneInfo"]) -> Optional[List[float]]:
    bboxes: List[List[float]] = []
    for x in infos:
        geom = _scene_geometry(x.obj)
        bb = _geom_bbox(geom) if geom else None
        if bb:
            bboxes.append(bb)
    return bbox_union(bboxes)


# ============================================================
# AOI shrink (Option B)
# ============================================================

def aoi_wkt_from_bbox_with_shrink(bbox: List[float], shrink_m: float) -> str:
    """
    bbox: [W, S, E, N] in lon/lat (EPSG:4326)
    shrink_m: meters to shrink inward (>=0). 0 => no shrink.
    Returns WKT polygon (lon/lat).

    Requires: shapely, pyproj
    """
    from shapely.geometry import Polygon
    from shapely.ops import transform
    import pyproj

    w, s, e, n = bbox
    poly_ll = Polygon([(w, s), (e, s), (e, n), (w, n), (w, s)])

    if not shrink_m or shrink_m <= 0:
        return poly_ll.wkt

    # Local metric projection centered at AOI centroid (Azimuthal Equidistant)
    cx, cy = poly_ll.centroid.x, poly_ll.centroid.y
    aeqd = pyproj.CRS.from_proj4(
        f"+proj=aeqd +lat_0={cy} +lon_0={cx} +datum=WGS84 +units=m +no_defs"
    )
    wgs84 = pyproj.CRS.from_epsg(4326)

    fwd = pyproj.Transformer.from_crs(wgs84, aeqd, always_xy=True).transform
    inv = pyproj.Transformer.from_crs(aeqd, wgs84, always_xy=True).transform

    poly_m = transform(fwd, poly_ll)
    shrunk_m = poly_m.buffer(-float(shrink_m))

    if shrunk_m.is_empty or shrunk_m.area <= 0:
        raise ValueError(f"aoi_shrink_m too large: {shrink_m} m (AOI became empty)")

    shrunk_ll = transform(inv, shrunk_m)

    # In rare cases buffer can produce MultiPolygon; take largest part.
    if shrunk_ll.geom_type == "MultiPolygon":
        shrunk_ll = max(list(shrunk_ll.geoms), key=lambda g: g.area)

    return shrunk_ll.wkt


# ============================================================
# SBAS pairing
# ============================================================

def sbas_pairs_from_times(
    times: List[datetime],
    *,
    max_temporal_days: int,
    k_neighbors: int,
    ensure_chain: bool = True,
) -> List[Tuple[int, int]]:
    """
    times: sorted ascending
    returns: list of (i,j) with i<j; indices refer to the times list
    """
    n = len(times)
    if n <= 1:
        return []

    pairs: Set[Tuple[int, int]] = set()

    # k-nearest forward and backward (symmetric)
    for i in range(n):
        for d in range(1, k_neighbors + 1):
            j = i + d
            if j >= n:
                break
            dt = (times[j] - times[i]).days
            if dt <= max_temporal_days:
                pairs.add((i, j))

        for d in range(1, k_neighbors + 1):
            j = i - d
            if j < 0:
                break
            dt = (times[i] - times[j]).days
            if dt <= max_temporal_days:
                pairs.add((j, i))

    # chain guarantee (i,i+1) if within limit
    if ensure_chain:
        for i in range(n - 1):
            dt = (times[i + 1] - times[i]).days
            if dt <= max_temporal_days:
                pairs.add((i, i + 1))

    return sorted(pairs)


# ============================================================
# Grouping + thinning
# ============================================================

@dataclass(frozen=True)
class SceneInfo:
    """
    Holds ASF search result plus derived grouping key and timestamp.
    """
    idx: int
    obj: Any
    t: datetime
    frame: Any  # frame/slice identifier if available (varies by ASF metadata)
    key: Tuple[Any, Any, Any, Any]  # (relativeOrbit, flightDirection, processingLevel, beamMode)


def extract_scene_info(results: Iterable[Any]) -> List[SceneInfo]:
    items = list(results)

    infos: List[SceneInfo] = []
    for i, r in enumerate(items):
        props = r.properties

        # Key names can vary, so try multiple candidates.
        rel_orb = safe_prop(props, ["relativeOrbit", "relativeOrbitNumber", "pathNumber"])
        fdir    = safe_prop(props, ["flightDirection", "orbitDirection"])
        plevel  = safe_prop(props, ["processingLevel", "productType"])
        bmode   = safe_prop(props, ["beamMode", "operationalMode"])

        # Frame/slice identifiers vary by product/metadata version.
        # If present, this lets us enforce "same frame only" selection.
        frame = safe_prop(
            props,
            [
                "frameNumber",
                "frame",
                "frameId",
                "frameID",
                "sliceNumber",
                "slice",
                "swath",
            ],
        )

        t = parse_time(props["startTime"])
        key = (rel_orb, fdir, plevel, bmode)

        infos.append(SceneInfo(idx=i, obj=r, t=t, frame=frame, key=key))

    infos.sort(key=lambda x: x.t)
    return infos


def group_by_track(infos: List[SceneInfo]) -> Dict[Tuple[Any, Any, Any, Any], List[SceneInfo]]:
    g: Dict[Tuple[Any, Any, Any, Any], List[SceneInfo]] = {}
    for info in infos:
        g.setdefault(info.key, []).append(info)
    for k in g:
        g[k].sort(key=lambda x: x.t)
    return g


def _aoi_overlap_area(infos: List[SceneInfo], aoi_bbox: List[float]) -> float:
    """Compute the intersection area between the union of scene footprints and the AOI.

    Returns the area of (scenes_union ∩ AOI) in the AOI's local metric CRS.
    Requires shapely and pyproj.
    """
    from shapely.geometry import Polygon, shape
    from shapely.ops import transform, unary_union
    import pyproj

    w, s, e, n = aoi_bbox
    aoi_poly_ll = Polygon([(w, s), (e, s), (e, n), (w, n), (w, s)])

    # Collect scene footprints
    scene_polys = []
    for info in infos:
        geom = _scene_geometry(info.obj)
        if geom:
            try:
                poly = shape(geom)
                if poly.is_valid and not poly.is_empty:
                    scene_polys.append(poly)
            except Exception:
                continue

    if not scene_polys:
        return 0.0

    # Project to local metric CRS for accurate area comparison
    cx, cy = aoi_poly_ll.centroid.x, aoi_poly_ll.centroid.y
    aeqd = pyproj.CRS.from_proj4(
        f"+proj=aeqd +lat_0={cy} +lon_0={cx} +datum=WGS84 +units=m +no_defs"
    )
    wgs84 = pyproj.CRS.from_epsg(4326)
    fwd = pyproj.Transformer.from_crs(wgs84, aeqd, always_xy=True).transform

    aoi_m = transform(fwd, aoi_poly_ll)
    scenes_union_ll = unary_union(scene_polys)
    scenes_union_m = transform(fwd, scenes_union_ll)

    intersection = aoi_m.intersection(scenes_union_m)
    return intersection.area if not intersection.is_empty else 0.0


def choose_group(
    groups: Dict[Tuple[Any, Any, Any, Any], List[SceneInfo]],
    mode: str = "max_aoi_overlap",
    aoi_bbox: Optional[List[float]] = None,
) -> Tuple[Tuple[Any, Any, Any, Any], List[SceneInfo]]:
    if not groups:
        raise ValueError("No groups found (empty results).")

    if mode == "max_aoi_overlap" and aoi_bbox is not None:
        # Pick the group whose scenes collectively cover the most of the AOI
        k = max(groups.keys(), key=lambda kk: _aoi_overlap_area(groups[kk], aoi_bbox))
        return k, groups[k]

    if mode == "largest":
        k = max(groups.keys(), key=lambda kk: len(groups[kk]))
        return k, groups[k]

    # fallback: largest
    k = max(groups.keys(), key=lambda kk: len(groups[kk]))
    return k, groups[k]


def choose_dominant_frame(infos: List[SceneInfo]) -> Tuple[Any, List[SceneInfo]]:
    """Pick the most common frame value within a time-sorted list.

    Returns (frame_value, filtered_infos). If no frame info exists, returns
    (None, original_infos).
    """
    if not infos:
        return None, infos

    counts: Dict[Any, int] = {}
    for x in infos:
        if x.frame is None:
            continue
        counts[x.frame] = counts.get(x.frame, 0) + 1

    if not counts:
        return None, infos

    dominant = max(counts.keys(), key=lambda k: counts[k])
    filtered = [x for x in infos if x.frame == dominant]
    filtered.sort(key=lambda z: z.t)
    return dominant, filtered


def frame_counts(infos: List[SceneInfo]) -> Dict[str, int]:
    counts: Dict[Any, int] = {}
    for x in infos:
        if x.frame is None:
            continue
        counts[x.frame] = counts.get(x.frame, 0) + 1
    # json-friendly keys
    return {str(k): int(v) for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))}


def thin_acquisitions(
    infos: List[SceneInfo],
    *,
    min_repeat_days: Optional[int] = None,
    max_acquisitions: Optional[int] = None,
    keep_ends: bool = True,
) -> List[SceneInfo]:
    """
    infos: time-sorted list
    - min_repeat_days: keep only acquisitions separated by >= this days (greedy)
    - max_acquisitions: cap list size by uniform sampling
    - keep_ends: keep first/last if possible
    """
    if len(infos) <= 2:
        return infos

    out = infos

    # (A) greedy thinning by minimum temporal spacing
    if min_repeat_days is not None and min_repeat_days > 0:
        thinned: List[SceneInfo] = []
        last_t: Optional[datetime] = None

        for x in out:
            if last_t is None:
                thinned.append(x)
                last_t = x.t
                continue
            if (x.t - last_t).days >= min_repeat_days:
                thinned.append(x)
                last_t = x.t

        if keep_ends and thinned:
            if thinned[-1].t != out[-1].t:
                thinned.append(out[-1])

            # dedup by (time, key)
            uniq: Dict[Tuple[datetime, Tuple[Any, Any, Any, Any]], SceneInfo] = {}
            for x in thinned:
                uniq[(x.t, x.key)] = x
            thinned = sorted(uniq.values(), key=lambda z: z.t)

        out = thinned

    # (B) cap by max_acquisitions (uniform sampling)
    if max_acquisitions is not None and max_acquisitions > 0 and len(out) > max_acquisitions:
        if max_acquisitions == 1:
            return [out[0]]
        if max_acquisitions == 2:
            return [out[0], out[-1]] if keep_ends else [out[0], out[1]]

        n = len(out)
        k = max_acquisitions
        idxs = [round(i * (n - 1) / (k - 1)) for i in range(k)]
        idxs = sorted(set(int(i) for i in idxs))
        out = [out[i] for i in idxs]
        out.sort(key=lambda z: z.t)

    return out


# ============================================================
# ASF search + download
# ============================================================

def search_s1_slc(cfg: Dict[str, Any]) -> asf.ASFSearchResults:
    bbox = cfg["aoi_bbox"]
    start = cfg["date_start"]
    end = cfg["date_end"]
    orbit = normalize_orbit_direction(cfg.get("orbit_direction"))

    dlcfg = cfg.get("s1_download", {}) or {}
    max_results = int(dlcfg.get("max_results", 5000))
    platform = dlcfg.get("platform")
    beam_mode = dlcfg.get("beam_mode", "IW")
    processing_level = dlcfg.get("processing_level", "SLC")
    shrink_m = float(dlcfg.get("aoi_shrink_m", 0) or 0)

    aoi_wkt = aoi_wkt_from_bbox_with_shrink(bbox, shrink_m)

    kwargs: Dict[str, Any] = dict(
        dataset=asf.DATASET.SENTINEL1,
        intersectsWith=aoi_wkt,
        start=f"{start}T00:00:00Z",
        end=f"{end}T23:59:59Z",
        beamMode=beam_mode,
        processingLevel=processing_level,
        maxResults=max_results,
    )
    if orbit:
        kwargs["flightDirection"] = orbit
    if platform:
        kwargs["platform"] = platform

    return asf.geo_search(**kwargs)


def sbas_select_and_download(
    cfg: Dict[str, Any],
    project_dir: Path,
    *,
    netrc_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Step02:
      1) search all candidate acquisitions
      2) group by (relativeOrbit, flightDirection, processingLevel, beamMode)
      3) choose one group (default: largest)
      4) thin acquisitions (optional)
      5) build SBAS pairs (time baseline + k-neighbors)
      6) select only acquisitions used in pairs
      7) download only those acquisitions
      8) write .state/sbas_pairs.json
    """
    results = search_s1_slc(cfg)
    results_list = list(results)

    if len(results_list) == 0:
        return {
            "searched": True,
            "downloaded": False,
            "total_candidates": 0,
            "chosen_group_size": 0,
            "selected_count": 0,
        }

    infos = extract_scene_info(results_list)
    groups = group_by_track(infos)

    sbas_cfg: Dict[str, Any] = cfg.get("sbas", {}) or {}
    group_mode = str(sbas_cfg.get("choose_group", "max_aoi_overlap"))
    chosen_key, chosen_infos = choose_group(groups, mode=group_mode, aoi_bbox=cfg["aoi_bbox"])

    chosen_infos_before_group_filters = chosen_infos
    chosen_group_frame_counts_before = frame_counts(chosen_infos_before_group_filters)

    # Enforce that we only use one frame/slice within the chosen track group.
    # Default is ON because SBAS stacks generally must not mix frames.
    # To disable (not recommended), set: sbas.enforce_same_frame: false
    enforce_same_frame = bool(sbas_cfg.get("enforce_same_frame", True))
    chosen_frame = None
    if enforce_same_frame:
        chosen_frame, chosen_infos = choose_dominant_frame(chosen_infos)
        if chosen_frame is None:
            raise ValueError(
                "Frame metadata not found in ASF results, but sbas.enforce_same_frame is enabled (default). "
                "Either adjust search filters (AOI/time) or explicitly disable with sbas.enforce_same_frame: false."
            )
    elif len(chosen_group_frame_counts_before) > 1:
        # Avoid silent surprises when user explicitly opted out.
        print(
            "WARNING: multiple frames detected in chosen group and sbas.enforce_same_frame is disabled; "
            f"frame_counts={chosen_group_frame_counts_before}"
        )

    # --- thinning acquisitions ---
    thin_cfg = sbas_cfg.get("thin_acquisitions", {}) or {}
    min_repeat_days = thin_cfg.get("min_repeat_days")
    max_acquisitions = thin_cfg.get("max_acquisitions")
    keep_ends = bool(thin_cfg.get("keep_ends", True))

    chosen_infos_before = chosen_infos
    chosen_infos = thin_acquisitions(
        chosen_infos,
        min_repeat_days=int(min_repeat_days) if min_repeat_days is not None else None,
        max_acquisitions=int(max_acquisitions) if max_acquisitions is not None else None,
        keep_ends=keep_ends,
    )

    # --- SBAS pairing ---
    times = [x.t for x in chosen_infos]
    pairs = sbas_pairs_from_times(
        times,
        max_temporal_days=int(sbas_cfg.get("max_temporal_days", 48)),
        k_neighbors=int(sbas_cfg.get("k_neighbors", 2)),
        ensure_chain=bool(sbas_cfg.get("ensure_chain", True)),
    )

    # acquisitions used by the pair set
    used: Set[int] = set()
    for i, j in pairs:
        used.add(i)
        used.add(j)

    chosen_used_infos = [chosen_infos[i] for i in sorted(used)]
    selected_objs = [x.obj for x in chosen_used_infos]

    chosen_group_bbox = bbox_from_infos(chosen_infos_before)
    selected_bbox = bbox_from_infos(chosen_used_infos)

    # --- write state file ---
    state_dir = project_dir / ".state"
    state_dir.mkdir(parents=True, exist_ok=True)
    pairs_path = state_dir / "sbas_pairs.json"

    def scene_id(r: Any) -> str:
        p = r.properties
        return (
            p.get("sceneName")
            or p.get("fileID")
            or p.get("granuleName")
            or p.get("productName")
            or str(r)
        )

    meta = {
        "generated_at": utc_now_iso(),
        "aoi_bbox": cfg.get("aoi_bbox"),
        "date_start": cfg.get("date_start"),
        "date_end": cfg.get("date_end"),
        "orbit_direction": cfg.get("orbit_direction"),
        "s1_download": cfg.get("s1_download", {}),
        "total_candidates": len(results_list),
        "groups": {str(k): len(v) for k, v in groups.items()},
        "chosen_group_key": str(chosen_key),
        "enforce_same_frame": enforce_same_frame,
        "chosen_frame": chosen_frame,
        "chosen_group_frame_counts": chosen_group_frame_counts_before,
        "chosen_group_bbox": chosen_group_bbox,
        "selected_bbox": selected_bbox,
        "chosen_group_size_before": len(chosen_infos_before),
        "chosen_group_size_after": len(chosen_infos),
        "selected_count": len(selected_objs),
        "selected_ids": [scene_id(o) for o in selected_objs],
        "pairs": pairs,  # indices in (chosen_infos after thinning)
        "pairs_count": len(pairs),
        "sbas_params": sbas_cfg,
        "thin_params": {
            "min_repeat_days": min_repeat_days,
            "max_acquisitions": max_acquisitions,
            "keep_ends": keep_ends,
        },
    }
    pairs_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    # --- download ---
    dlcfg = cfg.get("s1_download", {}) or {}
    out_dir = project_dir / dlcfg.get("out_dir", "data/s1_slc")
    out_dir.mkdir(parents=True, exist_ok=True)

    if bool(dlcfg.get("dry_search_only", False)):
        return {
            "searched": True,
            "downloaded": False,
            "total_candidates": len(results_list),
            "chosen_group_key": str(chosen_key),
            "chosen_group_size_before": len(chosen_infos_before),
            "chosen_group_size_after": len(chosen_infos),
            "selected_count": len(selected_objs),
            "pairs_count": len(pairs),
            "out_dir": str(out_dir),
            "pairs_file": str(pairs_path),
        }

    session = build_session(netrc_path=netrc_path)

    show_progress = bool(dlcfg.get("show_progress", True))
    skip_existing = bool(dlcfg.get("skip_existing", True))

    if show_progress:
        total = len(selected_objs)
        for i, obj in enumerate(selected_objs, 1):
            props = getattr(obj, "properties", {}) or {}
            sname = props.get("sceneName") or props.get("fileID") or props.get("productName") or str(obj)
            exists = any(out_dir.glob(f"{sname}*"))

            if skip_existing and exists:
                print(f"[{i}/{total}] skip existing: {sname}")
                continue

            print(f"[{i}/{total}] downloading: {sname}")
            # Per-product download gives us deterministic progress output.
            obj.download(path=str(out_dir), session=session)
        print(f"Download complete. out_dir={out_dir}")
    else:
        # Bulk download (can use multiple processes, but progress is limited).
        selected_results = asf.ASFSearchResults(selected_objs)
        selected_results.download(
            path=str(out_dir),
            session=session,
            processes=int(dlcfg.get("processes", 8)),
        )

    return {
        "searched": True,
        "downloaded": True,
        "total_candidates": len(results_list),
        "chosen_group_key": str(chosen_key),
        "chosen_group_size_before": len(chosen_infos_before),
        "chosen_group_size_after": len(chosen_infos),
        "selected_count": len(selected_objs),
        "pairs_count": len(pairs),
        "out_dir": str(out_dir),
        "pairs_file": str(pairs_path),
    }

def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import sys

    try:
        import yaml  # type: ignore
    except Exception as e:
        print(f"Failed to import PyYAML (yaml). Install pyyaml. Detail: {e}")
        return 2

    default_config = Path(__file__).parent.parent / "config.yaml"

    p = argparse.ArgumentParser(description="Search/select/download Sentinel-1 SLC scenes for SBAS")
    p.add_argument(
        "--config",
        type=Path,
        default=default_config,
        help=f"Path to config.yaml (default: {default_config})",
    )
    p.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Override project_dir (default: config file parent directory)",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce console output",
    )

    args = p.parse_args(argv)

    config_path: Path = args.config
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return 1

    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    
    # Normalize AOI bbox format
    cfg["aoi_bbox"] = normalize_aoi_bbox(cfg["aoi_bbox"])
    
    project_dir: Path = args.project_dir or Path(cfg.get("project_dir") or config_path.parent).expanduser().resolve()

    if not args.quiet:
        print(f"Loading config from: {config_path}")
        try:
            print("\nConfiguration:")
            print(f"  AOI: {cfg['aoi_bbox']}")
            print(f"  Date range: {cfg['date_start']} to {cfg['date_end']}")
            print(f"  Orbit: {cfg.get('orbit_direction')}")
            print(f"  Dry run: {cfg.get('s1_download', {}).get('dry_search_only', False)}")
            print(f"  AOI shrink: {cfg.get('s1_download', {}).get('aoi_shrink_m', 0)} m")
        except Exception:
            pass

        print(f"\nProject directory: {project_dir}")
        print("\nStarting SBAS search and selection...")
        print("-" * 60)

    try:
        result = sbas_select_and_download(cfg, project_dir)

        if not args.quiet:
            print("\n" + "=" * 60)
            print("RESULTS:")
            print("=" * 60)
            for key, value in result.items():
                print(f"  {key}: {value}")

            if result.get("pairs_file"):
                print(f"\n✓ Pairs file created: {result['pairs_file']}")

        return 0
    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback

        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import sys

    sys.exit(main())