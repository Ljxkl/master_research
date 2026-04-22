#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path
from typing import Optional, Tuple, List

import requests

# ASF "s1qc" public mirrors
ASF_POE = "https://s1qc.asf.alaska.edu/aux_poeorb/"
ASF_RES = "https://s1qc.asf.alaska.edu/aux_resorb/"

DATEFMT_SAFE = "%Y%m%dT%H%M%S"

# Example:
# S1A_OPER_AUX_POEORB_OPOD_20200128T120847_V20191230T225942_20200101T005942.EOF
EOF_RE = re.compile(
    r"^(S1[AB])_OPER_AUX_(POEORB|RESORB)_OPOD_\d{8}T\d{6}_V"
    r"(?P<start>\d{8}T\d{6})_(?P<end>\d{8}T\d{6})\.EOF$"
)

def parse_safe_timestamp_and_sat(path: str) -> Tuple[dt.datetime, str, Optional[dt.datetime]]:
    """
    Returns:
      fileTS:    end time (from SAFE name)
      satName:   S1A or S1B
      fileTSStart: start time (from SAFE name) if available
    """
    safename = os.path.basename(path)
    fields = safename.split("_")

    fileTSStart = None
    try:
        # Typical SLC name has ... _<start>_<stop>_<orbit>_...
        # In your original code: fields[-5] = start, fields[-4] = stop
        fileTS = dt.datetime.strptime(fields[-4], DATEFMT_SAFE)
        fileTSStart = dt.datetime.strptime(fields[-5], DATEFMT_SAFE)
    except Exception:
        # Fallback: just YYYYMMDD somewhere
        m = re.search(r"(?<=_)\d{8}", safename)
        if not m:
            raise ValueError(f"Cannot parse date from SAFE/ZIP name: {safename}")
        fileTS = dt.datetime.strptime(m.group(), "%Y%m%d")

    satName = fields[0] if fields and fields[0].startswith("S1") else "S1A"
    return fileTS, satName, fileTSStart

def list_eof_filenames(base_url: str, session: requests.Session) -> List[str]:
    """
    Fetch the directory listing HTML and extract .EOF filenames.
    """
    r = session.get(base_url, timeout=60)
    r.raise_for_status()
    html = r.text
    # Directory listing usually contains href="...EOF"
    names = sorted(set(re.findall(r'href="([^"]+\.EOF)"', html)))
    # Some listings include full paths; normalize to basename
    return [os.path.basename(n) for n in names]

def select_best_orbit(
    filenames: List[str],
    sat: str,
    orbit_type: str,  # "POEORB" or "RESORB"
    tref_start: Optional[dt.datetime],
    tref_end: dt.datetime,
) -> Optional[str]:
    """
    Choose one EOF file whose validity interval covers the acquisition interval.
    If tref_start is None, we just require it covers tref_end.
    """
    best = None
    best_span = None  # smaller span is often "more precise" fit, but not required

    for fn in filenames:
        m = EOF_RE.match(fn)
        if not m:
            continue
        if m.group(1) != sat:
            continue
        if m.group(2) != orbit_type:
            continue

        vstart = dt.datetime.strptime(m.group("start"), "%Y%m%dT%H%M%S")
        vend = dt.datetime.strptime(m.group("end"), "%Y%m%dT%H%M%S")

        if tref_start is not None:
            ok = (vstart <= tref_start) and (vend >= tref_end)
        else:
            ok = (vstart <= tref_end) and (vend >= tref_end)

        if not ok:
            continue

        span = (vend - vstart).total_seconds()
        if best is None or span < (best_span or span + 1):
            best = fn
            best_span = span

    return best

def download(url: str, outpath: Path, session: requests.Session) -> None:
    outpath.parent.mkdir(parents=True, exist_ok=True)
    tmp = outpath.with_suffix(outpath.suffix + ".part")

    with session.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    # very small file is likely an error HTML, but ASF should not do that
    if tmp.stat().st_size < 10_000:
        # Keep the file for debugging, but warn
        raise RuntimeError(f"Downloaded file too small ({tmp.stat().st_size} bytes): {tmp}")

    tmp.replace(outpath)

def main():
    ap = argparse.ArgumentParser(description="Fetch Sentinel-1 orbit EOF from ASF Alaska (s1qc) for a given SAFE/ZIP")
    ap.add_argument("-i", "--input", required=True, help="Path to SAFE package (.SAFE) or SLC ZIP")
    ap.add_argument("-o", "--output", dest="outdir", default=".", help="Output directory")
    ap.add_argument("--prefer", choices=["precise", "restituted"], default="precise",
                    help="Prefer POEORB (precise) or RESORB (restituted) first")
    args = ap.parse_args()

    fileTS, satName, fileTSStart = parse_safe_timestamp_and_sat(args.input)
    print("Reference time: ", fileTS)
    print("Satellite name: ", satName)

    # Prefer POEORB, fall back to RESORB (or reverse if user asked)
    order = [("precise", "POEORB", ASF_POE), ("restituted", "RESORB", ASF_RES)]
    if args.prefer == "restituted":
        order = list(reversed(order))

    session = requests.Session()

    match_file = None
    match_url = None

    for label, orbit_type, base_url in order:
        try:
            filenames = list_eof_filenames(base_url, session)
        except Exception as e:
            print(f"Failed to list {label} orbits from {base_url}: {e}", file=sys.stderr)
            continue

        chosen = select_best_orbit(
            filenames=filenames,
            sat=satName,
            orbit_type=orbit_type,
            tref_start=fileTSStart,
            tref_end=fileTS,
        )

        if chosen:
            match_file = chosen
            match_url = base_url + chosen
            break

    if not match_file or not match_url:
        print(f"Failed to find orbit for tref {fileTS} ({satName}).", file=sys.stderr)
        sys.exit(2)

    outdir = Path(args.outdir)
    outpath = outdir / match_file

    print("Downloading URL: ", match_url)
    try:
        download(match_url, outpath, session)
    except Exception as e:
        print(f"Failed to download orbit file: {e}", file=sys.stderr)
        sys.exit(1)

    print("Orbit downloaded successfully:", outpath)

if __name__ == "__main__":
    main()