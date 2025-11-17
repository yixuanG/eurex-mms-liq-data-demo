#!/usr/bin/env python3
"""
Step 1: Propose a 10-minute window starting at the beginning of continuous trading
for a given MarketSegmentID by scanning DI minute activity and (optionally) ISC/PSC
state change timestamps. This script does NOT slice files yet; it only detects and
prints a proposed window, and writes a small JSON manifest for confirmation.

Example (Colab):
  python scripts/make_samples.py \
    --seg 48 \
    --src "/content/Sample_Eurex_20201201_10MktSegID" \
    --out "{REPO_DIR}/data_samples" \
    --propose-only

After you confirm the proposed window, we will implement the actual slicing step
in a follow-up script revision.
"""

import argparse
import datetime as dt
import json
import glob
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# --- Helpers -----------------------------------------------------------------

TS_REGEX = re.compile(r"(?P<ns>\d{16,20})")  # capture 16-20 digit integers (ns epoch)


def _iter_ns_timestamps_from_file(path: str) -> Iterable[int]:
    """Yield candidate nanosecond timestamps from a text CSV line-by-line using regex.
    We pick all 16-20 digit integers and treat them as candidates. This is a heuristic
    suitable for window detection; later parsers will use structured decoding.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            for m in TS_REGEX.finditer(line):
                try:
                    ns = int(m.group("ns"))
                    # Basic sanity: Unix epoch ns for 2000-01-01..2035-01-01
                    if 946684800_000_000_000 <= ns <= 2051222400_000_000_000:
                        yield ns
                except Exception:
                    continue


def _ns_to_minute_utc(ns: int) -> dt.datetime:
    return dt.datetime.utcfromtimestamp(ns / 1_000_000_000).replace(second=0, microsecond=0)


def _build_minute_counts(di_path: str, limit: Optional[int] = None) -> Dict[dt.datetime, int]:
    counts: Dict[dt.datetime, int] = {}
    for i, ns in enumerate(_iter_ns_timestamps_from_file(di_path)):
        minute = _ns_to_minute_utc(ns)
        counts[minute] = counts.get(minute, 0) + 1
        if limit is not None and i + 1 >= limit:
            break
    return counts


def _first_sustained_minute(counts: Dict[dt.datetime, int], sustain: int = 5, threshold: Optional[int] = None) -> Optional[dt.datetime]:
    """Pick the first minute with non-zero (or >= threshold) activity, sustained for N minutes.
    If threshold is None, use > 0 as activity.
    """
    if not counts:
        return None
    mins = sorted(counts.keys())
    for i in range(len(mins)):
        ok = True
        for k in range(sustain):
            if i + k >= len(mins):
                ok = False
                break
            c = counts.get(mins[i + k], 0)
            if threshold is None:
                cond = c > 0
            else:
                cond = c >= threshold
            if not cond:
                ok = False
                break
        if ok:
            return mins[i]
    return None


def _earliest_state_ts(state_path: str) -> Optional[int]:
    if not os.path.exists(state_path):
        return None
    earliest: Optional[int] = None
    for ns in _iter_ns_timestamps_from_file(state_path):
        if earliest is None or ns < earliest:
            earliest = ns
    return earliest


def _format_iso_ns(ns: int) -> str:
    sec = ns // 1_000_000_000
    rem_ns = ns % 1_000_000_000
    return dt.datetime.utcfromtimestamp(sec).strftime("%Y-%m-%dT%H:%M:%S") + f".{rem_ns:09d}Z"


def propose_window(seg: int, src_root: str, sustain_minutes: int = 5, window_minutes: int = 10) -> Dict[str, object]:
    seg_dir = os.path.join(src_root, str(seg)) if os.path.isdir(os.path.join(src_root, str(seg))) else src_root
    # Expected file names inside seg_dir or root (some dumps place files at root)
    candidates = [
        os.path.join(seg_dir, f"DI_{seg}_20201201.csv"),
        os.path.join(src_root, f"DI_{seg}_20201201.csv"),
    ]
    di_path = next((p for p in candidates if os.path.exists(p)), None)
    if not di_path:
        di_path = _glob_search_file(src_root, seg, "DI")
    if not di_path:
        raise FileNotFoundError(f"DI file not found for seg={seg} under {src_root}")

    counts = _build_minute_counts(di_path)
    if not counts:
        raise RuntimeError("No timestamps detected in DI; cannot propose window.")

    # Optional state files
    isc_candidates = [
        os.path.join(seg_dir, f"ISC_{seg}_20201201.csv"),
        os.path.join(src_root, f"ISC_{seg}_20201201.csv"),
    ]
    psc_candidates = [
        os.path.join(seg_dir, f"PSC_{seg}_20201201.csv"),
        os.path.join(src_root, f"PSC_{seg}_20201201.csv"),
    ]
    isc_path = next((p for p in isc_candidates if os.path.exists(p)), None)
    psc_path = next((p for p in psc_candidates if os.path.exists(p)), None)
    if not isc_path:
        isc_path = _glob_search_file(src_root, seg, "ISC")
    if not psc_path:
        psc_path = _glob_search_file(src_root, seg, "PSC")
    isc_earliest_ns = _earliest_state_ts(isc_path) if isc_path else None
    psc_earliest_ns = _earliest_state_ts(psc_path) if psc_path else None

    # Pick first sustained minute of activity as open candidate
    open_minute = _first_sustained_minute(counts, sustain=sustain_minutes, threshold=None)
    if open_minute is None:
        # fallback: earliest minute with any activity
        open_minute = min(counts.keys())
    open_ns = int(open_minute.timestamp() * 1_000_000_000)
    end_ns = open_ns + window_minutes * 60 * 1_000_000_000

    # Prepare a small snapshot of counts around the open minute  
    mins_sorted = sorted(counts.keys())
    around = []
    for m in mins_sorted:
        if abs((m - open_minute).total_seconds()) <= 10 * 60:  # +-10 minutes
            around.append({"minute": m.isoformat() + "Z", "count": counts[m]})

    return {
        "segment": seg,
        "di_path": di_path,
        "isc_path": isc_path,
        "psc_path": psc_path,
        "isc_earliest_ns": isc_earliest_ns,
        "psc_earliest_ns": psc_earliest_ns,
        "open_minute": open_minute.isoformat() + "Z",
        "open_ns": open_ns,
        "open_iso": _format_iso_ns(open_ns),
        "end_ns": end_ns,
        "end_iso": _format_iso_ns(end_ns),
        "sustain_minutes": sustain_minutes,
        "window_minutes": window_minutes,
        "minute_counts_around_open": around,
    }


# --- Step 2: slicing helpers -------------------------------------------------

def _candidate_file_paths(seg: int, src_root: str, prefix: str) -> List[str]:
    seg_dir = os.path.join(src_root, str(seg))
    paths = []
    if os.path.isdir(seg_dir):
        paths.append(os.path.join(seg_dir, f"{prefix}_{seg}_20201201.csv"))
    paths.append(os.path.join(src_root, f"{prefix}_{seg}_20201201.csv"))
    return paths


def _first_existing(paths: List[str]) -> Optional[str]:
    for p in paths:
        if os.path.exists(p):
            return p
    return None


def _glob_search_file(src_root: str, seg: int, prefix: str) -> Optional[str]:
    """As a robust fallback, search recursively under src_root for files like
    {prefix}_{seg}_*.csv (allowing different date stamps or folder layouts).
    """
    patterns = [
        os.path.join(src_root, "**", f"{prefix}_{seg}_20201201.csv"),  # expected date
        os.path.join(src_root, "**", f"{prefix}_{seg}_*.csv"),          # any date suffix
    ]
    for pat in patterns:
        matches = glob.glob(pat, recursive=True)
        if matches:
            # Prefer shortest path (closest match)
            matches.sort(key=lambda x: len(x))
            return matches[0]
    return None


def _line_in_ns_range(line: str, open_ns: int, end_ns: int) -> bool:
    any_ts = False
    for m in TS_REGEX.finditer(line):
        try:
            ns = int(m.group("ns"))
        except Exception:
            continue
        # sanity bounds as in detection
        if not (946684800_000_000_000 <= ns <= 2051222400_000_000_000):
            continue
        any_ts = True
        if open_ns <= ns < end_ns:
            return True
    # If no timestamp was found in the line, do not include it here
    return False


def slice_file_by_ns(in_path: str, out_path: str, open_ns: int, end_ns: int) -> Tuple[int, int]:
    """Write lines from in_path to out_path if any timestamp in line is within [open_ns, end_ns).
    Returns (written_lines, scanned_lines).
    """
    scanned = 0
    written = 0
    with open(in_path, "r", encoding="utf-8", errors="ignore") as src, open(out_path, "w", encoding="utf-8", errors="ignore") as dst:
        for line in src:
            scanned += 1
            if _line_in_ns_range(line, open_ns, end_ns):
                dst.write(line)
                written += 1
    return written, scanned


def slice_segment_files(seg: int, src_root: str, out_dir: str, open_ns: int, end_ns: int) -> Dict[str, Dict[str, int]]:
    """Slice DI/DS/ISC/PSC into [open_ns, end_ns); copy IS fully (usually small).
    Returns a summary dict mapping file keys to counts.
    """
    os.makedirs(out_dir, exist_ok=True)
    summary: Dict[str, Dict[str, int]] = {}

    # DI
    di_path = _first_existing(_candidate_file_paths(seg, src_root, "DI")) or _glob_search_file(src_root, seg, "DI")
    if di_path:
        out_path = os.path.join(out_dir, f"DI_{seg}_20201201_window.csv")
        w, s = slice_file_by_ns(di_path, out_path, open_ns, end_ns)
        summary["DI"] = {"written": w, "scanned": s}

    # DS
    ds_path = _first_existing(_candidate_file_paths(seg, src_root, "DS")) or _glob_search_file(src_root, seg, "DS")
    if ds_path:
        out_path = os.path.join(out_dir, f"DS_{seg}_20201201_window.csv")
        w, s = slice_file_by_ns(ds_path, out_path, open_ns, end_ns)
        summary["DS"] = {"written": w, "scanned": s}

    # ISC
    isc_path = _first_existing(_candidate_file_paths(seg, src_root, "ISC")) or _glob_search_file(src_root, seg, "ISC")
    if isc_path:
        out_path = os.path.join(out_dir, f"ISC_{seg}_20201201_window.csv")
        w, s = slice_file_by_ns(isc_path, out_path, open_ns, end_ns)
        summary["ISC"] = {"written": w, "scanned": s}

    # PSC
    psc_path = _first_existing(_candidate_file_paths(seg, src_root, "PSC")) or _glob_search_file(src_root, seg, "PSC")
    if psc_path:
        out_path = os.path.join(out_dir, f"PSC_{seg}_20201201_window.csv")
        w, s = slice_file_by_ns(psc_path, out_path, open_ns, end_ns)
        summary["PSC"] = {"written": w, "scanned": s}

    # IS (copy fully; static and small)
    is_path = _first_existing(_candidate_file_paths(seg, src_root, "IS")) or _glob_search_file(src_root, seg, "IS")
    if is_path:
        out_path = os.path.join(out_dir, f"IS_{seg}_20201201_full.csv")
        with open(is_path, "r", encoding="utf-8", errors="ignore") as src, open(out_path, "w", encoding="utf-8", errors="ignore") as dst:
            count = 0
            for line in src:
                dst.write(line)
                count += 1
        summary["IS"] = {"written": count, "scanned": count}

    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Propose an opening 10-minute window for a segment (no slicing yet)")
    ap.add_argument("--seg", type=int, required=True, help="MarketSegmentID (e.g., 48)")
    ap.add_argument("--src", required=True, help="Extracted dataset root (e.g., /content/Sample_...)")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "data_samples"), help="Output directory for proposal JSON")
    ap.add_argument("--sustain", type=int, default=5, help="Sustained minutes of activity to qualify as open (default 5)")
    ap.add_argument("--window-minutes", type=int, default=10, help="Proposed window length in minutes (default 10)")
    ap.add_argument("--propose-only", action="store_true", help="Only propose window (default behavior)")
    args = ap.parse_args()

    # Create raw subdirectory for organized output
    raw_out_dir = os.path.join(args.out, "raw")
    os.makedirs(raw_out_dir, exist_ok=True)
    proposal = propose_window(args.seg, args.src, sustain_minutes=args.sustain, window_minutes=args.window_minutes)

    # Print a concise summary
    print("== Proposed window ==")
    print(json.dumps({k: v for k, v in proposal.items() if k in ("segment", "open_iso", "end_iso", "open_ns", "end_ns")}, indent=2))
    if proposal.get("isc_earliest_ns"):
        print("ISC earliest:", proposal["isc_earliest_ns"], _format_iso_ns(proposal["isc_earliest_ns"]))
    if proposal.get("psc_earliest_ns"):
        print("PSC earliest:", proposal["psc_earliest_ns"], _format_iso_ns(proposal["psc_earliest_ns"]))

    # Write JSON manifest for confirmation / downstream use
    out_path = os.path.join(raw_out_dir, f"proposed_window_seg{args.seg}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(proposal, f, indent=2)
    print(f"Saved proposal: {out_path}")

    if not args.propose_only:
        print("[INFO] Proceeding to slice files using the proposed window [open_ns, end_ns)...")
        summary = slice_segment_files(args.seg, args.src, raw_out_dir, proposal["open_ns"], proposal["end_ns"])
        print("== Slicing summary ==")
        for k, v in summary.items():
            print(f"  {k}: written={v['written']} scanned={v['scanned']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())