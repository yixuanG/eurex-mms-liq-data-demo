#!/usr/bin/env python3
"""
Batch process all segments from the dataset.

This is the master orchestration script that processes multiple segments
in sequence or parallel, producing full-day L{max} order book data for each.

Features:
- Auto-detect segments from source directory
- Auto-detect optimal depth per segment from segment_depth_summary.json
- Parallel processing support
- Per-segment depth configuration

Usage (Colab):

  # Recommended: Auto-detect segments and use optimal depth per segment
  python scripts/process_all_segments.py \
    --auto-detect \
    --auto-depth \
    --src-local "/content/Sample_Eurex_20201201_10MktSegID" \
    --out-drive "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_segments" \
    --date 20201201 \
    --parallel 2

  # Manual: Specify segments and fixed depth
  python scripts/process_all_segments.py \
    --segments 48 589 688 821 1176 1373 1374 \
    --src-local "/content/Sample_Eurex_20201201_10MktSegID" \
    --out-drive "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_segments" \
    --date 20201201 \
    --max-levels 6 \
    --parallel 2

  # Auto-detect segments but use fixed depth
  python scripts/process_all_segments.py \
    --auto-detect \
    --max-levels 10 \
    --src-local "/content/Sample_Eurex_20201201_10MktSegID" \
    --out-drive "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_segments" \
    --date 20201201
"""
import argparse
import glob
import json
import os
import re
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional


def detect_segments(src_dir: str) -> List[int]:
    """Auto-detect segment IDs from source directory structure."""
    segments = set()
    
    # Search for DI files which should exist for all segments
    di_files = glob.glob(f"{src_dir}/**/DI*.csv", recursive=True) + \
               glob.glob(f"{src_dir}/**/DI*.csv.gz", recursive=True)
    
    # Try to extract segment IDs from filenames or directory names
    pattern = re.compile(r'(?:seg|segment|mktseg)?[_\-]?(\d{1,5})', re.IGNORECASE)
    
    for path in di_files:
        # Try filename first
        matches = pattern.findall(os.path.basename(path))
        for m in matches:
            seg = int(m)
            if 1 <= seg <= 99999:  # Reasonable segment ID range
                segments.add(seg)
        
        # Try parent directory names
        parts = Path(path).parts
        for part in parts:
            matches = pattern.findall(part)
            for m in matches:
                seg = int(m)
                if 1 <= seg <= 99999:
                    segments.add(seg)
    
    return sorted(segments)


def is_segment_complete(seg: int, out_drive: str) -> bool:
    """Check if a segment has already been processed successfully.
    
    A segment is considered complete if it has:
    1. A metadata.json file
    2. An aggregated 1s parquet file in an l{X} subdirectory
    """
    seg_dir = Path(out_drive) / f"seg_{seg}"
    if not seg_dir.exists():
        return False
    
    # Check for metadata
    metadata_path = seg_dir / "metadata.json"
    if not metadata_path.exists():
        return False
    
    # Check for any l{X} directory with aggregated parquet
    lx_dirs = list(seg_dir.glob("l*"))
    if not lx_dirs:
        return False
    
    # Check if any l{X} directory has the aggregated 1s parquet file
    for lx_dir in lx_dirs:
        agg_files = list(lx_dir.glob("*_agg_1s_*.parquet"))
        if agg_files:
            return True
    
    return False


def load_depth_summary(out_drive: str) -> dict:
    """Load segment depth summary from JSON file."""
    summary_path = Path(out_drive).parent / "data_raw" / "segment_depth_summary.json"
    
    if not summary_path.exists():
        return {}
    
    try:
        with open(summary_path, 'r') as f:
            data = json.load(f)
            # Build dict: segment -> recommended_L
            depth_map = {}
            for item in data.get('results', []):
                seg = item.get('Segment')
                rec_l = item.get('Suggested_L', 6)
                if seg is not None:
                    depth_map[seg] = rec_l
            return depth_map
    except Exception as e:
        print(f"[WARN] Could not load depth summary: {e}")
        return {}


def process_one_segment(
    seg: int,
    src_local: str,
    out_drive: str,
    date: str,
    max_levels: int,
    script_dir: Path
) -> dict:
    """Process a single segment. Returns result dict."""
    start_time = time.time()
    
    cmd = [
        "python",
        str(script_dir / "process_full_segment.py"),
        "--seg", str(seg),
        "--src-local", src_local,
        "--out-drive", out_drive,
        "--date", date,
        "--max-levels", str(max_levels),
    ]
    
    print(f"\n{'='*60}")
    print(f"Starting segment {seg}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, capture_output=False)
    
    duration = time.time() - start_time
    
    return {
        "segment": seg,
        "success": result.returncode == 0,
        "duration_sec": int(duration),
        "exit_code": result.returncode,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Batch process all segments")
    ap.add_argument("--segments", type=int, nargs="+", help="List of segment IDs to process")
    ap.add_argument("--auto-detect", action="store_true", help="Auto-detect segments from source")
    ap.add_argument("--src-local", required=True, help="Raw data in Colab local SSD")
    ap.add_argument("--out-drive", required=True, help="Output directory in Google Drive")
    ap.add_argument("--date", required=True, help="Date string (e.g., 20201201)")
    ap.add_argument("--max-levels", type=int, default=None, help="Max depth levels (default: auto from depth summary)")
    ap.add_argument("--auto-depth", action="store_true", help="Auto-detect max levels from segment_depth_summary.json")
    ap.add_argument("--parallel", type=int, default=1, help="Number of segments to process in parallel (default: 1)")
    ap.add_argument("--skip-existing", action="store_true", help="Skip segments that have already been processed successfully")
    args = ap.parse_args()
    
    script_dir = Path(__file__).parent
    
    # Determine segments to process
    if args.auto_detect:
        print("[INFO] Auto-detecting segments...")
        segments = detect_segments(args.src_local)
        if not segments:
            print("[ERROR] No segments detected")
            return 1
        print(f"[INFO] Detected {len(segments)} segments: {segments}")
    elif args.segments:
        segments = args.segments
    else:
        print("[ERROR] Must specify --segments or --auto-detect")
        return 1
    
    # Load depth info if auto-depth is enabled
    depth_map = {}
    if args.auto_depth or args.max_levels is None:
        print("[INFO] Loading depth summary for auto-detection...")
        depth_map = load_depth_summary(args.out_drive)
        if depth_map:
            print(f"[INFO] Loaded depth info for {len(depth_map)} segments")
        else:
            print("[WARN] No depth summary found, using default max_levels=6")
    
    # Filter out already completed segments if requested
    if args.skip_existing:
        print("[INFO] Checking for existing completed segments...")
        original_count = len(segments)
        completed = []
        for seg in segments[:]:
            if is_segment_complete(seg, args.out_drive):
                completed.append(seg)
        
        segments = [seg for seg in segments if seg not in completed]
        
        if completed:
            print(f"[INFO] Skipping {len(completed)} already completed segments: {completed}")
        print(f"[INFO] {len(segments)} segments remaining to process")
        
        if not segments:
            print("[INFO] All segments already completed!")
            return 0
    
    # Prepare segment -> max_levels mapping
    segment_depths = {}
    for seg in segments:
        if depth_map and seg in depth_map:
            segment_depths[seg] = depth_map[seg]
        elif args.max_levels is not None:
            segment_depths[seg] = args.max_levels
        else:
            segment_depths[seg] = 6  # Safe default
    
    print(f"\n{'='*60}")
    print(f"BATCH PROCESSING {len(segments)} SEGMENTS")
    print(f"{'='*60}")
    print(f"Date: {args.date}")
    if args.auto_depth or args.max_levels is None:
        print(f"Max levels: AUTO (per-segment from depth summary)")
        for seg in segments:
            print(f"  Segment {seg:5d}: L{segment_depths[seg]}")
    else:
        print(f"Max levels: {args.max_levels} (fixed for all)")
    print(f"Parallelism: {args.parallel}")
    print(f"Segments: {segments}")
    print(f"{'='*60}\n")
    
    overall_start = time.time()
    results = []
    
    if args.parallel > 1:
        # Parallel processing
        print(f"[INFO] Using {args.parallel} parallel workers")
        with ProcessPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(
                    process_one_segment,
                    seg, args.src_local, args.out_drive, args.date, segment_depths[seg], script_dir
                ): seg
                for seg in segments
            }
            
            for future in as_completed(futures):
                seg = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"[ERROR] Segment {seg} raised exception: {e}")
                    results.append({
                        "segment": seg,
                        "success": False,
                        "error": str(e),
                    })
    else:
        # Sequential processing
        for seg in segments:
            result = process_one_segment(
                seg, args.src_local, args.out_drive, args.date, segment_depths[seg], script_dir
            )
            results.append(result)
    
    # Summary
    overall_duration = time.time() - overall_start
    
    print(f"\n\n{'='*60}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total duration: {int(overall_duration)} seconds")
    print(f"Segments processed: {len(results)}")
    
    success_count = sum(1 for r in results if r.get("success"))
    fail_count = len(results) - success_count
    
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")
    
    print(f"\nDetailed results:")
    for r in sorted(results, key=lambda x: x["segment"]):
        status = "✅" if r.get("success") else "❌"
        seg = r["segment"]
        duration = r.get("duration_sec", 0)
        print(f"  {status} Segment {seg:5d}: {duration:5d}s")
    
    # Save summary JSON
    summary_path = Path(args.out_drive) / "batch_summary.json"
    summary = {
        "date": args.date,
        "segments": segments,
        "max_levels": "auto" if (args.auto_depth or args.max_levels is None) else args.max_levels,
        "segment_depths": segment_depths,
        "parallel": args.parallel,
        "overall_duration_sec": int(overall_duration),
        "success_count": success_count,
        "fail_count": fail_count,
        "results": results,
    }
    
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n📊 Summary saved: {summary_path}")
    
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
