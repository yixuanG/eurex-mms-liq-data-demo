#!/usr/bin/env python3
"""
Process a single segment for the full trading day with maximum depth.

This script orchestrates the full pipeline for one segment:
1. Detect maximum depth available in the DI data
2. Slice full-day data (no 10-minute window limitation)
3. Parse and build L{max_depth} order book snapshots
4. Aggregate to 1-second metrics
5. Save all intermediate products to Drive

Usage (Colab):
  python scripts/process_full_segment.py \
    --seg 48 \
    --src-local "/content/Sample_Eurex_20201201_10MktSegID" \
    --out-drive "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_segments" \
    --date 20201201 \
    --max-levels 20

Output structure (in Drive):
  data_segments/seg_{seg}/
    ├── di_mapping.json
    ├── metadata.json
    ├── l{depth}/
    │   ├── snapshots.parquet
    │   ├── agg_1s.parquet
    │   └── agg_5s.parquet
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Color output helpers
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def log_step(msg: str):
    print(f"\n{Colors.OKBLUE}{'='*60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{msg}{Colors.ENDC}")
    print(f"{Colors.OKBLUE}{'='*60}{Colors.ENDC}\n")


def log_ok(msg: str):
    print(f"{Colors.OKGREEN}✅ {msg}{Colors.ENDC}")


def log_warn(msg: str):
    print(f"{Colors.WARNING}⚠️  {msg}{Colors.ENDC}")


def log_error(msg: str):
    print(f"{Colors.FAIL}❌ {msg}{Colors.ENDC}")


def run_cmd(cmd: list, desc: str) -> int:
    """Run a command and return exit code."""
    print(f"{Colors.OKCYAN}▶ {desc}{Colors.ENDC}")
    print(f"  Command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        log_error(f"{desc} failed with exit code {result.returncode}")
    return result.returncode


def main() -> int:
    ap = argparse.ArgumentParser(description="Process full segment with max depth for full trading day")
    ap.add_argument("--seg", type=int, required=True, help="Market Segment ID")
    ap.add_argument("--src-local", required=True, help="Path to raw data in Colab local SSD")
    ap.add_argument("--out-drive", required=True, help="Output directory in Google Drive")
    ap.add_argument("--date", required=True, help="Date string (e.g., 20201201)")
    ap.add_argument("--max-levels", type=int, default=20, help="Maximum depth levels to track (default: 20)")
    ap.add_argument("--skip-depth-check", action="store_true", help="Skip max depth detection step")
    args = ap.parse_args()

    start_time = time.time()
    
    # Setup paths
    script_dir = Path(__file__).parent
    src_dir = script_dir.parent / "src"
    sys.path.append(str(src_dir))
    
    seg_out = Path(args.out_drive) / f"seg_{args.seg}"
    seg_out.mkdir(parents=True, exist_ok=True)
    
    metadata = {
        "segment_id": args.seg,
        "date": args.date,
        "max_levels": args.max_levels,
        "processing_start": time.strftime("%Y-%m-%d %H:%M:%S"),
        "src_local": args.src_local,
        "out_drive": str(seg_out),
    }
    
    log_step(f"Processing Segment {args.seg} - Full Day {args.date}")
    
    # Step 1: Slice full-day data (no window restriction)
    log_step("Step 1: Slice full-day DI/DS data")
    di_slice = seg_out / f"DI_{args.seg}_{args.date}_fullday.csv"
    ds_slice = seg_out / f"DS_{args.seg}_{args.date}_fullday.csv"
    
    # For full-day: just copy/filter the files by segment
    slice_cmd = [
        "python", str(script_dir / "slice_full_day.py"),
        "--seg", str(args.seg),
        "--src", args.src_local,
        "--out", str(seg_out),
        "--date", args.date,
    ]
    
    if run_cmd(slice_cmd, "Slice full-day data") != 0:
        return 1
    
    if not di_slice.exists():
        log_error(f"DI slice not found: {di_slice}")
        return 1
    
    log_ok(f"DI slice ready: {di_slice} ({di_slice.stat().st_size / 1024 / 1024:.2f} MB)")
    
    # Step 2: Inspect schema and infer mapping
    log_step("Step 2: Infer DI schema mapping")
    mapping_json = seg_out / "di_mapping.json"
    
    inspect_cmd = [
        "python", str(script_dir / "inspect_schema.py"),
        "--di", str(di_slice),
        "--out", str(mapping_json),
        "--sample-limit", "500",
    ]
    
    if run_cmd(inspect_cmd, "Infer schema mapping") != 0:
        return 1
    
    log_ok(f"Mapping saved: {mapping_json}")
    
    # Step 3: Detect maximum depth (optional)
    actual_max_depth = args.max_levels
    
    if not args.skip_depth_check:
        log_step("Step 3: Detect maximum depth in data")
        depth_cmd = [
            "python", str(script_dir / "check_max_depth.py"),
            "--di", str(di_slice),
            "--mapping", str(mapping_json),
            "--sample-limit", "10000",
        ]
        
        # Run and capture to parse max depth (non-critical if fails)
        result = subprocess.run(depth_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            # Try to parse max depth from output
            for line in result.stdout.split('\n'):
                if "Maximum price level found:" in line:
                    try:
                        actual_max_depth = int(line.split(':')[-1].strip())
                        log_ok(f"Detected max depth: L{actual_max_depth}")
                    except:
                        pass
        else:
            log_warn("Depth detection failed, using default max_levels")
    else:
        log_warn(f"Skipping depth check, using --max-levels={args.max_levels}")
    
    metadata["actual_max_depth"] = actual_max_depth
    
    # Step 4: Parse and build L{depth} snapshots
    log_step(f"Step 4: Build L{actual_max_depth} order book snapshots")
    
    parse_cmd = [
        "python", str(script_dir / "parse_and_l5.py"),
        "--seg", str(args.seg),
        "--di", str(di_slice),
        "--mapping", str(mapping_json),
        "--out", str(seg_out),
        "--levels", str(actual_max_depth),
    ]
    
    if run_cmd(parse_cmd, f"Parse DI to L{actual_max_depth} snapshots") != 0:
        return 1
    
    snapshot_parq = seg_out / f"l{actual_max_depth}" / f"l{actual_max_depth}_snapshots_seg{args.seg}.parquet"
    if not snapshot_parq.exists():
        log_error(f"Snapshots not found: {snapshot_parq}")
        return 1
    
    log_ok(f"Snapshots ready: {snapshot_parq} ({snapshot_parq.stat().st_size / 1024 / 1024:.2f} MB)")
    
    # Step 5: Aggregate to 1-second metrics
    log_step("Step 5: Aggregate to 1-second metrics")
    
    agg_cmd = [
        "python", str(script_dir / "aggregate_l5.py"),
        "--seg", str(args.seg),
        "--l5", str(snapshot_parq),
        "--di", str(di_slice),
        "--mapping", str(mapping_json),
        "--out", str(seg_out),
    ]
    
    if run_cmd(agg_cmd, "Aggregate to 1s metrics") != 0:
        log_warn("Aggregation failed, but snapshots are available")
    else:
        agg_parq = seg_out / f"l{actual_max_depth}" / f"l{actual_max_depth}_agg_1s_seg{args.seg}.parquet"
        log_ok(f"Aggregates ready: {agg_parq}")
    
    # Save metadata
    metadata["processing_end"] = time.strftime("%Y-%m-%d %H:%M:%S")
    metadata["processing_duration_sec"] = int(time.time() - start_time)
    
    metadata_path = seg_out / "metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    log_ok(f"Metadata saved: {metadata_path}")
    
    # Summary
    log_step(f"✅ Segment {args.seg} processing complete!")
    print(f"Duration: {metadata['processing_duration_sec']} seconds")
    print(f"Output: {seg_out}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
