#!/usr/bin/env python3
"""
Slice full-day data for a specific segment.

Unlike make_samples.py (which slices a 10-minute window), this script
extracts all DI/DS/IS/ISC/PSC data for a given segment for the entire trading day.

Usage (Colab):
  python scripts/slice_full_day.py \
    --seg 48 \
    --src "/content/Sample_Eurex_20201201_10MktSegID" \
    --out "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_segments/seg_48" \
    --date 20201201
"""
import argparse
import glob
import gzip
import os
import shutil
from pathlib import Path
from typing import Optional


def find_file_for_segment(src: str, seg: int, file_type: str, date: str) -> Optional[str]:
    """Find a file (DI, DS, IS, etc.) for a given segment.
    
    Search patterns:
    - {src}/**/{file_type}*{seg}*.csv
    - {src}/**/{file_type}_{date}*.csv (then filter by seg in content)
    """
    # Try direct segment-specific patterns first
    patterns = [
        f"{src}/**/{file_type}*{seg}*.csv",
        f"{src}/**/{file_type}*{seg}*.csv.gz",
        f"{src}/{seg}/{file_type}*.csv",
        f"{src}/{seg}/{file_type}*.csv.gz",
        f"{src}/*{seg}*/{file_type}*.csv",
        f"{src}/*{seg}*/{file_type}*.csv.gz",
    ]
    
    for pattern in patterns:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    
    # Try date-based pattern (will need content filtering)
    generic_patterns = [
        f"{src}/**/{file_type}_{date}*.csv",
        f"{src}/**/{file_type}*{date}*.csv",
        f"{src}/**/{file_type}*.csv",
    ]
    
    for pattern in generic_patterns:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            # Return first match; caller should filter by segment
            return matches[0]
    
    return None


def filter_by_segment(input_path: str, output_path: str, seg: int) -> int:
    """Filter CSV file to keep only lines containing the segment ID.
    
    Returns number of lines written.
    """
    line_count = 0
    seg_str = str(seg)
    
    # Handle gzipped files
    if input_path.endswith('.gz'):
        opener = gzip.open
        mode = 'rt'
    else:
        opener = open
        mode = 'r'
    
    with opener(input_path, mode, encoding='utf-8', errors='ignore') as fin:
        with open(output_path, 'w', encoding='utf-8') as fout:
            for line in fin:
                # Simple heuristic: check if segment ID appears in the line
                # More robust: parse the line properly, but this works for Eurex format
                if seg_str in line:
                    fout.write(line)
                    line_count += 1
    
    return line_count


def copy_full_file(input_path: str, output_path: str) -> int:
    """Copy entire file (for IS which applies to all securities in segment).
    
    Returns number of lines copied.
    """
    line_count = 0
    
    if input_path.endswith('.gz'):
        opener = gzip.open
        mode = 'rt'
    else:
        opener = open
        mode = 'r'
    
    with opener(input_path, mode, encoding='utf-8', errors='ignore') as fin:
        with open(output_path, 'w', encoding='utf-8') as fout:
            for line in fin:
                fout.write(line)
                line_count += 1
    
    return line_count


def main() -> int:
    ap = argparse.ArgumentParser(description="Slice full-day data for a segment")
    ap.add_argument("--seg", type=int, required=True, help="Market Segment ID")
    ap.add_argument("--src", required=True, help="Raw data source directory (Colab local)")
    ap.add_argument("--out", required=True, help="Output directory (Google Drive)")
    ap.add_argument("--date", required=True, help="Date string (e.g., 20201201)")
    args = ap.parse_args()
    
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Slicing full-day data for segment {args.seg}")
    print(f"  Source: {args.src}")
    print(f"  Output: {out_dir}")
    print(f"  Date: {args.date}")
    
    file_types = {
        'DI': 'filter',  # Depth Incremental - needs filtering
        'DS': 'filter',  # Depth Snapshot - needs filtering
        'IS': 'copy',    # Instrument Snapshot - copy entire file
        'ISC': 'filter', # Instrument State Change - needs filtering
        'PSC': 'filter', # Product State Change - needs filtering
    }
    
    summary = {}
    
    for file_type, action in file_types.items():
        print(f"\n[{file_type}] Searching...")
        
        src_file = find_file_for_segment(args.src, args.seg, file_type, args.date)
        
        if not src_file:
            print(f"  ⚠️  No {file_type} file found, skipping")
            summary[file_type] = "not_found"
            continue
        
        print(f"  Found: {src_file}")
        
        out_file = out_dir / f"{file_type}_{args.seg}_{args.date}_fullday.csv"
        
        try:
            if action == 'filter':
                line_count = filter_by_segment(src_file, str(out_file), args.seg)
                print(f"  ✅ Filtered {line_count} lines to {out_file}")
            else:  # copy
                line_count = copy_full_file(src_file, str(out_file))
                print(f"  ✅ Copied {line_count} lines to {out_file}")
            
            file_size_mb = out_file.stat().st_size / 1024 / 1024
            summary[file_type] = {
                "lines": line_count,
                "size_mb": round(file_size_mb, 2),
                "path": str(out_file)
            }
        except Exception as e:
            print(f"  ❌ Failed: {e}")
            summary[file_type] = f"error: {e}"
    
    # Print summary
    print("\n" + "="*60)
    print("SLICING SUMMARY")
    print("="*60)
    for file_type, info in summary.items():
        if isinstance(info, dict):
            print(f"{file_type:4s}: {info['lines']:8,} lines, {info['size_mb']:8.2f} MB")
        else:
            print(f"{file_type:4s}: {info}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
