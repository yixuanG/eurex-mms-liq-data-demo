#!/usr/bin/env python3
"""
Check the maximum price level depth in DI data.

Usage:
    python check_max_depth.py --di <DI_FILE> --mapping <MAPPING_JSON> [--sample-limit N]
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from eurex_liquidity.parser import extract_entry_tokens_from_di_line, tokens_to_event, DiMapping


def check_max_depth(di_path: str, mapping: DiMapping, sample_limit: int = None) -> dict:
    """
    Analyze DI file to find maximum price level depth.
    
    Returns:
        dict with keys: max_level, level_counts, total_entries, sample_size
    """
    level_counts = Counter()
    total_entries = 0
    lines_scanned = 0
    
    with open(di_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            if sample_limit and lines_scanned >= sample_limit:
                break
                
            line = line.strip()
            if not line:
                continue
                
            entries = extract_entry_tokens_from_di_line(line)
            lines_scanned += 1
            
            for tokens in entries:
                evt = tokens_to_event(tokens, mapping)
                level = evt.get('price_level')
                
                if level is not None:
                    level_counts[level] += 1
                    total_entries += 1
    
    max_level = max(level_counts.keys()) if level_counts else 0
    
    return {
        'max_level': max_level,
        'level_counts': dict(sorted(level_counts.items())),
        'total_entries': total_entries,
        'lines_scanned': lines_scanned
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Check maximum depth level in DI data")
    ap.add_argument("--di", required=True, help="Path to DI CSV file")
    ap.add_argument("--mapping", required=True, help="Path to DI mapping JSON")
    ap.add_argument("--sample-limit", type=int, default=None, 
                    help="Limit number of lines to scan (default: scan all)")
    args = ap.parse_args()
    
    # Load mapping
    with open(args.mapping, 'r', encoding='utf-8') as f:
        mapping_dict = json.load(f)
    mapping = DiMapping(**mapping_dict)
    
    print(f"[INFO] Analyzing DI file: {args.di}")
    if args.sample_limit:
        print(f"[INFO] Sampling first {args.sample_limit} lines")
    else:
        print(f"[INFO] Scanning entire file...")
    
    result = check_max_depth(args.di, mapping, args.sample_limit)
    
    print(f"\n{'='*60}")
    print(f"MAXIMUM DEPTH ANALYSIS")
    print(f"{'='*60}")
    print(f"Maximum price level found: {result['max_level']}")
    print(f"Total entries analyzed: {result['total_entries']}")
    print(f"Lines scanned: {result['lines_scanned']}")
    
    print(f"\nPrice Level Distribution:")
    for level, count in result['level_counts'].items():
        pct = (count / result['total_entries'] * 100) if result['total_entries'] > 0 else 0
        print(f"  Level {level:2d}: {count:8,} entries ({pct:5.1f}%)")
    
    print(f"\n{'='*60}")
    print(f"RECOMMENDATION:")
    print(f"{'='*60}")
    max_useful_level = result['max_level']
    print(f"✅ Maximum useful level: L{max_useful_level}")
    print(f"⚠️  Using --levels > {max_useful_level} will not capture additional depth")
    print(f"💡 Suggested configurations:")
    print(f"   • L1 : Basic best bid/ask")
    if max_useful_level >= 5:
        print(f"   • L5 : Rich market depth (recommended)")
    if max_useful_level >= 10:
        print(f"   • L10: Deep institutional analysis")
    print(f"   • L{max_useful_level}: Maximum available depth")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
