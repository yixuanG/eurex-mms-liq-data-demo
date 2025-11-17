#!/usr/bin/env python3
"""
Aggregate L5 snapshots to 1-second metrics with multi-level analysis.

Enhanced metrics:
- L1 metrics (same as before)
- L5 depth metrics (total volume, weighted avg price, etc.)
- Market depth and liquidity measures
"""
import argparse
import json
import os
import sys
from typing import Dict

import pandas as pd
import numpy as np

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.eurex_liquidity.parser import (
    extract_entry_tokens_from_di_line,
    DiMapping,
    tokens_to_event,
)


def compute_l5_metrics(group_df: pd.DataFrame) -> pd.Series:
    """Compute L5 aggregated metrics for a 1-second group."""
    last_row = group_df.iloc[-1]  # Last snapshot in the second
    
    # L1 metrics (same as before)
    best_bid = last_row.get('bid_price_1')
    best_ask = last_row.get('ask_price_1')
    bid_size_1 = last_row.get('bid_size_1', 0) or 0
    ask_size_1 = last_row.get('ask_size_1', 0) or 0
    
    # L5 volume aggregation
    total_bid_volume = 0
    total_ask_volume = 0
    weighted_bid_price = 0
    weighted_ask_price = 0
    
    for i in range(1, 6):  # L1 to L5
        bid_price = last_row.get(f'bid_price_{i}')
        bid_size = last_row.get(f'bid_size_{i}', 0) or 0
        ask_price = last_row.get(f'ask_price_{i}')
        ask_size = last_row.get(f'ask_size_{i}', 0) or 0
        
        if bid_price and bid_size > 0:
            total_bid_volume += bid_size
            weighted_bid_price += bid_price * bid_size
            
        if ask_price and ask_size > 0:
            total_ask_volume += ask_size
            weighted_ask_price += ask_price * ask_size
    
    # Weighted average prices (handle None cases)
    avg_bid_price = weighted_bid_price / total_bid_volume if total_bid_volume > 0 else (best_bid if best_bid else None)
    avg_ask_price = weighted_ask_price / total_ask_volume if total_ask_volume > 0 else (best_ask if best_ask else None)
    
    # Standard L1 metrics
    spread_abs = (best_ask - best_bid) if (best_bid and best_ask) else None
    midprice = (best_ask + best_bid) / 2 if (best_bid and best_ask) else None
    spread_rel = spread_abs / midprice if (spread_abs and midprice) else None
    
    imbalance_l1 = (bid_size_1 - ask_size_1) / (bid_size_1 + ask_size_1) if (bid_size_1 + ask_size_1) > 0 else None
    imbalance_l5 = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume) if (total_bid_volume + total_ask_volume) > 0 else None
    
    microprice_l1 = (best_ask * bid_size_1 + best_bid * ask_size_1) / (bid_size_1 + ask_size_1) if (best_bid and best_ask and (bid_size_1 + ask_size_1) > 0) else midprice
    # Fix: Check that avg_bid_price and avg_ask_price are not None before multiplication
    microprice_l5 = ((avg_ask_price * total_bid_volume + avg_bid_price * total_ask_volume) / (total_bid_volume + total_ask_volume)) if (avg_bid_price is not None and avg_ask_price is not None and (total_bid_volume + total_ask_volume) > 0) else midprice
    
    return pd.Series({
        # L1 metrics
        'best_bid': best_bid,
        'best_ask': best_ask,
        'bid_size_1': bid_size_1,
        'ask_size_1': ask_size_1,
        'spread_abs': spread_abs,
        'spread_rel': spread_rel,
        'midprice': midprice,
        'imbalance_l1': imbalance_l1,
        'microprice_l1': microprice_l1,
        
        # L5 metrics
        'total_bid_volume': total_bid_volume,
        'total_ask_volume': total_ask_volume,
        'avg_bid_price': avg_bid_price,
        'avg_ask_price': avg_ask_price,
        'imbalance_l5': imbalance_l5,
        'microprice_l5': microprice_l5,
        'depth_ratio': total_ask_volume / total_bid_volume if total_bid_volume > 0 else None,
        
        # Market depth
        'total_volume': total_bid_volume + total_ask_volume,
        'volume_ratio_l1_to_l5': (bid_size_1 + ask_size_1) / (total_bid_volume + total_ask_volume) if (total_bid_volume + total_ask_volume) > 0 else None
    })


def precompute_all_update_counts(di_file: str, mapping: DiMapping) -> pd.DataFrame:
    """Pre-compute update/cancel counts per second from DI file in ONE PASS."""
    import os
    
    # Get file size for progress tracking
    file_size_mb = os.path.getsize(di_file) / (1024 * 1024)
    print(f"[INFO] Pre-computing update/cancel counts from DI file ({file_size_mb:.1f} MB)...")
    
    records = []
    line_count = 0
    
    with open(di_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line_count += 1
            
            if line_count % 100000 == 0:
                print(f"  Processed {line_count:,} DI lines, {len(records):,} events...")
            
            entries = extract_entry_tokens_from_di_line(line.strip())
            for entry in entries:
                evt = tokens_to_event(entry, mapping)
                ts_ns = evt.get('ts_ns')
                action = evt.get('md_update_action')
                security_id = evt.get('security_id')
                
                if ts_ns and security_id:
                    ts_s = ts_ns // 1_000_000_000
                    is_update = 1 if action in (0, 1, 5) else 0
                    is_cancel = 1 if action == 2 else 0
                    
                    records.append({
                        'security_id': security_id,
                        'ts_s': ts_s,
                        'update_count': is_update,
                        'cancel_count': is_cancel
                    })
    
    print(f"[INFO] Parsed {line_count:,} DI lines, extracted {len(records):,} events")
    
    if not records:
        return pd.DataFrame(columns=['security_id', 'ts_s', 'update_count', 'cancel_count'])
    
    # Aggregate counts by security and second
    print("[INFO] Aggregating event counts by second...")
    df = pd.DataFrame(records)
    counts_df = df.groupby(['security_id', 'ts_s']).agg({
        'update_count': 'sum',
        'cancel_count': 'sum'
    }).reset_index()
    
    print(f"[INFO] Computed counts for {len(counts_df):,} unique (security, second) pairs")
    return counts_df


def main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate L5 snapshots to 1-second metrics")
    ap.add_argument("--seg", type=int, required=True, help="Segment ID")
    ap.add_argument("--l5", required=True, help="Path to L5 snapshots CSV/Parquet")
    ap.add_argument("--di", required=True, help="Path to DI CSV for update counts")
    ap.add_argument("--mapping", required=True, help="Path to DI mapping JSON")
    ap.add_argument("--out", required=True, help="Output directory")
    args = ap.parse_args()

    # Load mapping
    with open(args.mapping, 'r', encoding='utf-8') as f:
        mapping_dict = json.load(f)
    mapping = DiMapping(**mapping_dict)

    # Load L5 data
    print(f"[INFO] Loading L5 snapshots from {args.l5}...")
    if args.l5.endswith('.parquet'):
        df = pd.read_parquet(args.l5)
    else:
        df = pd.read_csv(args.l5)
    
    print(f"[INFO] Loaded {len(df):,} L5 snapshots")
    
    if df.empty:
        print("[WARN] No L5 data found")
        return 1

    # Add second-level timestamp
    df['ts_s'] = (df['ts_ns'] // 1_000_000_000).astype(int)
    
    # Pre-compute update counts from DI file (one pass)
    counts_df = precompute_all_update_counts(args.di, mapping)
    
    # Group by security and second, then aggregate using OPTIMIZED approach
    print("[INFO] Aggregating snapshots by second...")
    num_groups = df.groupby(['security_id', 'ts_s']).ngroups
    print(f"[INFO] Processing {num_groups:,} unique (security, second) groups...")
    
    # For very large datasets (>1M snapshots), use chunked processing
    if len(df) > 1_000_000:
        print("[INFO] Large dataset detected - using chunked aggregation")
        
        # Process by security to reduce memory footprint
        result_chunks = []
        securities = df['security_id'].unique()
        print(f"[INFO] Processing {len(securities)} securities...")
        
        for i, security_id in enumerate(securities, 1):
            if i % 10 == 0 or i == len(securities):
                print(f"  Progress: {i}/{len(securities)} securities")
            
            sec_df = df[df['security_id'] == security_id]
            sec_result = sec_df.groupby(['security_id', 'ts_s'], as_index=False).apply(
                compute_l5_metrics
            ).reset_index(drop=True)
            result_chunks.append(sec_result)
        
        result_df = pd.concat(result_chunks, ignore_index=True)
    else:
        # Standard aggregation for smaller datasets
        result_df = df.groupby(['security_id', 'ts_s'], as_index=False).apply(
            compute_l5_metrics
        ).reset_index(drop=True)
    
    print(f"[INFO] Aggregation complete: {len(result_df):,} rows")
    
    # Merge with update counts
    print("[INFO] Merging update/cancel counts...")
    result_df = result_df.merge(
        counts_df,
        on=['security_id', 'ts_s'],
        how='left'
    )
    
    # Fill missing counts with 0
    result_df['update_count'] = result_df['update_count'].fillna(0).astype(int)
    result_df['cancel_count'] = result_df['cancel_count'].fillna(0).astype(int)
    
    result_df = result_df.sort_values(['security_id', 'ts_s']).reset_index(drop=True)
    print(f"[INFO] Final result: {len(result_df):,} rows")

    # Determine level from input file name
    level = 5  # Default
    if 'l20' in args.l5 or 'L20' in args.l5:
        level = 20
    elif 'l10' in args.l5 or 'L10' in args.l5:
        level = 10
    elif 'l6' in args.l5 or 'L6' in args.l5:
        level = 6
    elif 'l5' in args.l5 or 'L5' in args.l5:
        level = 5
    elif 'l4' in args.l5 or 'L4' in args.l5:
        level = 4
    elif 'l3' in args.l5 or 'L3' in args.l5:
        level = 3
    elif 'l2' in args.l5 or 'L2' in args.l5:
        level = 2
    elif 'l1' in args.l5 or 'L1' in args.l5:
        level = 1
    
    # Write outputs with L{level} subdirectory structure
    level_out_dir = os.path.join(args.out, f"l{level}")
    os.makedirs(level_out_dir, exist_ok=True)
    
    parq = os.path.join(level_out_dir, f"l{level}_agg_1s_seg{args.seg}.parquet") 
    csv_path = os.path.join(level_out_dir, f"l{level}_agg_1s_seg{args.seg}.csv")
    
    try:
        result_df.to_parquet(parq, index=False)
        print("[OK] Wrote:", parq, "rows=", len(result_df))
    except Exception as e:
        print("[WARN] Parquet write failed:", e)

    result_df.to_csv(csv_path, index=False)
    print("[OK] Wrote:", csv_path, "rows=", len(result_df))

    print("\n[Preview] First 6 rows:")
    print(result_df.head(6))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
