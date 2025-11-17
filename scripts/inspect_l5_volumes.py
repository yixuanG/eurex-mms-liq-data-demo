#!/usr/bin/env python3
"""
Inspect L5 snapshot data to verify if volumes are constant in raw data.
This checks if the constant volumes we see are a data quality issue or real market behavior.
"""
import argparse
import os
import sys
import pandas as pd
import numpy as np


def inspect_l5_volumes(snapshot_file: str):
    """Inspect volume distribution in L5 snapshots."""
    
    print(f"📂 Inspecting: {snapshot_file}\n")
    
    # Detect file format
    if snapshot_file.endswith('.parquet'):
        df = pd.read_parquet(snapshot_file)
        file_format = "Parquet"
    elif snapshot_file.endswith('.csv'):
        df = pd.read_csv(snapshot_file)
        file_format = "CSV"
    else:
        raise ValueError(f"Unsupported file format: {snapshot_file}")
    
    print(f"✅ Loaded {len(df):,} snapshots from {file_format} file\n")
    print(f"📊 Columns: {df.columns.tolist()}\n")
    
    # Identify volume columns
    bid_size_cols = [col for col in df.columns if 'bid_size' in col.lower()]
    ask_size_cols = [col for col in df.columns if 'ask_size' in col.lower()]
    
    print(f"🔍 Found {len(bid_size_cols)} bid size columns: {bid_size_cols}")
    print(f"🔍 Found {len(ask_size_cols)} ask size columns: {ask_size_cols}\n")
    
    # Check each level's volume distribution
    print("="*80)
    print("VOLUME DISTRIBUTION BY LEVEL")
    print("="*80)
    
    for i, (bid_col, ask_col) in enumerate(zip(bid_size_cols, ask_size_cols), 1):
        print(f"\n📊 Level {i}: {bid_col} / {ask_col}")
        print("-" * 60)
        
        # Bid side statistics
        bid_values = df[bid_col].dropna()
        print(f"  Bid side:")
        print(f"    Non-null values: {len(bid_values):,} / {len(df):,}")
        print(f"    Unique values: {bid_values.nunique()}")
        print(f"    Min: {bid_values.min()}, Max: {bid_values.max()}, Mean: {bid_values.mean():.2f}")
        print(f"    Std Dev: {bid_values.std():.4f}")
        
        if bid_values.nunique() <= 10:
            print(f"    Value counts:")
            value_counts = bid_values.value_counts().sort_index()
            for val, count in value_counts.items():
                pct = count / len(bid_values) * 100
                print(f"      {val}: {count:,} ({pct:.1f}%)")
        
        # Ask side statistics
        ask_values = df[ask_col].dropna()
        print(f"  Ask side:")
        print(f"    Non-null values: {len(ask_values):,} / {len(df):,}")
        print(f"    Unique values: {ask_values.nunique()}")
        print(f"    Min: {ask_values.min()}, Max: {ask_values.max()}, Mean: {ask_values.mean():.2f}")
        print(f"    Std Dev: {ask_values.std():.4f}")
        
        if ask_values.nunique() <= 10:
            print(f"    Value counts:")
            value_counts = ask_values.value_counts().sort_index()
            for val, count in value_counts.items():
                pct = count / len(ask_values) * 100
                print(f"      {val}: {count:,} ({pct:.1f}%)")
    
    # Show sample snapshots
    print("\n" + "="*80)
    print("SAMPLE SNAPSHOTS (First 10)")
    print("="*80)
    
    display_cols = ['ts_ns'] + bid_size_cols + ask_size_cols
    display_cols = [col for col in display_cols if col in df.columns]
    
    print(df[display_cols].head(10).to_string(index=False))
    
    # Check temporal variation
    print("\n" + "="*80)
    print("TEMPORAL VARIATION CHECK")
    print("="*80)
    
    if 'ts_ns' in df.columns:
        df_sorted = df.sort_values('ts_ns')
        
        # Sample every N snapshots to see if volumes change over time
        sample_indices = np.linspace(0, len(df_sorted)-1, min(20, len(df_sorted)), dtype=int)
        sample_df = df_sorted.iloc[sample_indices]
        
        print("\n📊 Sampled snapshots across time:")
        print(sample_df[display_cols].to_string(index=False))
        
        # Check if volumes ever change
        for col in bid_size_cols + ask_size_cols:
            if col in df.columns:
                changes = (df_sorted[col] != df_sorted[col].shift()).sum()
                print(f"\n{col}: {changes} changes detected (out of {len(df):,} snapshots)")
    
    # Summary and diagnosis
    print("\n" + "="*80)
    print("DIAGNOSIS")
    print("="*80)
    
    total_bid_cols_checked = len(bid_size_cols)
    constant_bid_cols = sum(1 for col in bid_size_cols if df[col].nunique() <= 2)
    
    total_ask_cols_checked = len(ask_size_cols)
    constant_ask_cols = sum(1 for col in ask_size_cols if df[col].nunique() <= 2)
    
    print(f"\n✓ Checked {total_bid_cols_checked} bid size columns")
    print(f"  - {constant_bid_cols} have ≤2 unique values (likely constant)")
    print(f"✓ Checked {total_ask_cols_checked} ask size columns")
    print(f"  - {constant_ask_cols} have ≤2 unique values (likely constant)")
    
    if constant_bid_cols == total_bid_cols_checked and constant_ask_cols == total_ask_cols_checked:
        print("\n⚠️  FINDING: Volumes are essentially CONSTANT in the raw data!")
        print("    This is NOT a processing artifact - the source data has stable depth.")
        print("    This is NORMAL for:")
        print("    • Low-liquidity instruments")
        print("    • Market makers maintaining constant depth")
        print("    • Stable orderbook environments")
    else:
        print("\n✓ FINDING: Volumes DO vary in the raw data")
        print("    The constant aggregated volumes may be due to:")
        print("    • Aggregation methodology")
        print("    • Time window selection")
        print("    • Filtering during reconstruction")
    
    return df


def main():
    ap = argparse.ArgumentParser(description="Inspect L5 snapshot volume distribution")
    ap.add_argument("--file", required=True, help="Path to L5 snapshots file (CSV or Parquet)")
    args = ap.parse_args()
    
    if not os.path.exists(args.file):
        print(f"❌ Error: File not found: {args.file}")
        return 1
    
    try:
        inspect_l5_volumes(args.file)
        print("\n✅ Inspection complete!")
        return 0
    except Exception as e:
        print(f"\n❌ Error during inspection: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
