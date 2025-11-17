#!/usr/bin/env python3
"""
Resume merging segment 50 from existing merged batch files.
Use this when the final merge step OOM'd but batch files exist.
"""
import argparse
import os
import pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Resume merge from checkpoint")
    ap.add_argument("--seg", type=int, required=True, help="Segment ID")
    ap.add_argument("--temp-dir", required=True, help="Path to _temp_chunks directory with merged_*.parquet files")
    ap.add_argument("--out-dir", required=True, help="Output directory for final parquet")
    ap.add_argument("--levels", type=int, default=5, help="Number of levels (default: 5)")
    args = ap.parse_args()
    
    # Find all merged batch files
    temp_path = Path(args.temp_dir)
    merged_files = sorted(temp_path.glob("merged_*.parquet"))
    
    if not merged_files:
        print(f"[ERROR] No merged_*.parquet files found in {args.temp_dir}")
        return 1
    
    print(f"[INFO] Found {len(merged_files)} merged batch files")
    print(f"[INFO] Starting ultra-conservative merge (5 files at a time)...")
    
    # Merge in even smaller batches to avoid OOM
    SUPER_BATCH_SIZE = 5
    super_batches = []
    batch_num = 0
    
    for i in range(0, len(merged_files), SUPER_BATCH_SIZE):
        batch_end = min(i + SUPER_BATCH_SIZE, len(merged_files))
        batch_files = merged_files[i:batch_end]
        batch_num += 1
        
        print(f"  Super-batch {batch_num}/{(len(merged_files) + SUPER_BATCH_SIZE - 1) // SUPER_BATCH_SIZE}: merging {len(batch_files)} files...")
        
        # Load and merge this super batch
        dfs = []
        for j, f in enumerate(batch_files):
            print(f"    Loading {f.name}...")
            dfs.append(pd.read_parquet(f))
        
        print(f"    Concatenating {len(dfs)} dataframes...")
        super_batch_df = pd.concat(dfs, ignore_index=True)
        
        print(f"    Sorting {len(super_batch_df):,} rows...")
        super_batch_df = super_batch_df.sort_values(['ts_ns', 'security_id']).reset_index(drop=True)
        
        # Write to temp super batch file with sequential numbering
        super_batch_file = temp_path / f"final_batch_{batch_num:02d}.parquet"
        print(f"    Writing to {super_batch_file.name}...")
        super_batch_df.to_parquet(super_batch_file, index=False)
        super_batches.append(super_batch_file)
        
        del dfs, super_batch_df
        print(f"    ✅ Super-batch {batch_num} complete, freed memory")
    
    print(f"\n[INFO] Created {len(super_batches)} super-batches")
    print(f"[INFO] Final merge using pairwise combination to minimize memory...")
    
    # Write outputs with L{levels} subdirectory structure  
    level_dir = f"l{args.levels}"
    out_path = Path(args.out_dir) / level_dir
    out_path.mkdir(parents=True, exist_ok=True)
    
    parq_file = out_path / f"l{args.levels}_snapshots_seg{args.seg}.parquet"
    csv_file = out_path / f"l{args.levels}_snapshots_seg{args.seg}.csv"
    
    # Pairwise merge to keep memory usage low
    # Merge 2 files at a time, write result, repeat
    current_files = list(super_batches)
    merge_round = 1
    
    while len(current_files) > 1:
        print(f"\n[Round {merge_round}] Merging {len(current_files)} files pairwise...")
        next_files = []
        
        for i in range(0, len(current_files), 2):
            if i + 1 < len(current_files):
                # Merge pair
                file1 = current_files[i]
                file2 = current_files[i + 1]
                print(f"  Merging {file1.name} + {file2.name}...")
                
                df1 = pd.read_parquet(file1)
                df2 = pd.read_parquet(file2)
                df_merged = pd.concat([df1, df2], ignore_index=True)
                del df1, df2
                
                # Write merged result
                merged_file = temp_path / f"round{merge_round}_pair{i//2}.parquet"
                df_merged.to_parquet(merged_file, index=False)
                next_files.append(merged_file)
                
                print(f"    → {merged_file.name} ({len(df_merged):,} rows)")
                del df_merged
            else:
                # Odd file, carry forward
                next_files.append(current_files[i])
                print(f"  Carrying forward {current_files[i].name}")
        
        current_files = next_files
        merge_round += 1
    
    # Final file - use streaming to avoid loading full 20M rows
    print(f"\n[INFO] Final merge complete!")
    print(f"[INFO] Writing to final location using streaming (to avoid OOM)...")
    
    # Use pyarrow for memory-efficient concatenation
    import pyarrow.parquet as pq
    import pyarrow as pa
    
    # Read all remaining files as arrow tables and concatenate
    print(f"[INFO] Streaming {len(current_files)} file(s) to {parq_file.name}...")
    
    tables = []
    total_rows = 0
    for f in current_files:
        print(f"  Reading {f.name}...")
        table = pq.read_table(f)
        total_rows += len(table)
        tables.append(table)
    
    print(f"[INFO] Concatenating {len(tables)} tables ({total_rows:,} total rows)...")
    final_table = pa.concat_tables(tables)
    
    print(f"[INFO] Writing parquet file ({total_rows:,} rows)...")
    pq.write_table(final_table, parq_file)
    print(f"[OK] Wrote: {parq_file} ({total_rows:,} rows)")
    
    # For CSV, read in chunks to avoid OOM
    print(f"\n[INFO] Writing CSV in chunks...")
    df_sample = pd.read_parquet(parq_file, nrows=3)  # Just for preview
    
    # Write CSV using chunked reading
    import pyarrow.csv as csv_writer
    chunk_size = 1_000_000
    first_chunk = True
    rows_written = 0
    
    with open(csv_file, 'w') as f_out:
        reader = pq.ParquetFile(parq_file)
        for batch in reader.iter_batches(batch_size=chunk_size):
            batch_df = batch.to_pandas()
            batch_df.to_csv(f_out, index=False, header=first_chunk, mode='a' if not first_chunk else 'w')
            first_chunk = False
            rows_written += len(batch_df)
            if rows_written % 5_000_000 == 0:
                print(f"  Written {rows_written:,} rows to CSV...")
    
    print(f"[OK] Wrote: {csv_file} ({rows_written:,} rows)")
    
    print("\n[Preview] First 3 rows:")
    print(df_sample.head(3))
    
    print(f"\n✅ Snapshots complete! Now running aggregation to create agg_1s files...")
    
    # Run aggregation step to create l5_agg_1s_seg50 files
    import subprocess
    import sys
    
    # Find the necessary files
    seg_dir = Path(args.out_dir)
    di_file = seg_dir / f"DI_{args.seg}_20201201_fullday.csv"
    mapping_file = seg_dir / "di_mapping.json"
    
    if not di_file.exists():
        print(f"[WARN] DI file not found: {di_file}")
        print("[WARN] Skipping aggregation step")
        return 0
    
    if not mapping_file.exists():
        print(f"[WARN] Mapping file not found: {mapping_file}")
        print("[WARN] Skipping aggregation step")
        return 0
    
    # Get path to aggregate_l5.py
    scripts_dir = Path(__file__).parent
    agg_script = scripts_dir / "aggregate_l5.py"
    
    print(f"\n[INFO] Running aggregation script...")
    print(f"  Script: {agg_script}")
    print(f"  L5 snapshots: {parq_file}")
    print(f"  DI file: {di_file}")
    print(f"  Mapping: {mapping_file}")
    print(f"  Output: {seg_dir}")
    
    cmd = [
        sys.executable,
        str(agg_script),
        "--seg", str(args.seg),
        "--l5", str(parq_file),
        "--di", str(di_file),
        "--mapping", str(mapping_file),
        "--out", str(seg_dir)
    ]
    
    print(f"\n[CMD] {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        print(f"\n[WARN] Aggregation failed with exit code {result.returncode}")
        print("[INFO] But snapshots are complete, you can retry aggregation separately")
        return 0
    
    print(f"\n✅ SUCCESS! Segment {args.seg} fully complete (snapshots + aggregation)!")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
