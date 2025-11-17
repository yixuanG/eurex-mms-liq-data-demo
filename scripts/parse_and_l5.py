#!/usr/bin/env python3
"""
Parse DI events and build L5 (5-level) order book snapshots.

Enhanced version of parse_and_l1.py that tracks top 5 price levels.
"""
import argparse
import json
import os
import sys
from collections import defaultdict

import pandas as pd

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.eurex_liquidity.parser import (
    extract_entry_tokens_from_di_line,
    DiMapping,
    tokens_to_event,
)
from src.eurex_liquidity.orderbook_multi import MultiLevelBook


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse DI events into L5 snapshots per security")
    ap.add_argument("--seg", type=int, required=True, help="Segment ID")
    ap.add_argument("--di", required=True, help="Path to sliced DI CSV")
    ap.add_argument("--mapping", required=True, help="Path to DI mapping JSON")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--levels", type=int, default=5, help="Number of levels to track (default: 5)")
    args = ap.parse_args()

    # Load mapping
    with open(args.mapping, 'r', encoding='utf-8') as f:
        mapping_dict = json.load(f)
    mapping = DiMapping(**mapping_dict)

    # Track books per security
    books: defaultdict[int, MultiLevelBook] = defaultdict(lambda: MultiLevelBook(max_levels=args.levels))
    snapshots = []
    
    # For large files: write snapshots to disk in chunks to avoid OOM
    CHUNK_SIZE = 50000  # Write every 50K snapshots
    chunk_files = []
    chunk_counter = 0

    lines_processed = 0
    events_processed = 0
    changes_detected = 0

    print(f"[INFO] Parsing DI file: {args.di}")
    print(f"[INFO] Tracking top {args.levels} levels per side")
    print(f"[INFO] Memory optimization: writing snapshots in chunks of {CHUNK_SIZE}")

    # Create temp directory for chunks
    level_dir = f"l{args.levels}"
    level_out_dir = os.path.join(args.out, level_dir)
    os.makedirs(level_out_dir, exist_ok=True)
    temp_dir = os.path.join(level_out_dir, "_temp_chunks")
    os.makedirs(temp_dir, exist_ok=True)

    def flush_snapshots_to_disk():
        """Write current snapshots to a temporary chunk file and clear memory."""
        nonlocal chunk_counter, snapshots
        if not snapshots:
            return
        
        chunk_file = os.path.join(temp_dir, f"chunk_{chunk_counter:04d}.parquet")
        df_chunk = pd.DataFrame(snapshots)
        
        # Sort chunk before writing to enable efficient merge later
        df_chunk = df_chunk.sort_values(['ts_ns', 'security_id']).reset_index(drop=True)
        df_chunk.to_parquet(chunk_file, index=False)
        chunk_files.append(chunk_file)
        
        print(f"  💾 Wrote chunk {chunk_counter} ({len(snapshots):,} snapshots) to disk, freed memory")
        chunk_counter += 1
        snapshots = []
        return snapshots

    with open(args.di, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 5000 == 0:
                print(f"  Processed {line_num} lines, {changes_detected:,} changes...")
                
            lines_processed += 1
            entries = extract_entry_tokens_from_di_line(line.strip())
            
            for entry in entries:
                evt = tokens_to_event(entry, mapping)
                events_processed += 1
                
                sec = evt.get('security_id')
                if sec is None:
                    continue
                    
                sec = int(sec)
                changed = books[sec].apply_event(evt)
                
                if changed:
                    changes_detected += 1
                    # Get L5 snapshot
                    snap = books[sec].snapshot_l5()
                    snap['security_id'] = sec
                    snap['action'] = evt.get('md_update_action', 0)
                    snapshots.append(snap)
                    
                    # Flush to disk if chunk is full
                    if len(snapshots) >= CHUNK_SIZE:
                        snapshots = flush_snapshots_to_disk()

    # Flush any remaining snapshots
    if snapshots:
        flush_snapshots_to_disk()

    print(f"[INFO] Processing complete:")
    print(f"  Lines: {lines_processed}")
    print(f"  Events: {events_processed}")
    print(f"  Changes: {changes_detected}")
    print(f"  Chunks written: {len(chunk_files)}")

    if not chunk_files:
        print("[WARN] No L5 changes detected; nothing to write.")
        return 0

    # Combine all sorted chunks using merge (more memory efficient than loading all at once)
    print(f"[INFO] Merging {len(chunk_files)} pre-sorted chunks...")
    
    # For very large datasets, write directly to final file without loading all in memory
    # We'll combine chunks in batches to avoid OOM
    BATCH_SIZE = 20  # Merge 20 chunks at a time
    
    if len(chunk_files) > BATCH_SIZE:
        print(f"[INFO] Using batched merge (batches of {BATCH_SIZE} chunks)")
        merged_files = []
        
        for batch_start in range(0, len(chunk_files), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(chunk_files))
            batch_files = chunk_files[batch_start:batch_end]
            
            print(f"  Merging batch {batch_start//BATCH_SIZE + 1} (chunks {batch_start+1}-{batch_end})...")
            
            # Load and concat this batch
            batch_dfs = [pd.read_parquet(f) for f in batch_files]
            batch_df = pd.concat(batch_dfs, ignore_index=True)
            
            # Sort the batch
            batch_df = batch_df.sort_values(['ts_ns', 'security_id']).reset_index(drop=True)
            
            # Write to temp merged file
            merged_file = os.path.join(temp_dir, f"merged_{batch_start:04d}.parquet")
            batch_df.to_parquet(merged_file, index=False)
            merged_files.append(merged_file)
            
            del batch_dfs, batch_df
        
        print(f"[INFO] Created {len(merged_files)} merged batches, final merge...")
        
        # Final merge of all batches
        final_dfs = [pd.read_parquet(f) for f in merged_files]
        df = pd.concat(final_dfs, ignore_index=True)
        df = df.sort_values(['ts_ns', 'security_id']).reset_index(drop=True)
    else:
        # Small enough to merge directly
        print(f"  Loading all {len(chunk_files)} chunks...")
        dfs = []
        for i, chunk_file in enumerate(chunk_files):
            if (i + 1) % 10 == 0:
                print(f"    Chunk {i + 1}/{len(chunk_files)}...")
            dfs.append(pd.read_parquet(chunk_file))
        
        df = pd.concat(dfs, ignore_index=True)
        df = df.sort_values(['ts_ns', 'security_id']).reset_index(drop=True)
    
    print(f"[INFO] Final dataset: {len(df):,} snapshots")
    
    # Clean up chunk files
    print(f"[INFO] Cleaning up temporary chunks...")
    import shutil
    shutil.rmtree(temp_dir)

    # Write outputs with L{levels} subdirectory structure
    level_dir = f"l{args.levels}"
    level_out_dir = os.path.join(args.out, level_dir)
    os.makedirs(level_out_dir, exist_ok=True)
    
    parq = os.path.join(level_out_dir, f"l{args.levels}_snapshots_seg{args.seg}.parquet")
    csv_path = os.path.join(level_out_dir, f"l{args.levels}_snapshots_seg{args.seg}.csv")
    
    try:
        df.to_parquet(parq, index=False)
        print("[OK] Wrote:", parq, "rows=", len(df))
    except Exception as e:
        print("[WARN] Parquet write failed:", e)

    df.to_csv(csv_path, index=False)
    print("[OK] Wrote:", csv_path, "rows=", len(df))

    print("\n[Preview] First 3 rows:")
    print(df.head(3))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
