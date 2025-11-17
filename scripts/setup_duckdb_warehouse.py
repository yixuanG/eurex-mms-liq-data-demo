#!/usr/bin/env python3
"""
Initialize DuckDB warehouse for Eurex liquidity analysis.

This script:
1. Creates/connects to the DuckDB database
2. Loads 1s aggregates from all processed segments
3. Creates multi-granularity views (5s, 1m, 5m, 15m)
4. Adds product metadata
5. Creates useful analytics views

Usage (Colab):
  python scripts/setup_duckdb_warehouse.py \
    --db-path "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/warehouse/eurex.duckdb" \
    --data-dir "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_segments" \
    --recreate
"""
import argparse
import glob
import sys
from pathlib import Path

import duckdb
import pandas as pd


def create_base_table(con: duckdb.DuckDBPyConnection, data_dir: str, mode: str = 'recreate') -> int:
    """Load 1s aggregates from all segments into base table.
    
    Args:
        con: DuckDB connection
        data_dir: Directory containing segment data
        mode: 'recreate' (drop/create) or 'incremental' (add only new segments)
    """
    print("\n[1/5] Loading 1s aggregates from all segments...")
    print("="*70)
    print(f"Mode: {mode.upper()}")
    print("="*70)
    
    # Find all 1s aggregate Parquet files
    pattern = f"{data_dir}/seg_*/l*/*_agg_1s_*.parquet"
    files = glob.glob(pattern)
    
    if not files:
        print(f"⚠️  No aggregate files found in {data_dir}")
        print(f"   Pattern: {pattern}")
        return 0
    
    print(f"Found {len(files)} aggregate files")
    
    if mode == 'recreate':
        # FULL REFRESH: Drop and recreate entire table
        print("\n🔄 FULL REFRESH MODE: Dropping and recreating table...")
        for f in sorted(files):
            rel_path = Path(f).relative_to(data_dir)
            print(f"  - {rel_path}")
        
        con.execute("DROP TABLE IF EXISTS metrics_1s")
        
        # Load each file separately and add segment_id from path
        print("\nLoading files with segment_id extraction...")
        for i, f in enumerate(files):
            # Extract segment ID from path: .../seg_48/...
            seg_id = None
            for part in Path(f).parts:
                if part.startswith('seg_'):
                    seg_id = int(part.split('_')[1])
                    break
            
            if seg_id is None:
                print(f"  ⚠️  Cannot extract segment_id from {f}, skipping")
                continue
            
            if i == 0:
                # Create table from first file
                con.execute(f"""
                    CREATE TABLE metrics_1s AS
                    SELECT *, {seg_id} AS segment_id
                    FROM read_parquet('{f}')
                """)
            else:
                # Insert subsequent files
                con.execute(f"""
                    INSERT INTO metrics_1s
                    SELECT *, {seg_id} AS segment_id
                    FROM read_parquet('{f}')
                """)
            
            print(f"  ✅ Loaded segment {seg_id}: {Path(f).name}")
        
    elif mode == 'incremental':
        # INCREMENTAL: Add only new segments
        print("\n➕ INCREMENTAL MODE: Adding only new segments...")
        
        # Check if table exists
        table_exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'metrics_1s'
        """).fetchone()[0] > 0
        
        if not table_exists:
            print("⚠️  Table doesn't exist. Switching to recreate mode...")
            return create_base_table(con, data_dir, mode='recreate')
        
        # Get existing segments
        existing_segments = set(
            con.execute("SELECT DISTINCT segment_id FROM metrics_1s").df()['segment_id'].tolist()
        )
        print(f"\nExisting segments in database: {sorted(existing_segments)}")
        
        # Extract segment IDs from file paths
        new_files = []
        for f in files:
            # Extract segment ID from path: seg_48/...
            seg_match = Path(f).parts
            for part in seg_match:
                if part.startswith('seg_'):
                    seg_id = int(part.split('_')[1])
                    if seg_id not in existing_segments:
                        new_files.append((f, seg_id))
                    break
        
        if not new_files:
            print("\n✅ No new segments to add. Database is up to date.")
            row_count = con.execute("SELECT COUNT(*) FROM metrics_1s").fetchone()[0]
            return row_count
        
        # Group by segment
        new_segments = {}
        for f, seg_id in new_files:
            if seg_id not in new_segments:
                new_segments[seg_id] = []
            new_segments[seg_id].append(f)
        
        print(f"\n📥 New segments to add: {sorted(new_segments.keys())}")
        
        # Insert new segments
        for seg_id in sorted(new_segments.keys()):
            seg_files = new_segments[seg_id]
            print(f"\n  Adding segment {seg_id} ({len(seg_files)} files)...")
            for f in seg_files:
                rel_path = Path(f).relative_to(data_dir)
                print(f"    - {rel_path}")
            
            # Insert each file with segment_id
            for f in seg_files:
                con.execute(f"""
                    INSERT INTO metrics_1s
                    SELECT *, {seg_id} AS segment_id
                    FROM read_parquet('{f}')
                """)
            
            # Count rows for this segment
            seg_count = con.execute(
                f"SELECT COUNT(*) FROM metrics_1s WHERE segment_id = {seg_id}"
            ).fetchone()[0]
            print(f"    ✅ Added {seg_count:,} rows for segment {seg_id}")
    
    else:
        raise ValueError(f"Unknown mode: {mode}. Use 'recreate' or 'incremental'")
    
    # Get total row count
    row_count = con.execute("SELECT COUNT(*) FROM metrics_1s").fetchone()[0]
    segments = con.execute("SELECT DISTINCT segment_id FROM metrics_1s ORDER BY segment_id").df()
    
    print(f"\n✅ Total rows in metrics_1s: {row_count:,}")
    print(f"   Segments: {segments['segment_id'].tolist()}")
    
    # Show sample
    print("\nSample data:")
    sample = con.execute("SELECT * FROM metrics_1s LIMIT 3").df()
    print(sample.to_string())
    
    return row_count


def create_time_views(con: duckdb.DuckDBPyConnection):
    """Create multi-granularity time aggregation views."""
    print("\n[2/5] Creating time aggregation views...")
    print("="*70)
    
    # 5-second view (recommended default for dashboards)
    print("Creating metrics_5s view...")
    con.execute("""
    CREATE OR REPLACE VIEW metrics_5s AS
    SELECT 
        segment_id,
        security_id,
        (ts_s / 5) * 5 AS ts_s,
        -- Spread metrics (time-weighted average)
        AVG(spread_rel * 10000) AS avg_spread_bps,  -- Convert to bps
        MIN(spread_rel * 10000) AS min_spread_bps,
        MAX(spread_rel * 10000) AS max_spread_bps,
        AVG(spread_abs) AS avg_spread_abs,
        -- Imbalance metrics
        AVG(imbalance_l5) AS avg_imbalance,
        AVG(microprice_l5) AS avg_microprice,
        -- Volume metrics (sum)
        SUM(bid_size_1) AS total_bid_size,
        SUM(ask_size_1) AS total_ask_size,
        AVG(bid_size_1) AS avg_bid_size,
        AVG(ask_size_1) AS avg_ask_size,
        -- Activity metrics (sum)
        SUM(update_count) AS total_updates,
        SUM(cancel_count) AS total_cancels,
        -- Price range
        MIN(best_bid) AS min_bid,
        MAX(best_ask) AS max_ask,
        AVG(best_bid) AS avg_bid,
        AVG(best_ask) AS avg_ask,
        -- Event counts
        COUNT(*) AS num_1s_intervals
    FROM metrics_1s
    WHERE spread_abs IS NOT NULL  -- Filter invalid data
    GROUP BY segment_id, security_id, (ts_s / 5) * 5
    ORDER BY segment_id, security_id, ts_s
    """)
    
    count_5s = con.execute("SELECT COUNT(*) FROM metrics_5s").fetchone()[0]
    print(f"  ✅ metrics_5s: {count_5s:,} rows")
    
    # 1-minute view
    print("Creating metrics_1m view...")
    con.execute("""
    CREATE OR REPLACE VIEW metrics_1m AS
    SELECT 
        segment_id,
        security_id,
        (ts_s / 60) * 60 AS ts_s,
        -- Spread metrics
        AVG(spread_rel * 10000) AS avg_spread_bps,
        MIN(spread_rel * 10000) AS min_spread_bps,
        MAX(spread_rel * 10000) AS max_spread_bps,
        STDDEV(spread_rel * 10000) AS std_spread_bps,
        AVG(spread_abs) AS avg_spread_abs,
        -- Imbalance metrics
        AVG(imbalance_l5) AS avg_imbalance,
        STDDEV(imbalance_l5) AS std_imbalance,
        AVG(microprice_l5) AS avg_microprice,
        -- Volume metrics
        SUM(bid_size_1) AS total_bid_size,
        SUM(ask_size_1) AS total_ask_size,
        AVG(bid_size_1) AS avg_bid_size,
        AVG(ask_size_1) AS avg_ask_size,
        -- Activity metrics
        SUM(update_count) AS total_updates,
        SUM(cancel_count) AS total_cancels,
        AVG(update_count) AS avg_updates_per_sec,
        AVG(cancel_count) AS avg_cancels_per_sec,
        -- Price range
        MIN(best_bid) AS min_bid,
        MAX(best_ask) AS max_ask,
        AVG(best_bid) AS avg_bid,
        AVG(best_ask) AS avg_ask,
        -- Event counts
        COUNT(*) AS num_1s_intervals
    FROM metrics_1s
    WHERE spread_abs IS NOT NULL
    GROUP BY segment_id, security_id, (ts_s / 60) * 60
    ORDER BY segment_id, security_id, ts_s
    """)
    
    count_1m = con.execute("SELECT COUNT(*) FROM metrics_1m").fetchone()[0]
    print(f"  ✅ metrics_1m: {count_1m:,} rows")
    
    # 5-minute view
    print("Creating metrics_5m view...")
    con.execute("""
    CREATE OR REPLACE VIEW metrics_5m AS
    SELECT 
        segment_id,
        security_id,
        (ts_s / 300) * 300 AS ts_s,
        -- Spread metrics
        AVG(spread_rel * 10000) AS avg_spread_bps,
        MIN(spread_rel * 10000) AS min_spread_bps,
        MAX(spread_rel * 10000) AS max_spread_bps,
        STDDEV(spread_rel * 10000) AS std_spread_bps,
        -- Imbalance
        AVG(imbalance_l5) AS avg_imbalance,
        STDDEV(imbalance_l5) AS std_imbalance,
        -- Volume
        SUM(bid_size_1) AS total_bid_size,
        SUM(ask_size_1) AS total_ask_size,
        -- Activity
        SUM(update_count) AS total_updates,
        SUM(cancel_count) AS total_cancels,
        -- Price range
        MIN(best_bid) AS min_bid,
        MAX(best_ask) AS max_ask,
        -- Volatility proxy
        (MAX(best_ask) - MIN(best_bid)) / AVG((best_bid + best_ask) / 2) AS price_range_pct,
        -- Event counts
        COUNT(*) AS num_1s_intervals
    FROM metrics_1s
    WHERE spread_abs IS NOT NULL
    GROUP BY segment_id, security_id, (ts_s / 300) * 300
    ORDER BY segment_id, security_id, ts_s
    """)
    
    count_5m = con.execute("SELECT COUNT(*) FROM metrics_5m").fetchone()[0]
    print(f"  ✅ metrics_5m: {count_5m:,} rows")
    
    print("\n✅ Time aggregation views created")


def create_analytics_views(con: duckdb.DuckDBPyConnection):
    """Create analytics and summary views."""
    print("\n[3/5] Creating analytics views...")
    print("="*70)
    
    # Segment summary
    print("Creating segment_summary view...")
    con.execute("""
    CREATE OR REPLACE VIEW segment_summary AS
    SELECT 
        segment_id,
        COUNT(DISTINCT security_id) AS num_securities,
        COUNT(*) AS num_1s_intervals,
        MIN(ts_s) AS first_ts_s,
        MAX(ts_s) AS last_ts_s,
        (MAX(ts_s) - MIN(ts_s)) / 3600.0 AS trading_hours,
        -- Spread statistics (NULL-safe for one-sided markets)
        AVG(spread_rel * 10000) AS avg_spread_bps,
        MEDIAN(spread_rel * 10000) AS median_spread_bps,
        MIN(spread_rel * 10000) AS min_spread_bps,
        MAX(spread_rel * 10000) AS max_spread_bps,
        -- Activity statistics
        SUM(update_count) AS total_updates,
        SUM(cancel_count) AS total_cancels,
        SUM(update_count) / NULLIF(MAX(ts_s) - MIN(ts_s), 0) AS updates_per_sec,
        -- Volume statistics
        AVG(COALESCE(bid_size_1, 0) + COALESCE(ask_size_1, 0)) AS avg_total_depth_l1,
        -- Market quality flags
        SUM(CASE WHEN spread_abs IS NOT NULL THEN 1 ELSE 0 END) AS num_two_sided_quotes,
        SUM(CASE WHEN best_bid IS NULL AND best_ask IS NOT NULL THEN 1 ELSE 0 END) AS num_ask_only_quotes,
        SUM(CASE WHEN best_bid IS NOT NULL AND best_ask IS NULL THEN 1 ELSE 0 END) AS num_bid_only_quotes
    FROM metrics_1s
    GROUP BY segment_id
    ORDER BY segment_id
    """)
    
    print("  ✅ segment_summary created")
    
    # Security-level summary
    print("Creating security_summary view...")
    con.execute("""
    CREATE OR REPLACE VIEW security_summary AS
    SELECT 
        segment_id,
        security_id,
        COUNT(*) AS num_1s_intervals,
        MIN(ts_s) AS first_ts_s,
        MAX(ts_s) AS last_ts_s,
        -- Liquidity metrics (NULL-safe for one-sided markets)
        AVG(spread_rel * 10000) AS avg_spread_bps,
        MEDIAN(spread_rel * 10000) AS median_spread_bps,
        AVG(imbalance_l5) AS avg_imbalance,
        AVG(COALESCE(bid_size_1, 0) + COALESCE(ask_size_1, 0)) AS avg_total_depth_l1,
        -- Activity
        SUM(update_count) AS total_updates,
        SUM(cancel_count) AS total_cancels,
        SUM(update_count) / NULLIF(MAX(ts_s) - MIN(ts_s), 0) AS updates_per_sec,
        -- Price range
        MIN(best_bid) AS min_bid,
        MAX(best_ask) AS max_ask,
        -- Market quality flags
        SUM(CASE WHEN spread_abs IS NOT NULL THEN 1 ELSE 0 END) AS num_two_sided_quotes,
        SUM(CASE WHEN best_bid IS NULL AND best_ask IS NOT NULL THEN 1 ELSE 0 END) AS num_ask_only_quotes,
        SUM(CASE WHEN best_bid IS NOT NULL AND best_ask IS NULL THEN 1 ELSE 0 END) AS num_bid_only_quotes
    FROM metrics_1s
    GROUP BY segment_id, security_id
    ORDER BY segment_id, total_updates DESC
    """)
    
    print("  ✅ security_summary created")
    
    # Top active securities
    print("Creating top_securities view...")
    con.execute("""
    CREATE OR REPLACE VIEW top_securities AS
    SELECT 
        segment_id,
        security_id,
        total_updates,
        avg_spread_bps,
        median_spread_bps,
        avg_imbalance,
        avg_total_depth_l1,
        updates_per_sec
    FROM security_summary
    ORDER BY total_updates DESC
    LIMIT 100
    """)
    
    print("  ✅ top_securities created")
    
    print("\n✅ Analytics views created")


def create_indexes(con: duckdb.DuckDBPyConnection):
    """Create indexes for performance."""
    print("\n[4/5] Creating indexes...")
    print("="*70)
    
    # Note: DuckDB automatically optimizes queries, but we can hint at common access patterns
    print("DuckDB uses automatic indexing and query optimization")
    print("  - Parquet files are already columnar and compressed")
    print("  - Views are virtual and don't need separate indexes")
    print("  ✅ No manual indexing needed")


def export_sample_data(con: duckdb.DuckDBPyConnection, db_path: str):
    """Export sample data for validation."""
    print("\n[5/5] Exporting sample data...")
    print("="*70)
    
    warehouse_dir = Path(db_path).parent
    samples_dir = warehouse_dir / "samples"
    samples_dir.mkdir(exist_ok=True)
    
    # Export segment summary
    summary = con.execute("SELECT * FROM segment_summary").df()
    summary_path = samples_dir / "segment_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"  ✅ {summary_path.name}")
    
    # Export top securities
    top_sec = con.execute("SELECT * FROM top_securities LIMIT 20").df()
    top_path = samples_dir / "top_20_securities.csv"
    top_sec.to_csv(top_path, index=False)
    print(f"  ✅ {top_path.name}")
    
    # Export 5s sample for one segment
    sample_5s = con.execute("""
        SELECT * FROM metrics_5s 
        WHERE segment_id = (SELECT MIN(segment_id) FROM metrics_5s)
        LIMIT 1000
    """).df()
    sample_5s_path = samples_dir / "sample_5s_metrics.csv"
    sample_5s.to_csv(sample_5s_path, index=False)
    print(f"  ✅ {sample_5s_path.name}")
    
    print(f"\n✅ Sample data exported to {samples_dir}/")


def print_summary(con: duckdb.DuckDBPyConnection):
    """Print database summary."""
    print("\n" + "="*70)
    print("DATABASE SUMMARY")
    print("="*70)
    
    # Tables
    tables = con.execute("SHOW TABLES").df()
    print(f"\nTables: {len(tables)}")
    for _, row in tables.iterrows():
        print(f"  - {row['name']}")
    
    # Views (get from information_schema instead)
    views = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type = 'VIEW' 
        ORDER BY table_name
    """).df()
    print(f"\nViews: {len(views)}")
    for _, row in views.iterrows():
        print(f"  - {row['table_name']}")
    
    # Segment summary
    print("\nSegment Summary:")
    seg_summary = con.execute("SELECT * FROM segment_summary").df()
    print(seg_summary.to_string(index=False))
    
    # Row counts
    print("\nRow Counts:")
    print(f"  - metrics_1s: {con.execute('SELECT COUNT(*) FROM metrics_1s').fetchone()[0]:,}")
    print(f"  - metrics_5s: {con.execute('SELECT COUNT(*) FROM metrics_5s').fetchone()[0]:,}")
    print(f"  - metrics_1m: {con.execute('SELECT COUNT(*) FROM metrics_1m').fetchone()[0]:,}")
    print(f"  - metrics_5m: {con.execute('SELECT COUNT(*) FROM metrics_5m').fetchone()[0]:,}")


def main():
    ap = argparse.ArgumentParser(description="Setup DuckDB warehouse for Eurex liquidity analysis")
    ap.add_argument("--db-path", required=True, help="Path to DuckDB database file")
    ap.add_argument("--data-dir", required=True, help="Directory containing processed segment data")
    ap.add_argument("--recreate", action="store_true", help="Drop and recreate all tables/views (full refresh)")
    ap.add_argument("--mode", choices=['recreate', 'incremental'], default=None,
                    help="Loading mode: 'recreate' (full refresh) or 'incremental' (add new segments only). "
                         "If not specified, uses 'recreate' when --recreate flag is set, otherwise 'incremental'")
    args = ap.parse_args()
    
    # Determine mode
    if args.mode:
        mode = args.mode
    elif args.recreate:
        mode = 'recreate'
    else:
        mode = 'incremental'
    
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    print("="*70)
    print("EUREX LIQUIDITY ANALYSIS - DUCKDB WAREHOUSE SETUP")
    print("="*70)
    print(f"Database: {db_path}")
    print(f"Data dir: {args.data_dir}")
    print(f"Mode:     {mode.upper()}")
    print("="*70)
    
    # Connect to database
    print("\nConnecting to DuckDB...")
    con = duckdb.connect(str(db_path))
    print(f"✅ Connected to {db_path}")
    
    try:
        # Load base table
        row_count = create_base_table(con, args.data_dir, mode=mode)
        
        if row_count == 0:
            print("\n⚠️  No data loaded. Cannot create views.")
            return 1
        
        # Create views
        create_time_views(con)
        create_analytics_views(con)
        create_indexes(con)
        export_sample_data(con, str(db_path))
        
        # Summary
        print_summary(con)
        
        print("\n" + "="*70)
        print("✅ WAREHOUSE SETUP COMPLETE")
        print("="*70)
        print("\nNext steps:")
        print("  1. Connect Power BI to DuckDB")
        print("  2. Use metrics_5s view for default dashboard")
        print("  3. Add granularity slicer (1s/5s/1m/5m)")
        print("  4. Create visualizations")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
