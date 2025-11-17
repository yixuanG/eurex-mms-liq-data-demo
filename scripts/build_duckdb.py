#!/usr/bin/env python3
"""
Build DuckDB warehouse from L1 and L5 aggregated data.

Usage:
    python build_duckdb.py --seg 48 --data-dir path/to/data_samples/48-FSTK-ADSG --db path/to/eurex.duckdb
"""
import argparse
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd


def create_schema(con: duckdb.DuckDBPyConnection):
    """Create eurex schema if it doesn't exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS eurex")
    con.execute("SET schema = 'eurex'")
    print("[OK] Schema 'eurex' ready and set as default")


def load_l1_data(con: duckdb.DuckDBPyConnection, seg: int, data_dir: str) -> bool:
    """Load L1 aggregated data into DuckDB."""
    l1_dir = os.path.join(data_dir, "l1")
    parquet_file = os.path.join(l1_dir, f"l1_agg_1s_seg{seg}.parquet")
    csv_file = os.path.join(l1_dir, f"l1_agg_1s_seg{seg}.csv")
    
    # Try parquet first, fall back to CSV
    if os.path.exists(parquet_file):
        source_file = parquet_file
        file_format = "parquet"
    elif os.path.exists(csv_file):
        source_file = csv_file
        file_format = "csv"
    else:
        print(f"[WARN] No L1 data found for segment {seg}")
        return False
    
    print(f"[INFO] Loading L1 data from {source_file}")
    
    # Create or replace table
    con.execute(f"""
        CREATE OR REPLACE TABLE l1_agg_seg{seg} AS 
        SELECT 
            security_id,
            ts_s,
            to_timestamp(ts_s) as timestamp,
            best_bid,
            best_ask,
            bid_size,
            ask_size,
            spread_abs,
            spread_rel,
            imbalance,
            microprice,
            update_count,
            cancel_count,
            {seg} as segment_id
        FROM read_{file_format}('{source_file}')
    """)
    
    count = con.execute(f"SELECT COUNT(*) FROM l1_agg_seg{seg}").fetchone()[0]
    print(f"[OK] Loaded {count:,} L1 records into l1_agg_seg{seg}")
    return True


def load_l5_data(con: duckdb.DuckDBPyConnection, seg: int, data_dir: str) -> bool:
    """Load L5 aggregated data into DuckDB."""
    l5_dir = os.path.join(data_dir, "l5")
    parquet_file = os.path.join(l5_dir, f"l5_agg_1s_seg{seg}.parquet")
    csv_file = os.path.join(l5_dir, f"l5_agg_1s_seg{seg}.csv")
    
    # Try parquet first, fall back to CSV
    if os.path.exists(parquet_file):
        source_file = parquet_file
        file_format = "parquet"
    elif os.path.exists(csv_file):
        source_file = csv_file
        file_format = "csv"
    else:
        print(f"[WARN] No L5 data found for segment {seg}")
        return False
    
    print(f"[INFO] Loading L5 data from {source_file}")
    
    # Create or replace table
    con.execute(f"""
        CREATE OR REPLACE TABLE l5_agg_seg{seg} AS 
        SELECT 
            security_id,
            ts_s,
            to_timestamp(ts_s) as timestamp,
            best_bid,
            best_ask,
            bid_size_1 as bid_size,
            ask_size_1 as ask_size,
            spread_abs,
            spread_rel,
            imbalance_l1 as imbalance,
            microprice_l1 as microprice,
            total_bid_volume,
            total_ask_volume,
            avg_bid_price,
            avg_ask_price,
            imbalance_l5,
            microprice_l5,
            depth_ratio,
            volume_ratio_l1_to_l5,
            update_count,
            cancel_count,
            {seg} as segment_id
        FROM read_{file_format}('{source_file}')
    """)
    
    count = con.execute(f"SELECT COUNT(*) FROM l5_agg_seg{seg}").fetchone()[0]
    print(f"[OK] Loaded {count:,} L5 records into l5_agg_seg{seg}")
    return True


def create_views(con: duckdb.DuckDBPyConnection, seg: int):
    """Create useful views for analysis."""
    
    # View 1: L1 with time-based features
    con.execute(f"""
        CREATE OR REPLACE VIEW v_l1_enriched_seg{seg} AS
        SELECT 
            *,
            EXTRACT(hour FROM timestamp) as hour,
            EXTRACT(minute FROM timestamp) as minute,
            EXTRACT(second FROM timestamp) as second,
            (best_bid + best_ask) / 2.0 as midprice,
            CASE 
                WHEN spread_abs > 0 THEN (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
                ELSE NULL 
            END as spread_bps
        FROM l1_agg_seg{seg}
    """)
    print(f"[OK] Created view v_l1_enriched_seg{seg}")
    
    # View 2: L5 with enhanced metrics
    con.execute(f"""
        CREATE OR REPLACE VIEW v_l5_enriched_seg{seg} AS
        SELECT 
            *,
            EXTRACT(hour FROM timestamp) as hour,
            EXTRACT(minute FROM timestamp) as minute,
            EXTRACT(second FROM timestamp) as second,
            (best_bid + best_ask) / 2.0 as midprice,
            total_bid_volume + total_ask_volume as total_volume,
            CASE 
                WHEN total_ask_volume > 0 
                THEN total_bid_volume / total_ask_volume 
                ELSE NULL 
            END as bid_ask_volume_ratio,
            CASE 
                WHEN spread_abs > 0 THEN (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
                ELSE NULL 
            END as spread_bps
        FROM l5_agg_seg{seg}
    """)
    print(f"[OK] Created view v_l5_enriched_seg{seg}")
    
    # View 3: L1 vs L5 comparison (if both exist)
    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW v_l1_vs_l5_seg{seg} AS
            SELECT 
                l1.security_id,
                l1.ts_s,
                l1.timestamp,
                l1.best_bid as l1_best_bid,
                l1.best_ask as l1_best_ask,
                l1.spread_abs as l1_spread,
                l1.microprice as l1_microprice,
                l5.microprice_l5 as l5_microprice,
                l5.total_bid_volume,
                l5.total_ask_volume,
                l5.imbalance_l5,
                l5.volume_ratio_l1_to_l5,
                l5.depth_ratio
            FROM l1_agg_seg{seg} l1
            INNER JOIN l5_agg_seg{seg} l5 
                ON l1.security_id = l5.security_id 
                AND l1.ts_s = l5.ts_s
        """)
        print(f"[OK] Created view v_l1_vs_l5_seg{seg}")
    except Exception as e:
        print(f"[WARN] Could not create L1 vs L5 view: {e}")


def create_summary_stats(con: duckdb.DuckDBPyConnection, seg: int):
    """Create summary statistics table."""
    
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE summary_stats_seg{seg} AS
            SELECT 
                'L1' as level,
                COUNT(*) as record_count,
                COUNT(DISTINCT security_id) as unique_securities,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp,
                AVG(spread_abs) as avg_spread,
                AVG(update_count) as avg_updates_per_second
            FROM l1_agg_seg{seg}
            
            UNION ALL
            
            SELECT 
                'L5' as level,
                COUNT(*) as record_count,
                COUNT(DISTINCT security_id) as unique_securities,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp,
                AVG(spread_abs) as avg_spread,
                AVG(update_count) as avg_updates_per_second
            FROM l5_agg_seg{seg}
        """)
        print(f"[OK] Created summary statistics table summary_stats_seg{seg}")
    except Exception as e:
        print(f"[WARN] Could not create summary stats: {e}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build DuckDB warehouse from L1/L5 data")
    ap.add_argument("--seg", type=int, required=True, help="Market segment ID")
    ap.add_argument("--data-dir", required=True, help="Path to data_samples/XX-XXXX directory")
    ap.add_argument("--db", required=True, help="Path to DuckDB database file")
    args = ap.parse_args()
    
    # Create directory for database if needed
    db_dir = os.path.dirname(args.db)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    
    print(f"[INFO] Building DuckDB warehouse: {args.db}")
    print(f"[INFO] Segment: {args.seg}")
    print(f"[INFO] Data directory: {args.data_dir}")
    
    # Connect to DuckDB
    con = duckdb.connect(args.db)
    
    try:
        # Create schema
        create_schema(con)
        
        # Load data
        l1_loaded = load_l1_data(con, args.seg, args.data_dir)
        l5_loaded = load_l5_data(con, args.seg, args.data_dir)
        
        if not l1_loaded and not l5_loaded:
            print("[ERROR] No data was loaded")
            return 1
        
        # Create views
        create_views(con, args.seg)
        
        # Create summary statistics
        create_summary_stats(con, args.seg)
        
        # Show what was created
        print("\n" + "="*60)
        print("DATABASE CONTENTS")
        print("="*60)
        
        tables = con.execute("""
            SELECT table_schema, table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'eurex'
            ORDER BY table_type, table_name
        """).fetchdf()
        
        print(tables.to_string(index=False))
        
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        
        try:
            stats = con.execute(f"SELECT * FROM summary_stats_seg{args.seg}").fetchdf()
            print(stats.to_string(index=False))
        except:
            pass
        
        print("\n[OK] DuckDB warehouse built successfully!")
        print(f"[OK] Database location: {args.db}")
        
    finally:
        con.close()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())