#!/usr/bin/env python3
"""
Aggregate L1 snapshots and DI actions to 1-second metrics per security_id.

Inputs:
- --l1: CSV (or Parquet) produced by parse_and_l1.py (ts_ns, best_bid, bid_size, best_ask, ask_size, action, security_id)
- --di: sliced DI CSV (window file), used to compute per-second update/cancel counts
- --mapping: DI mapping JSON for parsing DI entries

Outputs (to --out):
- l1_agg_1s_seg{seg}.parquet
- l1_agg_1s_seg{seg}.csv

Metrics:
- best_bid, best_ask (last value within the second)
- bid_size, ask_size (last value within the second)
- spread_abs = best_ask - best_bid
- spread_rel = spread_abs / ((best_ask + best_bid)/2)
- imbalance = (bid_size - ask_size) / (bid_size + ask_size)
- microprice = (best_ask*bid_size + best_bid*ask_size) / (bid_size + ask_size)
- update_count, cancel_count (from DI md_update_action; updates={0,1,5}, cancels={2})
"""
import argparse
import json
import os
from typing import Dict

import pandas as pd

# Local imports
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.eurex_liquidity.parser import (
    extract_entry_tokens_from_di_line,
    DiMapping,
    tokens_to_event,
)


def _load_mapping(path: str) -> DiMapping:
    with open(path, 'r', encoding='utf-8') as f:
        d = json.load(f)
    return DiMapping(
        md_update_action_idx=d['md_update_action_idx'],
        entry_type_idx=d['entry_type_idx'],
        price_level_idx=d['price_level_idx'],
        security_id_idx=d['security_id_idx'],
        price_idx=d['price_idx'],
        size_idx=d['size_idx'],
        ts_ns_idx=d['ts_ns_idx'],
    )


def _floor_sec(ts_ns: int) -> int:
    return int(ts_ns // 1_000_000_000)


def _read_l1(l1_path: str) -> pd.DataFrame:
    if l1_path.endswith('.parquet'):
        df = pd.read_parquet(l1_path)
    else:
        df = pd.read_csv(l1_path)
    # Ensure expected columns exist
    needed = {"ts_ns","best_bid","bid_size","best_ask","ask_size","security_id"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"L1 file missing columns: {missing}")
    return df


def _aggregate_l1_per_second(df: pd.DataFrame) -> pd.DataFrame:
    # Compute second key
    df = df.copy()
    df['ts_s'] = df['ts_ns'].astype('int64') // 1_000_000_000
    # Sort and take last snapshot in each second per security
    df = df.sort_values(['security_id','ts_ns'])
    last_per_sec = df.groupby(['security_id','ts_s'], as_index=False).tail(1)

    # Derive metrics
    out = last_per_sec.copy()
    out['spread_abs'] = out['best_ask'] - out['best_bid']
    mid = (out['best_ask'] + out['best_bid']) / 2.0
    out['spread_rel'] = out['spread_abs'] / mid.replace({0: pd.NA})
    denom = (out['bid_size'].fillna(0) + out['ask_size'].fillna(0))
    out['imbalance'] = (out['bid_size'].fillna(0) - out['ask_size'].fillna(0)) / denom.replace({0: pd.NA})
    out['microprice'] = (out['best_ask']*out['bid_size'].fillna(0) + out['best_bid']*out['ask_size'].fillna(0)) / denom.replace({0: pd.NA})

    # Keep selected columns
    cols = ['security_id','ts_s','best_bid','best_ask','bid_size','ask_size','spread_abs','spread_rel','imbalance','microprice']
    return out[cols].reset_index(drop=True)


def _count_di_actions_per_second(di_path: str, mapping: DiMapping) -> pd.DataFrame:
    rows = []
    with open(di_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            entries = extract_entry_tokens_from_di_line(line)
            for e in entries:
                evt = tokens_to_event(e, mapping)
                ts_ns = evt.get('ts_ns')
                sec = evt.get('security_id')
                act = evt.get('md_update_action')
                if ts_ns is None or sec is None or act is None:
                    continue
                ts_s = _floor_sec(int(ts_ns))
                rows.append({'security_id': int(sec), 'ts_s': ts_s, 'act': int(act)})
    if not rows:
        return pd.DataFrame(columns=['security_id','ts_s','update_count','cancel_count'])
    df = pd.DataFrame(rows)
    # Map actions to categories
    df['update'] = df['act'].isin({0,1,5}).astype('int64')
    df['cancel'] = (df['act'] == 2).astype('int64')
    agg = df.groupby(['security_id','ts_s'], as_index=False).agg(update_count=('update','sum'), cancel_count=('cancel','sum'))
    return agg


def main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate 1s L1 metrics and DI action counts")
    ap.add_argument("--seg", type=int, required=True, help="MarketSegmentID (e.g., 48)")
    ap.add_argument("--l1", required=True, help="Path to L1 snapshots CSV or Parquet")
    ap.add_argument("--di", required=True, help="Path to sliced DI CSV")
    ap.add_argument("--mapping", required=True, help="Path to DI mapping JSON")
    ap.add_argument("--out", required=True, help="Output directory")
    args = ap.parse_args()

    l1 = _read_l1(args.l1)
    l1_1s = _aggregate_l1_per_second(l1)

    mapping = _load_mapping(args.mapping)
    di_counts = _count_di_actions_per_second(args.di, mapping)

    df = l1_1s.merge(di_counts, on=['security_id','ts_s'], how='left')
    df['update_count'] = df['update_count'].fillna(0).astype('int64')
    df['cancel_count'] = df['cancel_count'].fillna(0).astype('int64')

    # Write outputs with L1 subdirectory structure
    l1_out_dir = os.path.join(args.out, "l1")
    os.makedirs(l1_out_dir, exist_ok=True)
    
    parq = os.path.join(l1_out_dir, f"l1_agg_1s_seg{args.seg}.parquet")
    csv = os.path.join(l1_out_dir, f"l1_agg_1s_seg{args.seg}.csv")

    try:
        df.to_parquet(parq, index=False)
        print("[OK] Wrote:", parq, "rows=", len(df))
    except Exception as e:
        print("[WARN] Parquet write failed:", e)

    df.to_csv(csv, index=False)
    print("[OK] Wrote:", csv, "rows=", len(df))

    print("\n[Preview] First 6 rows:")
    print(df.head(6))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
