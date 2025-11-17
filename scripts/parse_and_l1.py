#!/usr/bin/env python3
"""
Parse sliced DI sample using an inferred mapping and build L1 snapshots.

This step converts nested DI lines into structured events, reconstructs L1
(best bid/ask) per security_id, and writes event-driven snapshots.

Outputs (to --out):
- l1_snapshots_seg{seg}.parquet
- l1_snapshots_seg{seg}.csv (for quick preview)

Usage (Colab):
  python scripts/parse_and_l1.py \
    --seg 48 \
    --di "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_samples/DI_48_20201201_window.csv" \
    --mapping "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_samples/di_mapping_seg48.json" \
    --out "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_samples"
"""
import argparse
import json
import os
from typing import Dict, List

import pandas as pd

# Local imports
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.eurex_liquidity.parser import (
    extract_entry_tokens_from_di_line,
    DiMapping,
    tokens_to_event,
)
from src.eurex_liquidity.orderbook import L1Book


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


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse DI and build L1 snapshots")
    ap.add_argument("--seg", type=int, required=True, help="MarketSegmentID (e.g., 48)")
    ap.add_argument("--di", required=True, help="Path to sliced DI CSV")
    ap.add_argument("--mapping", required=True, help="Path to DI mapping JSON")
    ap.add_argument("--out", required=True, help="Output directory")
    args = ap.parse_args()

    mapping = _load_mapping(args.mapping)

    # Build L1 books per security_id
    books: Dict[int, L1Book] = {}
    rows: List[Dict[str, object]] = []

    with open(args.di, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            entries = extract_entry_tokens_from_di_line(line)
            for e in entries:
                evt = tokens_to_event(e, mapping)
                ts_ns = evt.get('ts_ns')
                sec = evt.get('security_id')
                if ts_ns is None or sec is None:
                    continue
                sec = int(sec)
                if sec not in books:
                    books[sec] = L1Book()
                changed = books[sec].apply_event(evt)
                if changed:
                    snap = books[sec].snapshot(action=evt.get('md_update_action'))
                    snap['security_id'] = sec
                    rows.append(snap)

    if not rows:
        print("[WARN] No L1 changes detected; nothing to write.")
        return 0

    df = pd.DataFrame(rows).sort_values(['ts_ns', 'security_id']).reset_index(drop=True)

    # Write outputs with L1 subdirectory structure
    l1_out_dir = os.path.join(args.out, "l1")
    os.makedirs(l1_out_dir, exist_ok=True)
    
    parq = os.path.join(l1_out_dir, f"l1_snapshots_seg{args.seg}.parquet")
    csv = os.path.join(l1_out_dir, f"l1_snapshots_seg{args.seg}.csv")

    try:
        df.to_parquet(parq, index=False)
        print("[OK] Wrote:", parq, "rows=", len(df))
    except Exception as e:
        print("[WARN] Parquet write failed:", e)

    df.to_csv(csv, index=False)
    print("[OK] Wrote:", csv, "rows=", len(df))

    # Print a small preview
    print("\n[Preview] First 6 rows:")
    print(df.head(6))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
