#!/usr/bin/env python3
"""
Inspect the sliced DI sample and infer a minimal mapping for key fields needed
for L1 order book reconstruction. Writes a JSON mapping file to data_samples/.

Usage (Colab):
  python scripts/inspect_schema.py \
    --di "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_samples/DI_48_20201201_window.csv" \
    --out "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_samples/di_mapping_seg48.json" \
    --sample-limit 200

Notes:
- Keeps assumptions minimal and data-driven; see src/eurex_liquidity/parser.py
- Prints a short preview of parsed events to visually validate the mapping.
"""
import argparse
import json
import os
from typing import List

# Local import
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.eurex_liquidity.parser import (
    extract_entry_tokens_from_di_line,
    infer_di_mapping,
    tokens_to_event,
)


def _read_lines(path: str, limit: int) -> List[str]:
    lines: List[str] = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            lines.append(line.rstrip('\n'))
            if limit and i + 1 >= limit:
                break
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description="Infer DI schema mapping and write JSON")
    ap.add_argument("--di", required=True, help="Path to sliced DI CSV (window file)")
    ap.add_argument("--out", required=True, help="Output JSON path for DI mapping")
    ap.add_argument("--sample-limit", type=int, default=200, help="Lines to sample for inference")
    args = ap.parse_args()

    lines = _read_lines(args.di, args.sample_limit)
    if not lines:
        print("[ERROR] Empty DI file or unreadable:", args.di)
        return 1

    mapping = infer_di_mapping(lines, sample_limit=args.sample_limit)
    if mapping is None:
        print("[ERROR] Could not infer mapping from DI")
        return 2

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    payload = {
        "md_update_action_idx": mapping.md_update_action_idx,
        "entry_type_idx": mapping.entry_type_idx,
        "price_level_idx": mapping.price_level_idx,
        "security_id_idx": mapping.security_id_idx,
        "price_idx": mapping.price_idx,
        "size_idx": mapping.size_idx,
        "ts_ns_idx": mapping.ts_ns_idx,
    }
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)
    print("[OK] Wrote DI mapping JSON to:", args.out)
    print(json.dumps(payload, indent=2))

    # Preview a few parsed events for sanity check
    print("\n[Preview] First 6 parsed entries:")
    shown = 0
    for line in lines:
        entries = extract_entry_tokens_from_di_line(line)
        for e in entries:
            evt = tokens_to_event(e, mapping)
            print(evt)
            shown += 1
            if shown >= 6:
                break
        if shown >= 6:
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
