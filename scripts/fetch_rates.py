#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Optional

import pandas as pd

# Local imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.eurex_liquidity.rates import (
    fetch_sdmx_json,
    sdmx_json_to_df,
    estr_from_df,
    yield_curve_from_df,
    write_df_to_duckdb,
)


def _info(msg: str):
    print(f"[INFO] {msg}")


def _warn(msg: str):
    print(f"[WARN] {msg}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch rates from ECB SDW SDMX-JSON URLs and load into DuckDB"
    )
    parser.add_argument(
        "--db",
        default=os.path.join(os.path.dirname(__file__), "..", "warehouse", "eurex.duckdb"),
        help="Path to DuckDB database file (default: warehouse/eurex.duckdb)",
    )
    parser.add_argument(
        "--estr-url",
        default=os.environ.get("ESTR_URL", ""),
        help="ECB SDW SDMX-JSON URL for €STR series (daily)",
    )
    parser.add_argument(
        "--yc-url",
        default=os.environ.get("YC_URL", ""),
        help="ECB SDW SDMX-JSON URL for zero-coupon yield curve series",
    )
    parser.add_argument(
        "--yc-maturity-col",
        default="",
        help="Column name in SDMX frame that holds maturity/tenor (if auto-detect fails)",
    )
    parser.add_argument(
        "--table-estr",
        default="eurex.dim_estr",
        help="DuckDB target table for €STR (default: eurex.dim_estr)",
    )
    parser.add_argument(
        "--table-yc",
        default="eurex.dim_yield_curve",
        help="DuckDB target table for yield curve (default: eurex.dim_yield_curve)",
    )

    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _info(f"DuckDB path: {db_path}")

    if args.estr_url:
        _info("Fetching €STR...")
        data = fetch_sdmx_json(args.estr_url)
        df_raw = sdmx_json_to_df(data)
        if df_raw.empty:
            _warn("€STR SDMX frame is empty")
        else:
            df_estr = estr_from_df(df_raw)
            _info(f"€STR rows: {len(df_estr)} | sample:\n{df_estr.head()}\n")
            write_df_to_duckdb(df_estr, db_path, args.table_estr, replace=True)
            _info(f"Wrote {len(df_estr)} rows to {args.table_estr}")
    else:
        _warn("--estr-url not provided; skipping €STR")

    if args.yc_url:
        _info("Fetching Yield Curve...")
        data = fetch_sdmx_json(args.yc_url)
        df_raw = sdmx_json_to_df(data)
        if df_raw.empty:
            _warn("Yield curve SDMX frame is empty")
        else:
            mat_col = args.yc_maturity_col if args.yc_maturity_col else None
            df_yc = yield_curve_from_df(df_raw, maturity_col=mat_col)
            _info(f"YC rows: {len(df_yc)} | sample:\n{df_yc.head()}\n")
            write_df_to_duckdb(df_yc, db_path, args.table_yc, replace=True)
            _info(f"Wrote {len(df_yc)} rows to {args.table_yc}")
    else:
        _warn("--yc-url not provided; skipping Yield Curve")

    _info("Done.")


if __name__ == "__main__":
    main()
