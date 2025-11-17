import json
import re
from typing import Any, Dict, List, Optional, Tuple
import datetime as dt

import duckdb
import pandas as pd
import requests


def fetch_sdmx_json(url: str, timeout: int = 20) -> Dict[str, Any]:
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _obs_time_from_index(obs_dims: List[Dict[str, Any]], idx_parts: List[int]) -> str:
    time_idx = 0
    for i, d in enumerate(obs_dims):
        if d.get("id", "").upper() in ("TIME", "TIME_PERIOD"):
            time_idx = i
            break
    values = obs_dims[time_idx].get("values", [])
    j = idx_parts[time_idx] if time_idx < len(idx_parts) else 0
    j = min(j, len(values) - 1) if values else 0
    return values[j].get("id") if values else None


def sdmx_json_to_df(data: Dict[str, Any]) -> pd.DataFrame:
    datasets = data.get("dataSets", [])
    if not datasets:
        return pd.DataFrame()

    series_dict = datasets[0].get("series", {})
    struct = data.get("structure", {})
    dims_series = struct.get("dimensions", {}).get("series", [])
    dims_obs = struct.get("dimensions", {}).get("observation", [])

    rows: List[Dict[str, Any]] = []

    for s_key, s_payload in series_dict.items():
        s_idx = [int(x) for x in s_key.split(":")] if s_key else []
        s_vals: Dict[str, Any] = {}
        for i, dim in enumerate(dims_series):
            vals = dim.get("values", [])
            j = s_idx[i] if i < len(s_idx) else 0
            j = min(j, len(vals) - 1) if vals else 0
            s_vals[dim.get("id", f"dim_{i}")] = vals[j].get("id") if vals else None

        obs = s_payload.get("observations", {})
        for o_key, o_val in obs.items():
            o_idx = [int(x) for x in o_key.split(":")] if o_key else [0]
            t = _obs_time_from_index(dims_obs, o_idx)
            if t is None:
                continue
            val = None
            if isinstance(o_val, list) and o_val:
                val = o_val[0]
            elif isinstance(o_val, (int, float)):
                val = o_val
            row = {"TIME_PERIOD": t, "value": val}
            row.update(s_vals)
            rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["TIME_PERIOD"] = pd.to_datetime(df["TIME_PERIOD"], errors="coerce")
        df = df.sort_values("TIME_PERIOD").reset_index(drop=True)
    return df


def estr_from_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df[["TIME_PERIOD", "value"]].copy()
    out = out.rename(columns={"TIME_PERIOD": "date", "value": "rate"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out = out.dropna(subset=["rate"]).drop_duplicates(["date"]).reset_index(drop=True)
    return out


def _parse_tenor_to_years(s: str) -> Optional[float]:
    if not isinstance(s, str) or not s:
        return None
    m = re.search(r"([0-9]+\.?[0-9]*)\s*([YyMmDdWw])", s)
    if not m:
        return None
    num = float(m.group(1))
    unit = m.group(2).upper()
    if unit == "Y":
        return num
    if unit == "M":
        return num / 12.0
    if unit == "W":
        return num / 52.0
    if unit == "D":
        return num / 365.0
    return None


def yield_curve_from_df(df: pd.DataFrame, maturity_col: Optional[str] = None) -> pd.DataFrame:
    candidates = []
    if maturity_col is None:
        for c in df.columns:
            cu = str(c).upper()
            if cu in ("MATURITY", "TENOR", "TERM") or "MATUR" in cu or "TENOR" in cu or cu.endswith("_MAT"):
                candidates.append(c)
        if candidates:
            maturity_col = candidates[0]
    if maturity_col is None:
        series_dim_cols = [c for c in df.columns if c not in ("TIME_PERIOD", "value")]
        maturity_col = series_dim_cols[0] if series_dim_cols else None

    out_cols = ["TIME_PERIOD", "value"]
    if maturity_col and maturity_col not in out_cols:
        out_cols.append(maturity_col)
    out = df[out_cols].copy()
    out = out.rename(columns={"TIME_PERIOD": "date", "value": "rate", maturity_col: "maturity" if maturity_col else "maturity"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    if "maturity" not in out.columns:
        out["maturity"] = None
    out["maturity_years"] = out["maturity"].map(_parse_tenor_to_years)
    out = out.dropna(subset=["rate"]).reset_index(drop=True)
    return out


def write_df_to_duckdb(df: pd.DataFrame, db_path: str, table: str, replace: bool = True) -> None:
    con = duckdb.connect(db_path)
    con.sql("CREATE SCHEMA IF NOT EXISTS eurex;")
    con.register("_tmp_df", df)
    if replace:
        con.sql(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _tmp_df;")
    else:
        con.sql(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM _tmp_df WHERE 0=1;")
        con.sql(f"INSERT INTO {table} SELECT * FROM _tmp_df;")
    con.close()
