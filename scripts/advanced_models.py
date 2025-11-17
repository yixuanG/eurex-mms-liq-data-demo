#!/usr/bin/env python3
"""
Advanced Liquidity Models for Power BI Dashboard
Implements Kyle's Lambda, Amihud, Spread Decomposition, and other metrics
"""

import pandas as pd
import numpy as np
import duckdb
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')


def estimate_kyle_lambda(df, group_col='segment_id'):
    """
    Estimate Kyle's lambda: ΔP = λ × Q + ε
    Price change per unit of signed order flow
    """
    results = []
    
    for group_val in df[group_col].unique():
        group_data = df[df[group_col] == group_val].copy()
        group_data = group_data.sort_values('ts_s')
        
        group_data['price_change'] = group_data.groupby('security_id')['midprice'].diff()
        group_data['signed_flow'] = group_data['imbalance_l5']
        
        valid_data = group_data[['price_change', 'signed_flow']].replace(
            [np.inf, -np.inf], np.nan
        ).dropna()
        
        if len(valid_data) > 50:
            X = valid_data[['signed_flow']]
            y = valid_data['price_change']
            
            model = LinearRegression()
            model.fit(X, y)
            
            lambda_estimate = model.coef_[0]
            r_squared = model.score(X, y)
            
            residuals = y - model.predict(X)
            mse = np.mean(residuals ** 2)
            se_lambda = np.sqrt(mse / np.sum((X['signed_flow'] - X['signed_flow'].mean()) ** 2))
            
            results.append({
                group_col: group_val,
                'kyle_lambda': lambda_estimate,
                'lambda_abs': abs(lambda_estimate),
                'r_squared': r_squared,
                'std_error': se_lambda,
                'n_observations': len(valid_data),
                'liquidity_score': 1 / abs(lambda_estimate) if lambda_estimate != 0 else np.inf
            })
    
    return pd.DataFrame(results)


def calculate_amihud_illiquidity(df, freq='1H'):
    """
    Calculate Amihud (2002) illiquidity measure
    ILLIQ = |Return| / Volume
    """
    results = []
    
    for (segment, security), group in df.groupby(['segment_id', 'security_id']):
        group = group.sort_values('ts_s').copy()
        group['datetime'] = pd.to_datetime(group['ts_s'], unit='s')
        
        resampled = group.set_index('datetime').resample(freq).agg({
            'midprice': ['first', 'last'],
            'total_bid_volume': 'mean',
            'total_ask_volume': 'mean'
        })
        
        resampled['return'] = np.log(resampled[('midprice', 'last')] / resampled[('midprice', 'first')])
        resampled['abs_return'] = abs(resampled['return'])
        resampled['volume'] = resampled[('total_bid_volume', 'mean')] + resampled[('total_ask_volume', 'mean')]
        
        resampled['amihud_illiq'] = resampled['abs_return'] / resampled['volume']
        resampled['amihud_illiq'] = resampled['amihud_illiq'].replace([np.inf, -np.inf], np.nan)
        
        results.append({
            'segment_id': segment,
            'security_id': security,
            'avg_amihud': resampled['amihud_illiq'].mean(),
            'median_amihud': resampled['amihud_illiq'].median(),
            'std_amihud': resampled['amihud_illiq'].std(),
            'n_periods': resampled['amihud_illiq'].notna().sum()
        })
    
    return pd.DataFrame(results)


def decompose_spread(df, horizon_periods=5):
    """
    Decompose spread: Effective = Price Impact + Realized Spread
    """
    results = []
    
    for (segment, security), group in df.groupby(['segment_id', 'security_id']):
        group = group.sort_values('ts_s').copy()
        
        if len(group) < horizon_periods + 10:
            continue
        
        group['midpoint_future'] = group['midprice'].shift(-horizon_periods)
        group['effective_spread_bps'] = group['spread_rel'] * 10000
        group['direction'] = np.sign(group['imbalance_l5'])
        group['price_impact_bps'] = (
            2 * (group['midpoint_future'] - group['midprice']) * 
            group['direction'] / group['midprice'] * 10000
        )
        group['realized_spread_bps'] = group['effective_spread_bps'] - group['price_impact_bps']
        
        valid = group[['effective_spread_bps', 'price_impact_bps', 'realized_spread_bps']].replace(
            [np.inf, -np.inf], np.nan
        ).dropna()
        
        if len(valid) > 10:
            avg_effective = valid['effective_spread_bps'].mean()
            avg_price_impact = valid['price_impact_bps'].mean()
            avg_realized = valid['realized_spread_bps'].mean()
            
            results.append({
                'segment_id': segment,
                'security_id': security,
                'effective_spread_bps': avg_effective,
                'price_impact_bps': abs(avg_price_impact),
                'realized_spread_bps': abs(avg_realized),
                'adverse_selection_pct': abs(avg_price_impact) / avg_effective * 100 if avg_effective != 0 else 0,
                'transient_pct': abs(avg_realized) / avg_effective * 100 if avg_effective != 0 else 0
            })
    
    return pd.DataFrame(results)


def main(db_path):
    """Run all advanced models and save to DuckDB"""
    con = duckdb.connect(db_path, read_only=False)
    
    # Load base data
    print("Loading base metrics...")
    metrics_1s = con.execute("""
        SELECT * FROM metrics_1s ORDER BY segment_id, security_id, ts_s
    """).df()
    
    print(f"Loaded {len(metrics_1s):,} rows")
    
    # Calculate Kyle's Lambda
    print("\nCalculating Kyle's Lambda...")
    kyle_segment = estimate_kyle_lambda(metrics_1s, 'segment_id')
    kyle_security = estimate_kyle_lambda(metrics_1s, 'security_id')
    
    # Calculate Amihud
    print("Calculating Amihud Illiquidity...")
    amihud = calculate_amihud_illiquidity(metrics_1s)
    
    # Spread Decomposition
    print("Calculating Spread Decomposition...")
    spread_decomp = decompose_spread(metrics_1s)
    
    # Save to DuckDB
    print("\nSaving results to DuckDB...")
    
    con.execute("DROP TABLE IF EXISTS kyle_lambda_segment")
    con.execute("CREATE TABLE kyle_lambda_segment AS SELECT * FROM kyle_segment")
    
    con.execute("DROP TABLE IF EXISTS kyle_lambda_security")
    con.execute("CREATE TABLE kyle_lambda_security AS SELECT * FROM kyle_security")
    
    con.execute("DROP TABLE IF EXISTS amihud_illiquidity")
    con.execute("CREATE TABLE amihud_illiquidity AS SELECT * FROM amihud")
    
    con.execute("DROP TABLE IF EXISTS spread_decomposition")
    con.execute("CREATE TABLE spread_decomposition AS SELECT * FROM spread_decomp")
    
    con.close()
    print("Complete!")
    
    return {
        'kyle_segment': kyle_segment,
        'kyle_security': kyle_security,
        'amihud': amihud,
        'spread_decomp': spread_decomp
    }


if __name__ == "__main__":
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/warehouse/eurex.duckdb"
    main(db_path)
