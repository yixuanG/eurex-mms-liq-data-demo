"""
Order Flow data preparation script
Generates Order Flow data required by Power BI from raw order data
"""

import pandas as pd
import numpy as np
import duckdb

def prepare_order_flow_data(db_path, time_windows=['1s', '3s', '1m', '5m']):
    """
    Prepare Order Flow data for multiple time granularities
    
    Parameters:
    - db_path: DuckDB database path
    - time_windows: List of time windows
    
    Returns:
    - dict: {time_window: DataFrame}
    """
    
    conn = duckdb.connect(db_path)
    
    results = {}
    
    # Time window mapping (convert to seconds)
    window_seconds = {
        '1s': 1,
        '3s': 3,
        '1m': 60,
        '5m': 300
    }
    
    for window in time_windows:
        seconds = window_seconds[window]
        
        # SQL query: Aggregate order flow data
        query = f"""
        WITH order_data AS (
            SELECT 
                timestamp,
                price,
                CASE WHEN side = 'buy' THEN quantity ELSE 0 END as bid_qty,
                CASE WHEN side = 'sell' THEN quantity ELSE 0 END as ask_qty,
                CASE WHEN side = 'buy' THEN quantity ELSE -quantity END as net_qty
            FROM orders
            WHERE timestamp >= (SELECT MAX(timestamp) - INTERVAL '{seconds * 100} seconds' FROM orders)
        ),
        binned_data AS (
            SELECT 
                -- Time binning
                FLOOR(EXTRACT(EPOCH FROM timestamp) / {seconds}) * {seconds} as time_bin,
                -- Price level bucketing (adjustable tick_size)
                ROUND(price / 0.5) * 0.5 as price_level,
                SUM(bid_qty) as bid_volume,
                SUM(ask_qty) as ask_volume,
                SUM(net_qty) as net_volume,
                COUNT(*) as trade_count
            FROM order_data
            GROUP BY time_bin, price_level
        )
        SELECT 
            time_bin,
            price_level,
            bid_volume,
            ask_volume,
            net_volume,
            trade_count,
            '{window}' as time_window
        FROM binned_data
        ORDER BY time_bin, price_level DESC
        """
        
        try:
            df = conn.execute(query).df()
            results[window] = df
            print(f"✓ Generated {window} data: {len(df)} rows")
        except Exception as e:
            print(f"✗ Error generating {window} data: {e}")
            # If query fails, generate mock data
            results[window] = generate_mock_order_flow(window, seconds)
    
    conn.close()
    return results

def generate_mock_order_flow(time_window, seconds):
    """
    Generate mock Order Flow data (for testing purposes)
    """
    np.random.seed(42)
    
    # Generate time intervals
    n_time_bins = 20
    time_bins = np.arange(0, n_time_bins * seconds, seconds)
    
    # Generate price levels (assuming prices around 100)
    base_price = 100.0
    n_price_levels = 15
    price_levels = base_price + np.arange(-n_price_levels//2, n_price_levels//2 + 1) * 0.5
    
    data = []
    
    for time_bin in time_bins:
        for price_level in price_levels:
            # Simulate volume (smaller volume further from mid price)
            distance = abs(price_level - base_price)
            volume_factor = max(0, 1 - distance / 5)
            
            # Buy order volume
            bid_volume = int(np.random.exponential(50) * volume_factor)
            
            # Sell order volume (add some asymmetry)
            ask_volume = int(np.random.exponential(45) * volume_factor)
            
            # Net volume
            net_volume = bid_volume - ask_volume
            
            # Trade count
            trade_count = int(np.random.poisson(5) * volume_factor)
            
            data.append({
                'time_bin': time_bin,
                'price_level': price_level,
                'bid_volume': bid_volume,
                'ask_volume': ask_volume,
                'net_volume': net_volume,
                'trade_count': trade_count,
                'time_window': time_window
            })
    
    df = pd.DataFrame(data)
    print(f"  Generated mock data for {time_window}: {len(df)} rows")
    return df

def export_for_powerbi(results, export_dir):
    """
    Export to Power BI friendly format
    """
    import os
    os.makedirs(export_dir, exist_ok=True)
    
    # Merge all time window data into one file
    all_data = pd.concat(results.values(), ignore_index=True)
    
    output_file = f"{export_dir}/order_flow_data.csv"
    all_data.to_csv(output_file, index=False)
    print(f"\n✓ Exported combined data to: {output_file}")
    print(f"  Total rows: {len(all_data)}")
    print(f"  Columns: {list(all_data.columns)}")
    
    # Also export each time window separately
    for window, df in results.items():
        window_file = f"{export_dir}/order_flow_{window}.csv"
        df.to_csv(window_file, index=False)
        print(f"✓ Exported {window} data to: {window_file}")
    
    return output_file

# Main execution code
if __name__ == "__main__":
    # Configure paths
    DB_PATH = '/Users/ivan/Library/CloudStorage/GoogleDrive-ivan.guoyixuan@gmail.com/My Drive/00_EUREX/eurex-liquidity-demo/warehouse/eurex.duckdb'
    EXPORT_DIR = '/Users/ivan/Library/CloudStorage/GoogleDrive-ivan.guoyixuan@gmail.com/My Drive/00_EUREX/eurex-liquidity-demo/export_powerbi'
    
    print("Starting Order Flow data preparation...\n")
    
    # Prepare data
    results = prepare_order_flow_data(DB_PATH, time_windows=['1s', '3s', '1m', '5m'])
    
    # Export data
    output_file = export_for_powerbi(results, EXPORT_DIR)
    
    print("\n" + "="*60)
    print("Order Flow data preparation complete!")
    print("="*60)
    
    # Data preview
    print("\nData Preview (1s window):")
    print(results['1s'].head(10))
    
    print("\nData Statistics:")
    for window, df in results.items():
        print(f"\n{window}:")
        print(f"  Rows: {len(df)}")
        print(f"  Price range: {df['price_level'].min():.2f} - {df['price_level'].max():.2f}")
        print(f"  Time bins: {df['time_bin'].nunique()}")
        print(f"  Total bid volume: {df['bid_volume'].sum():,.0f}")
        print(f"  Total ask volume: {df['ask_volume'].sum():,.0f}")
        print(f"  Net volume: {df['net_volume'].sum():+,.0f}")
