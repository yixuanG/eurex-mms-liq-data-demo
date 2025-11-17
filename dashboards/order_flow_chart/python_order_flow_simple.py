# Power BI Python Visual - Simple Order Flow (for debugging)
# Simplified version with detailed error messages

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime

# Debug information
print("=" * 60)
print("ORDER FLOW CHART - DEBUGGING MODE")
print("=" * 60)

# Power BI execution section
if 'dataset' in locals():
    print("\n✅ Dataset found!")
    print(f"   Rows: {len(dataset)}")
    print(f"   Columns: {list(dataset.columns)}")
    
    df = dataset.copy()
    
    # Display first few rows
    print("\n📊 First 5 rows:")
    print(df.head())
    
    # Check required columns
    required_cols = ['time_bin', 'price_level', 'bid_volume', 'ask_volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"\n❌ ERROR: Missing columns: {missing_cols}")
        print(f"   Available columns: {list(df.columns)}")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f'Missing columns: {missing_cols}', 
                ha='center', va='center', fontsize=14, color='red')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        plt.show()
    else:
        print("\n✅ All required columns present")
        
        # Data type conversion
        try:
            df['time_bin'] = pd.to_numeric(df['time_bin'], errors='coerce')
            df['price_level'] = pd.to_numeric(df['price_level'], errors='coerce')
            df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
            df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
            
            # Remove NaN values
            df = df.dropna(subset=['time_bin', 'price_level', 'bid_volume', 'ask_volume'])
            
            print(f"\n✅ Data converted. Rows after cleaning: {len(df)}")
            
            if len(df) == 0:
                raise ValueError("No valid data after cleaning")
            
            # Calculate net_volume
            if 'net_volume' not in df.columns:
                df['net_volume'] = df['bid_volume'] - df['ask_volume']
            else:
                df['net_volume'] = pd.to_numeric(df['net_volume'], errors='coerce')
            
            # Convert time
            df['time_str'] = pd.to_datetime(df['time_bin'], unit='s').dt.strftime('%H:%M')
            
            print(f"\n📊 Data summary:")
            print(f"   Time range: {df['time_str'].min()} - {df['time_str'].max()}")
            print(f"   Price range: {df['price_level'].min():.2f} - {df['price_level'].max():.2f}")
            print(f"   Unique times: {df['time_bin'].nunique()}")
            print(f"   Unique prices: {df['price_level'].nunique()}")
            print(f"   Total bid volume: {df['bid_volume'].sum():,.0f}")
            print(f"   Total ask volume: {df['ask_volume'].sum():,.0f}")
            
            # Limit data volume
            max_times = 20
            max_prices = 20
            
            # Get most recent times
            recent_times = sorted(df['time_bin'].unique())[-max_times:]
            df_filtered = df[df['time_bin'].isin(recent_times)]
            
            # Get most active prices
            price_activity = df_filtered.groupby('price_level')[['bid_volume', 'ask_volume']].sum().sum(axis=1)
            top_prices = price_activity.nlargest(max_prices).index
            df_filtered = df_filtered[df_filtered['price_level'].isin(top_prices)]
            
            print(f"\n📊 After filtering:")
            print(f"   Rows: {len(df_filtered)}")
            print(f"   Time bins: {df_filtered['time_bin'].nunique()}")
            print(f"   Price levels: {df_filtered['price_level'].nunique()}")
            
            # Create pivot tables
            print("\n🔄 Creating pivot tables...")
            
            pivot_bid = df_filtered.pivot_table(
                values='bid_volume',
                index='price_level',
                columns='time_bin',
                fill_value=0
            ).sort_index(ascending=False)
            
            pivot_ask = df_filtered.pivot_table(
                values='ask_volume',
                index='price_level',
                columns='time_bin',
                fill_value=0
            ).sort_index(ascending=False)
            
            pivot_net = df_filtered.pivot_table(
                values='net_volume',
                index='price_level',
                columns='time_bin',
                fill_value=0
            ).sort_index(ascending=False)
            
            print(f"   Pivot shape: {pivot_bid.shape}")
            print(f"   Rows (prices): {len(pivot_bid.index)}")
            print(f"   Cols (times): {len(pivot_bid.columns)}")
            
            if len(pivot_bid) == 0 or len(pivot_bid.columns) == 0:
                raise ValueError("Pivot table is empty!")
            
            # Create chart
            print("\n🎨 Creating chart...")
            
            n_rows = len(pivot_bid.index)
            n_cols = len(pivot_bid.columns)
            
            fig, ax = plt.subplots(figsize=(max(12, n_cols * 0.7), max(10, n_rows * 0.5)))
            
            # Get time labels
            time_labels = df_filtered.groupby('time_bin')['time_str'].first().reindex(pivot_bid.columns).values
            
            # Simple heatmap
            im = ax.imshow(pivot_net.values, cmap='RdYlGn', aspect='auto')
            
            # Add text labels
            for i in range(min(n_rows, 30)):  # Limit number of labels
                for j in range(min(n_cols, 30)):
                    if i < len(pivot_bid.index) and j < len(pivot_bid.columns):
                        bid_vol = int(pivot_bid.iloc[i, j])
                        ask_vol = int(pivot_ask.iloc[i, j])
                        
                        if bid_vol > 0 or ask_vol > 0:
                            text = f'{bid_vol}×{ask_vol}' if bid_vol > 0 and ask_vol > 0 else str(bid_vol if bid_vol > 0 else ask_vol)
                            ax.text(j, i, text, ha='center', va='center', 
                                   fontsize=7, color='black', fontweight='bold')
            
            # Axes
            ax.set_xticks(range(min(n_cols, 30)))
            ax.set_xticklabels(time_labels[:min(n_cols, 30)], rotation=45, ha='right', fontsize=8)
            
            ax.set_yticks(range(min(n_rows, 30)))
            ax.set_yticklabels([f'{p:.2f}' for p in pivot_bid.index[:min(n_rows, 30)]], fontsize=9)
            
            ax.set_title('Order Flow - Simple View (Bid × Ask)', fontsize=14, fontweight='bold')
            ax.set_xlabel('Time', fontsize=11)
            ax.set_ylabel('Price', fontsize=11)
            
            plt.colorbar(im, ax=ax, label='Net Volume')
            plt.tight_layout()
            
            print("\n✅ Chart created successfully!")
            print("=" * 60)
            
            plt.show()
            
        except Exception as e:
            print(f"\n❌ ERROR during processing:")
            print(f"   {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f'Error: {str(e)}', 
                    ha='center', va='center', fontsize=12, color='red',
                    wrap=True)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            plt.show()

else:
    print("\n❌ No dataset found!")
    print("   Make sure you've dragged the required fields to the Values area:")
    print("   - time_bin")
    print("   - price_level")
    print("   - bid_volume")
    print("   - ask_volume")
