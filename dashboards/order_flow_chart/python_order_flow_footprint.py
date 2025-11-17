# Power BI Python Visual - Order Flow Footprint Chart
# Similar to Sierra Chart / BookMap footprint style
# Bid and Ask in the same cell, distributed left and right

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
import numpy as np
from datetime import datetime

def create_footprint_chart(data, time_window='5m', max_time_bins=30, max_price_levels=30):
    """
    Create Footprint style Order Flow chart
    
    Parameters:
    - data: DataFrame with columns [time_bin, price_level, bid_volume, ask_volume, net_volume]
    - time_window: Time window identifier
    - max_time_bins: Maximum number of time windows to display (width limit)
    - max_price_levels: Maximum number of price levels to display (height limit)
    """
    
    # 0. Data validation
    if data is None or len(data) == 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=16)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        return fig
    
    # 1. Data preprocessing - convert time
    data = data.copy()
    data['time_str'] = pd.to_datetime(data['time_bin'], unit='s').dt.strftime('%H:%M:%S')
    
    # 2. Limit data volume (take most recent time windows and price range)
    unique_times = sorted(data['time_bin'].unique())[-max_time_bins:]
    data = data[data['time_bin'].isin(unique_times)]
    
    # Find most active price range
    price_activity = data.groupby('price_level')['bid_volume'].sum() + data.groupby('price_level')['ask_volume'].sum()
    top_prices = price_activity.nlargest(max_price_levels).index.sort_values(ascending=False)
    data = data[data['price_level'].isin(top_prices)]
    
    # 3. Create pivot tables
    pivot_bid = data.pivot_table(
        values='bid_volume',
        index='price_level',
        columns='time_bin',
        fill_value=0
    ).sort_index(ascending=False)
    
    pivot_ask = data.pivot_table(
        values='ask_volume',
        index='price_level',
        columns='time_bin',
        fill_value=0
    ).sort_index(ascending=False)
    
    pivot_net = data.pivot_table(
        values='net_volume',
        index='price_level',
        columns='time_bin',
        fill_value=0
    ).sort_index(ascending=False)
    
    # Validate pivot tables have data
    if len(pivot_bid) == 0 or len(pivot_bid.columns) == 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No data after filtering', ha='center', va='center', fontsize=16)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        return fig
    
    # 4. Get time labels
    time_labels = data.groupby('time_bin')['time_str'].first().reindex(pivot_bid.columns).values
    
    # 5. Create chart
    n_rows = len(pivot_bid.index)
    n_cols = len(pivot_bid.columns)
    
    fig, ax = plt.subplots(figsize=(max(12, n_cols * 0.6), max(10, n_rows * 0.4)))
    
    # 6. Draw each cell
    cell_width = 1.0
    cell_height = 1.0
    
    for i in range(len(pivot_bid.index)):
        for j in range(len(pivot_bid.columns)):
            # Safely get values
            try:
                bid_vol = pivot_bid.iloc[i, j]
                ask_vol = pivot_ask.iloc[i, j]
                net_vol = pivot_net.iloc[i, j]
            except (IndexError, KeyError):
                continue
            
            # Draw cell background - color based on net volume
            if abs(net_vol) > 0:
                if net_vol > 0:
                    # Buy pressure - green
                    intensity = min(abs(net_vol) / (pivot_net.values.max() + 1), 1.0)
                    color = (0.9 * (1-intensity), 1.0, 0.9 * (1-intensity))  # Light to dark green
                elif net_vol < 0:
                    # Sell pressure - red
                    intensity = min(abs(net_vol) / (abs(pivot_net.values.min()) + 1), 1.0)
                    color = (1.0, 0.9 * (1-intensity), 0.9 * (1-intensity))  # Light to dark red
            else:
                color = (1.0, 1.0, 1.0)  # White
            
            rect = patches.Rectangle(
                (j, i), cell_width, cell_height,
                linewidth=0.5,
                edgecolor='gray',
                facecolor=color,
                alpha=0.6
            )
            ax.add_patch(rect)
            
            # Draw text - Bid on left, Ask on right
            cell_center_x = j + cell_width / 2
            cell_center_y = i + cell_height / 2
            
            # Bid (left side, green)
            if bid_vol > 0:
                ax.text(
                    cell_center_x - 0.25, cell_center_y,
                    f'{int(bid_vol)}',
                    ha='right', va='center',
                    fontsize=8,
                    color='darkgreen',
                    fontweight='bold'
                )
            
            # Ask (right side, red)
            if ask_vol > 0:
                ax.text(
                    cell_center_x + 0.25, cell_center_y,
                    f'{int(ask_vol)}',
                    ha='left', va='center',
                    fontsize=8,
                    color='darkred',
                    fontweight='bold'
                )
            
            # If only one, display net volume centered
            if (bid_vol == 0 or ask_vol == 0) and net_vol != 0:
                ax.text(
                    cell_center_x, cell_center_y,
                    f'{int(net_vol):+d}',
                    ha='center', va='center',
                    fontsize=7,
                    color='black',
                    style='italic'
                )
    
    # 7. Set axes
    ax.set_xlim(0, n_cols)
    ax.set_ylim(0, n_rows)
    
    # X-axis - Time labels (rotate 45 degrees to avoid overlap)
    ax.set_xticks([i + 0.5 for i in range(n_cols)])
    ax.set_xticklabels(time_labels, rotation=45, ha='right', fontsize=8)
    ax.xaxis.tick_top()  # Place time labels on top
    ax.xaxis.set_label_position('top')
    
    # Y-axis - Price levels
    ax.set_yticks([i + 0.5 for i in range(n_rows)])
    ax.set_yticklabels([f'{p:.2f}' for p in pivot_bid.index], fontsize=9)
    
    # Labels
    ax.set_xlabel('Time', fontsize=11, fontweight='bold')
    ax.set_ylabel('Price Level', fontsize=11, fontweight='bold')
    ax.set_title(f'Order Flow Footprint - {time_window}', fontsize=14, fontweight='bold', pad=20)
    
    # Invert Y-axis so prices decrease from top to bottom
    ax.invert_yaxis()
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='lightgreen', edgecolor='gray', label='Buy Pressure (Bid > Ask)'),
        Patch(facecolor='lightcoral', edgecolor='gray', label='Sell Pressure (Ask > Bid)'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='darkgreen', 
                   markersize=8, label='Bid Volume (left)'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='darkred', 
                   markersize=8, label='Ask Volume (right)')
    ]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.02, 1), 
              fontsize=9, framealpha=0.9)
    
    plt.tight_layout()
    return fig

# Power BI execution section
if 'dataset' in locals():
    df = dataset.copy()
    
    # Ensure correct data types
    df['time_bin'] = pd.to_numeric(df['time_bin'])
    df['price_level'] = pd.to_numeric(df['price_level'])
    df['bid_volume'] = pd.to_numeric(df['bid_volume'])
    df['ask_volume'] = pd.to_numeric(df['ask_volume'])
    
    # Calculate net_volume (if not present)
    if 'net_volume' not in df.columns:
        df['net_volume'] = df['bid_volume'] - df['ask_volume']
    else:
        df['net_volume'] = pd.to_numeric(df['net_volume'])
    
    # Get time window
    time_window = df['time_window'].iloc[0] if 'time_window' in df.columns else '5m'
    
    # Create chart (adjust max_time_bins and max_price_levels to control display range)
    fig = create_footprint_chart(df, time_window, max_time_bins=40, max_price_levels=25)
    plt.show()
else:
    print("Please provide dataset with columns: time_bin, price_level, bid_volume, ask_volume")
