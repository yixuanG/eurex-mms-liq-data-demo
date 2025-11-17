# Power BI Python Visual - Advanced Order Flow Footprint Chart
# Full Sierra Chart / Jigsaw style simulation, including cumulative Delta

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
import numpy as np
from datetime import datetime

def create_advanced_footprint(data, time_window='5m', max_time_bins=25, max_price_levels=25):
    """
    Create advanced Footprint chart - includes bid x ask format and cumulative delta
    """
    
    # 0. Data validation
    if data is None or len(data) == 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=16)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        return fig
    
    # 1. Data preprocessing
    data = data.copy()
    data['time_str'] = pd.to_datetime(data['time_bin'], unit='s').dt.strftime('%H:%M')
    
    # 2. Limit data volume
    unique_times = sorted(data['time_bin'].unique())[-max_time_bins:]
    data = data[data['time_bin'].isin(unique_times)]
    
    # Find most active price range
    price_activity = data.groupby('price_level')[['bid_volume', 'ask_volume']].sum().sum(axis=1)
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
    
    # 4. Calculate cumulative Delta (by time)
    cumulative_delta = pivot_net.cumsum(axis=1)
    
    # 5. Get time labels
    time_labels = data.groupby('time_bin')['time_str'].first().reindex(pivot_bid.columns).values
    
    # 6. Create chart - add one column for cumulative delta
    n_rows = len(pivot_bid.index)
    n_cols = len(pivot_bid.columns) + 1  # +1 for cumulative delta column
    
    fig, ax = plt.subplots(figsize=(max(14, n_cols * 0.7), max(12, n_rows * 0.45)))
    
    cell_width = 1.0
    cell_height = 1.0
    
    # 7. Draw main Order Flow cells
    for i in range(len(pivot_bid.index)):
        for j in range(len(pivot_bid.columns)):
            # Safely get values
            try:
                bid_vol = int(pivot_bid.iloc[i, j])
                ask_vol = int(pivot_ask.iloc[i, j])
                net_vol = int(pivot_net.iloc[i, j])
            except (IndexError, KeyError):
                continue  # Skip invalid cells
            
            # Background color - color based on delta
            if net_vol > 0:
                # Buy pressure - green
                max_delta = pivot_net.values.max()
                intensity = min(abs(net_vol) / (max_delta + 1), 1.0) if max_delta > 0 else 0
                color = (0.85 + 0.15*(1-intensity), 1.0, 0.85 + 0.15*(1-intensity))
            elif net_vol < 0:
                # Sell pressure - red
                min_delta = abs(pivot_net.values.min())
                intensity = min(abs(net_vol) / (min_delta + 1), 1.0) if min_delta > 0 else 0
                color = (1.0, 0.85 + 0.15*(1-intensity), 0.85 + 0.15*(1-intensity))
            else:
                color = (0.98, 0.98, 0.98)
            
            rect = patches.Rectangle(
                (j, i), cell_width, cell_height,
                linewidth=0.8,
                edgecolor='#CCCCCC',
                facecolor=color,
                alpha=0.7
            )
            ax.add_patch(rect)
            
            # Text content - use "bid x ask" format
            cell_center_x = j + cell_width / 2
            cell_center_y = i + cell_height / 2
            
            # If both bid and ask exist, use "bid x ask" format
            if bid_vol > 0 and ask_vol > 0:
                text = f'{bid_vol} × {ask_vol}'
                # Decide color based on which side is larger
                if bid_vol > ask_vol:
                    text_color = '#006400'  # Dark green
                    fontweight = 'bold'
                elif ask_vol > bid_vol:
                    text_color = '#8B0000'  # Dark red
                    fontweight = 'bold'
                else:
                    text_color = '#333333'
                    fontweight = 'normal'
                
                ax.text(
                    cell_center_x, cell_center_y,
                    text,
                    ha='center', va='center',
                    fontsize=7,
                    color=text_color,
                    fontweight=fontweight
                )
            
            # If only bid
            elif bid_vol > 0:
                ax.text(
                    cell_center_x, cell_center_y,
                    f'{bid_vol}',
                    ha='center', va='center',
                    fontsize=8,
                    color='#006400',
                    fontweight='bold'
                )
            
            # If only ask
            elif ask_vol > 0:
                ax.text(
                    cell_center_x, cell_center_y,
                    f'{ask_vol}',
                    ha='center', va='center',
                    fontsize=8,
                    color='#8B0000',
                    fontweight='bold'
                )
            
            # Display net value (small text, below)
            if abs(net_vol) > 5:  # Only display significant net values
                ax.text(
                    cell_center_x, cell_center_y + 0.35,
                    f'{net_vol:+d}',
                    ha='center', va='top',
                    fontsize=5,
                    color='black',
                    style='italic',
                    alpha=0.6
                )
    
    # 8. Draw cumulative Delta column (rightmost)
    delta_col_x = len(pivot_bid.columns)
    
    for i in range(len(pivot_bid.index)):
        try:
            cumul_delta = int(cumulative_delta.iloc[i, -1])  # Cumulative delta of last time window
        except (IndexError, KeyError):
            cumul_delta = 0
        
        # Background color
        max_cumul = abs(cumulative_delta.values).max()
        if cumul_delta > 0:
            intensity = min(abs(cumul_delta) / (max_cumul + 1), 1.0)
            color = (0.7 + 0.3*(1-intensity), 1.0, 0.7 + 0.3*(1-intensity))
        elif cumul_delta < 0:
            intensity = min(abs(cumul_delta) / (max_cumul + 1), 1.0)
            color = (1.0, 0.7 + 0.3*(1-intensity), 0.7 + 0.3*(1-intensity))
        else:
            color = (0.95, 0.95, 0.95)
        
        rect = patches.Rectangle(
            (delta_col_x, i), cell_width, cell_height,
            linewidth=1.0,
            edgecolor='black',
            facecolor=color,
            alpha=0.8
        )
        ax.add_patch(rect)
        
        # Text
        ax.text(
            delta_col_x + 0.5, i + 0.5,
            f'{cumul_delta:+d}',
            ha='center', va='center',
            fontsize=9,
            color='black',
            fontweight='bold'
        )
    
    # 9. Set axes
    ax.set_xlim(0, n_cols)
    ax.set_ylim(0, n_rows)
    
    # X-axis
    x_labels = list(time_labels) + ['Δ']
    ax.set_xticks([i + 0.5 for i in range(n_cols)])
    ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=8)
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position('top')
    
    # Y-axis
    ax.set_yticks([i + 0.5 for i in range(n_rows)])
    ax.set_yticklabels([f'{p:.2f}' for p in pivot_bid.index], fontsize=9, fontweight='bold')
    
    # Title and labels
    ax.set_xlabel('Time', fontsize=11, fontweight='bold')
    ax.set_ylabel('Price', fontsize=11, fontweight='bold')
    ax.set_title(f'Order Flow Footprint - {time_window}\\n(Format: Bid × Ask, Green=Buy Pressure, Red=Sell Pressure)', 
                 fontsize=12, fontweight='bold', pad=25)
    
    # Invert Y-axis
    ax.invert_yaxis()
    
    # Add vertical separator line (cumulative delta column)
    ax.axvline(x=delta_col_x, color='black', linewidth=2, alpha=0.7)
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='lightgreen', edgecolor='gray', alpha=0.7, label='Buy Pressure (Bid > Ask)'),
        Patch(facecolor='lightcoral', edgecolor='gray', alpha=0.7, label='Sell Pressure (Ask > Bid)'),
        plt.Line2D([0], [0], color='w', marker='s', markerfacecolor='lightgreen', 
                   markersize=10, markeredgecolor='black', label='Cumulative Δ (Buy)'),
        plt.Line2D([0], [0], color='w', marker='s', markerfacecolor='lightcoral', 
                   markersize=10, markeredgecolor='black', label='Cumulative Δ (Sell)')
    ]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.05, 1), 
              fontsize=8, framealpha=0.95)
    
    plt.tight_layout()
    return fig

# Power BI execution section
if 'dataset' in locals():
    df = dataset.copy()
    
    # Data type conversion
    df['time_bin'] = pd.to_numeric(df['time_bin'])
    df['price_level'] = pd.to_numeric(df['price_level'])
    df['bid_volume'] = pd.to_numeric(df['bid_volume'])
    df['ask_volume'] = pd.to_numeric(df['ask_volume'])
    
    if 'net_volume' not in df.columns:
        df['net_volume'] = df['bid_volume'] - df['ask_volume']
    else:
        df['net_volume'] = pd.to_numeric(df['net_volume'])
    
    time_window = df['time_window'].iloc[0] if 'time_window' in df.columns else '5m'
    
    # Create advanced footprint chart
    # Adjustable parameters: max_time_bins controls how many time windows to display, max_price_levels controls how many price levels to display
    fig = create_advanced_footprint(df, time_window, max_time_bins=30, max_price_levels=30)
    plt.show()
else:
    print("Please provide dataset with columns: time_bin, price_level, bid_volume, ask_volume")
