# Power BI Python Visual - Order Flow Heatmap (Matplotlib)
# Closer to traditional Order Flow chart style

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

def create_order_flow_heatmap(data, time_window='1s'):
    """
    Create Bookmap-style Order Flow heatmap
    
    Parameters:
    - data: DataFrame with columns [time_bin, price_level, bid_volume, ask_volume, net_volume]
    - time_window: Time window (1s, 3s, 1m, 5m)
    """
    
    # Data pivot tables
    pivot_bid = data.pivot_table(
        values='bid_volume', 
        index='price_level', 
        columns='time_bin',
        fill_value=0
    )
    
    pivot_ask = data.pivot_table(
        values='ask_volume', 
        index='price_level', 
        columns='time_bin',
        fill_value=0
    )
    
    pivot_net = data.pivot_table(
        values='net_volume', 
        index='price_level', 
        columns='time_bin',
        fill_value=0
    )
    
    # Create custom color mapping
    # Green (buy orders) -> White (neutral) -> Red (sell orders)
    colors_bid = ['#ffffff', '#90EE90', '#00AA00']
    colors_ask = ['#ffffff', '#FFB6C1', '#FF0000']
    
    cmap_bid = LinearSegmentedColormap.from_list('bid', colors_bid)
    cmap_ask = LinearSegmentedColormap.from_list('ask', colors_ask)
    
    # Create chart
    fig, axes = plt.subplots(1, 3, figsize=(18, 10))
    
    # Subplot 1: Bid Volume
    im1 = axes[0].imshow(
        pivot_bid.values,
        aspect='auto',
        cmap=cmap_bid,
        interpolation='nearest'
    )
    axes[0].set_title(f'Bid Volume - {time_window}', fontsize=14, fontweight='bold')
    axes[0].set_ylabel('Price Level', fontsize=12)
    axes[0].set_xlabel('Time Bins', fontsize=12)
    axes[0].set_yticks(range(len(pivot_bid.index)))
    axes[0].set_yticklabels([f'{p:.2f}' for p in pivot_bid.index])
    
    # Add value annotations
    for i in range(len(pivot_bid.index)):
        for j in range(len(pivot_bid.columns)):
            val = pivot_bid.values[i, j]
            if val > 0:
                text = axes[0].text(j, i, f'{int(val)}',
                                   ha="center", va="center",
                                   color="black" if val < pivot_bid.values.max()/2 else "white",
                                   fontsize=8)
    
    plt.colorbar(im1, ax=axes[0], label='Volume')
    
    # Subplot 2: Ask Volume
    im2 = axes[1].imshow(
        pivot_ask.values,
        aspect='auto',
        cmap=cmap_ask,
        interpolation='nearest'
    )
    axes[1].set_title(f'Ask Volume - {time_window}', fontsize=14, fontweight='bold')
    axes[1].set_ylabel('Price Level', fontsize=12)
    axes[1].set_xlabel('Time Bins', fontsize=12)
    axes[1].set_yticks(range(len(pivot_ask.index)))
    axes[1].set_yticklabels([f'{p:.2f}' for p in pivot_ask.index])
    
    # Add value annotations
    for i in range(len(pivot_ask.index)):
        for j in range(len(pivot_ask.columns)):
            val = pivot_ask.values[i, j]
            if val > 0:
                text = axes[1].text(j, i, f'{int(val)}',
                                   ha="center", va="center",
                                   color="black" if val < pivot_ask.values.max()/2 else "white",
                                   fontsize=8)
    
    plt.colorbar(im2, ax=axes[1], label='Volume')
    
    # Subplot 3: Net Volume (Imbalance)
    # Use diverging colormap
    vmax = abs(pivot_net.values).max()
    im3 = axes[2].imshow(
        pivot_net.values,
        aspect='auto',
        cmap='RdYlGn',  # Red (sell pressure) -> Yellow (neutral) -> Green (buy pressure)
        interpolation='nearest',
        vmin=-vmax,
        vmax=vmax
    )
    axes[2].set_title(f'Net Volume (Imbalance) - {time_window}', fontsize=14, fontweight='bold')
    axes[2].set_ylabel('Price Level', fontsize=12)
    axes[2].set_xlabel('Time Bins', fontsize=12)
    axes[2].set_yticks(range(len(pivot_net.index)))
    axes[2].set_yticklabels([f'{p:.2f}' for p in pivot_net.index])
    
    # Add value annotations
    for i in range(len(pivot_net.index)):
        for j in range(len(pivot_net.columns)):
            val = pivot_net.values[i, j]
            if abs(val) > 0:
                text = axes[2].text(j, i, f'{int(val):+d}',
                                   ha="center", va="center",
                                   color="black" if abs(val) < vmax/2 else "white",
                                   fontsize=8)
    
    plt.colorbar(im3, ax=axes[2], label='Net Volume')
    
    plt.tight_layout()
    return fig

# Power BI execution section
if 'dataset' in locals():
    df = dataset.copy()
    
    # Ensure correct data types
    df['price_level'] = pd.to_numeric(df['price_level'])
    df['bid_volume'] = pd.to_numeric(df['bid_volume'])
    df['ask_volume'] = pd.to_numeric(df['ask_volume'])
    df['net_volume'] = pd.to_numeric(df.get('net_volume', df['bid_volume'] - df['ask_volume']))
    
    # Get time window
    time_window = df['time_window'].iloc[0] if 'time_window' in df.columns else '1s'
    
    # Create chart
    fig = create_order_flow_heatmap(df, time_window)
    plt.show()
else:
    print("Please provide dataset with columns: time_bin, price_level, bid_volume, ask_volume")
