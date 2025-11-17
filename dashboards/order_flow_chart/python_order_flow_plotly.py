# Power BI Python Visual - Order Flow Chart (Plotly)
# Usage in Power BI: Insert → Python Visual → paste this code

import plotly.graph_objects as go
import pandas as pd
import numpy as np

# Power BI will automatically pass in the 'dataset' DataFrame
# Required columns: price_level, bid_volume, ask_volume, net_volume, timestamp_bin

# Example data structure (Power BI will provide this in actual use)
# dataset = pd.DataFrame({
#     'price_level': [100, 101, 102, 103, 104],
#     'bid_volume': [50, 30, 20, 10, 5],
#     'ask_volume': [5, 10, 20, 30, 50],
#     'net_volume': [45, 20, 0, -20, -45],
#     'time_window': '1s'  # or 3s, 1m, 5m
# })

def create_order_flow_chart(data, time_window='1s'):
    """
    Create Order Flow heatmap
    
    Parameters:
    - data: DataFrame with columns [price_level, bid_volume, ask_volume, net_volume]
    - time_window: Time window (1s, 3s, 1m, 5m)
    """
    
    # Data preprocessing
    data = data.sort_values('price_level', ascending=False)
    
    # Create chart
    fig = go.Figure()
    
    # Bid side (green) - left side
    fig.add_trace(go.Bar(
        y=data['price_level'],
        x=-data['bid_volume'],  # Negative value to display on left side
        orientation='h',
        name='Bid',
        marker=dict(
            color=data['bid_volume'],
            colorscale='Greens',
            showscale=True,
            colorbar=dict(x=-0.15, title='Bid Volume')
        ),
        text=data['bid_volume'],
        textposition='inside',
        hovertemplate='<b>Price:</b> %{y}<br>' +
                      '<b>Bid:</b> %{text}<br>' +
                      '<extra></extra>'
    ))
    
    # Ask side (red) - right side
    fig.add_trace(go.Bar(
        y=data['price_level'],
        x=data['ask_volume'],
        orientation='h',
        name='Ask',
        marker=dict(
            color=data['ask_volume'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(x=1.15, title='Ask Volume')
        ),
        text=data['ask_volume'],
        textposition='inside',
        hovertemplate='<b>Price:</b> %{y}<br>' +
                      '<b>Ask:</b> %{text}<br>' +
                      '<extra></extra>'
    ))
    
    # Layout settings
    fig.update_layout(
        title=f'Order Flow - {time_window}',
        xaxis=dict(
            title='Volume',
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor='black'
        ),
        yaxis=dict(
            title='Price Level',
            side='right'
        ),
        barmode='overlay',
        height=600,
        width=800,
        showlegend=True,
        legend=dict(x=0.5, y=1.1, orientation='h'),
        template='plotly_white'
    )
    
    return fig

# Power BI execution section
if 'dataset' in locals():
    # Get data from Power BI
    df = dataset.copy()
    
    # Ensure correct data types
    df['price_level'] = pd.to_numeric(df['price_level'])
    df['bid_volume'] = pd.to_numeric(df['bid_volume'])
    df['ask_volume'] = pd.to_numeric(df['ask_volume'])
    
    # Get time window (if time_window column exists)
    time_window = df['time_window'].iloc[0] if 'time_window' in df.columns else '1s'
    
    # Create chart
    fig = create_order_flow_chart(df, time_window)
    
    # Display
    fig.show()
else:
    print("Please provide dataset with columns: price_level, bid_volume, ask_volume")
