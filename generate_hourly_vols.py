#!/usr/bin/env python3
"""
Generate hourly volatility weights for the trading system.

This script downloads BTC price data from Yahoo Finance, calculates hourly
volatility patterns, and outputs a JSON file with 168 values (7 days × 24 hours)
that can be loaded by the Rust trading system for vol-weighted time interpolation.

Usage:
    python3 -m venv venv
    source venv/bin/activate
    pip install yfinance pandas numpy
    python generate_hourly_vols.py
"""

import json
import pandas as pd
import numpy as np
import yfinance as yf

# 1. Download hourly BTC data (max period for 1h is 730 days)
print("Downloading BTC-USD hourly data...")
ticker = "BTC-USD"
data = yf.download(ticker, period="2y", interval="1h")

# 2. Calculate Log Returns
data['log_return'] = np.log(data['Close'] / data['Close'].shift(1))

# 3. Extract Hour and Day of Week
# 0 = Monday, 6 = Sunday (matches chrono's weekday().num_days_from_monday())
data['day_of_week'] = data.index.dayofweek
data['hour'] = data.index.hour

# 4. Group by Day and Hour and calculate Volatility (Std Dev)
vol_by_hour = data.groupby(['day_of_week', 'hour'])['log_return'].std()

# 5. Display the volatility matrix
vol_matrix = vol_by_hour.unstack()
day_map = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
vol_matrix.index = vol_matrix.index.map(day_map)

print("\nAverage Hourly Volatility (Standard Deviation of Log Returns):")
print(vol_matrix)
print(f"\nTotal data points: {len(data)} hours")

# 6. Flatten to 168 values in order: Monday 00:00 → Sunday 23:00
# This matches the indexing in WeightedVolTimeStrategy:
#   hour_idx = day_idx * 24 + hour  (where day_idx 0 = Monday)
hourly_vols_list = []
for day in range(7):  # 0=Mon through 6=Sun
    for hour in range(24):
        hourly_vols_list.append(float(vol_by_hour[(day, hour)]))

# 7. Save to JSON in cache directory
import os
os.makedirs('cache', exist_ok=True)
output_file = 'cache/hourly_vols.json'
with open(output_file, 'w') as f:
    json.dump(hourly_vols_list, f, indent=2)

print(f"\nSaved {len(hourly_vols_list)} hourly volatility values to {output_file}")
print(f"  Min: {min(hourly_vols_list):.6f}")
print(f"  Max: {max(hourly_vols_list):.6f}")
print(f"  Mean: {np.mean(hourly_vols_list):.6f}")
print(f"\nTo use in trading system, add to config.toml:")
print(f'  hourly_vols_file = "{output_file}"')
print(f'  vol_time_strategy = "weighted"')

