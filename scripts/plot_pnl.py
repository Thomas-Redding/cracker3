import pandas as pd
import matplotlib.pyplot as pd_plt
import matplotlib.pyplot as plt
import argparse
import os
from datetime import datetime

def plot_pnl(csv_file):
    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} not found.")
        return

    # Read CSV
    df = pd.read_csv(csv_file)
    
    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Handle missing columns if reading old CSVs
    if 'realized_pnl' not in df.columns:
        print("Warning: 'realized_pnl' not found in CSV. Plotting 0.")
        df['realized_pnl'] = 0.0
    
    # Set up the plot style
    plt.style.use('ggplot')
    
    # Create a figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
    
    # --- Plot 1: Total Equity ---
    # Primary Axis: Total Equity ($)
    if 'total_equity' in df.columns:
        l1 = ax1.plot(df['datetime'], df['total_equity'], label='Total Equity ($)', color='blue', linewidth=2)
        ax1.set_ylabel('Total Equity ($)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
    else:
        l1 = ax1.plot(df['datetime'], df['realized_pnl'], label='Realized PnL ($)', color='blue', linewidth=2)
        ax1.set_ylabel('Realized PnL ($)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
    
    ax1.grid(True, linestyle='--', alpha=0.7)

    # Secondary Axis: Expected Utility (if available)
    if 'expected_utility' in df.columns:
        ax1_twin = ax1.twinx()
        l2 = ax1_twin.plot(df['datetime'], df['expected_utility'], label='Exp. Utility', color='gray', linestyle='--', alpha=0.7)
        ax1_twin.set_ylabel('Expected Utility', color='gray')
        ax1_twin.tick_params(axis='y', labelcolor='gray')
        ax1_twin.grid(False) # avoid clutter
        lines = l1 + l2
    else:
        lines = l1

    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left')
    ax1.set_title(f'Backtest Analysis - {os.path.basename(csv_file)}')

    # --- Plot 2: Risk / Return Stats ---
    # Primary Axis: Expected Return (%)
    lines_2 = []
    if 'expected_return' in df.columns:
        l3 = ax2.plot(df['datetime'], df['expected_return'] * 100, label='Exp. Return %', color='green')
        ax2.set_ylabel('Exp. Return (%)', color='green')
        ax2.tick_params(axis='y', labelcolor='green')
        lines_2 += l3
    else:
        # Plot empty if missing to maintain subplot structure
        ax2.text(0.5, 0.5, 'Expected Return not in data', ha='center', transform=ax2.transAxes)

    # Secondary Axis: Probability of Loss (%)
    if 'prob_loss' in df.columns:
        ax2_twin = ax2.twinx()
        l4 = ax2_twin.plot(df['datetime'], df['prob_loss'] * 100, label='Prob. Loss %', color='red', linestyle='--')
        ax2_twin.set_ylabel('Prob. Loss (%)', color='red')
        ax2_twin.tick_params(axis='y', labelcolor='red')
        ax2_twin.set_ylim(0, 100)
        ax2_twin.grid(False)
        lines_2 += l4
    
    if lines_2:
        labels_2 = [l.get_label() for l in lines_2]
        ax2.legend(lines_2, labels_2, loc='upper left')

    # --- Plot 3: Positions & Exposure ---
    # Primary Axis: Position Count
    if 'positions_count' in df.columns:
        l5 = ax3.plot(df['datetime'], df['positions_count'], label='Positions Count', color='purple')
    elif 'total_positions' in df.columns: # fallback for old schema
        l5 = ax3.plot(df['datetime'], df['total_positions'], label='Positions Count', color='purple')
    else:
        l5 = []
        
    if l5:
        ax3.set_ylabel('Position Count', color='purple')
        ax3.tick_params(axis='y', labelcolor='purple')
    
    # Secondary Axis: Total Value ($)
    lines_3 = l5 if l5 else []
    
    if 'total_value' in df.columns:
        ax3_twin = ax3.twinx()
        l6 = ax3_twin.plot(df['datetime'], df['total_value'], label='Total Exposure ($)', color='orange', linestyle=':')
        ax3_twin.set_ylabel('Total Exposure ($)', color='orange')
        ax3_twin.tick_params(axis='y', labelcolor='orange')
        ax3_twin.grid(False)
        lines_3 += l6

    if lines_3:
        labels_3 = [l.get_label() for l in lines_3]
        ax3.legend(lines_3, labels_3, loc='upper left')
    
    ax3.set_xlabel('Time')
    
    # Format x-axis dates
    fig.autofmt_xdate()
    
    plt.tight_layout()
    
    # Save plot
    output_file = csv_file.replace('.csv', '.png')
    plt.savefig(output_file)
    print(f"Plot saved to: {output_file}")
    
    # Show plot
    try:
        # Check if running in a headless environment
        if os.environ.get('DISPLAY', '') == '':
            print("Headless environment detected. Skipping plt.show().")
        else:
            plt.show()
    except Exception as e:
        print(f"Could not display plot: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot PnL history from CSV')
    parser.add_argument('file', type=str, help='Path to history CSV file', nargs='?', default='backtest_results/portfolio_history.csv')
    args = parser.parse_args()
    
    plot_pnl(args.file)
