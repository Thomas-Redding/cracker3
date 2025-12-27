use clap::Parser;
use trading_bot::connectors::{deribit, backtest}; // Assuming crate name is 'trading_bot'
use trading_bot::strategy::GammaScalp;
use trading_bot::models::MarketEvent;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    mode: String, 
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.mode == "live" {
        // --- LIVE MODE ---
        let api_key = std::env::var("DERIBIT_KEY").expect("Key required for live trading");
        let instruments = vec!["BTC-29MAR24-60000-C".to_string()];
        
        let stream = deribit::DeribitStream::new(instruments).await;
        let exec = deribit::DeribitExec::new(api_key).await;
        
        let mut strategy = GammaScalp::new(stream, exec);
        strategy.run().await;

    } else {
        // --- BACKTEST MODE ---
        println!("Loading historical data...");
        // In reality, load this from a parquet file
        let mock_data = vec![
            MarketEvent { 
                timestamp: 1700000000, 
                instrument: "BTC-29MAR24-60000-C".into(), 
                best_bid: Some(100.0), 
                best_ask: Some(101.0), 
                delta: Some(0.6) // Trigger the strategy
            }
        ];

        let stream = backtest::BacktestStream::new(mock_data);
        let exec = backtest::MockExec::new();
        
        let mut strategy = GammaScalp::new(stream, exec);
        strategy.run().await;
    }
}