use clap::Parser;
use trading_bot::connectors::{deribit, backtest, polymarket};
use trading_bot::strategy::GammaScalp;
use trading_bot::models::MarketEvent;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    mode: String, 
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    match args.mode.as_str() {
        "live-deribit" => {
            let api_key = std::env::var("DERIBIT_KEY").expect("Key required for live trading");
            let instruments = vec!["BTC-29MAR24-60000-C".to_string()];
            
            let stream = deribit::DeribitStream::new(instruments).await;
            let exec = deribit::DeribitExec::new(api_key).await;
            
            let mut strategy = GammaScalp::new(stream, exec);
            strategy.run().await;    
        },
        "live-poly" => {
            println!("Starting Polymarket Engine...");
            // Example Token ID: 'Yes' share of "Will Bitcoin hit 100k in 2024?"
            // You can get these from the Polymarket UI URL or API.
            let token_ids = vec![
                "21742633143463906290569050155826241533067272736897614950488156847949938836455".to_string() 
            ];
            
            let stream = polymarket::PolymarketStream::new(token_ids).await;
            let exec = polymarket::PolymarketExec::new("dummy_key".into()).await;
            
            let mut strategy = GammaScalp::new(stream, exec);
            strategy.run().await;
        },
        "backtest" => {
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
        _ => println!("Unknown mode"),
    }
}