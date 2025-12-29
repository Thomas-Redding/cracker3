// src/main.rs

use clap::Parser;
use std::sync::Arc;
use trading_bot::connectors::{backtest, deribit, polymarket};
use trading_bot::dashboard::DashboardServer;
use trading_bot::engine::MarketRouter;
use trading_bot::models::MarketEvent;
use trading_bot::strategy::{GammaScalp, MomentumStrategy};
use trading_bot::traits::Strategy;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    mode: String,

    /// Path to historical data file (JSONL format) for historical-backtest mode
    #[arg(long)]
    file: Option<String>,

    /// Enable realtime playback simulation for historical-backtest mode
    #[arg(long, default_value = "false")]
    realtime: bool,

    /// Speed multiplier for realtime playback (e.g., 2.0 = 2x speed)
    #[arg(long, default_value = "1.0")]
    speed: f64,

    /// Enable the web dashboard on the specified port
    #[arg(long)]
    dashboard: Option<u16>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    match args.mode.as_str() {
        "live-deribit" => {
            run_deribit_multi_strategy(args.dashboard).await;
        }
        "live-poly" => {
            run_polymarket_multi_strategy(args.dashboard).await;
        }
        "backtest" => {
            run_backtest(args.dashboard).await;
        }
        "historical-backtest" => {
            let file_path = args.file.expect("--file is required for historical-backtest mode");
            run_historical_backtest(&file_path, args.realtime, args.speed, args.dashboard).await;
        }
        _ => println!("Unknown mode. Use: live-deribit, live-poly, backtest, or historical-backtest"),
    }
}

/// Helper function to start the dashboard server in the background
fn start_dashboard(strategies: Vec<Arc<dyn Strategy>>, port: u16) {
    let dashboard = DashboardServer::new(strategies, port);
    tokio::spawn(async move {
        if let Err(e) = dashboard.run().await {
            eprintln!("Dashboard server error: {}", e);
        }
    });
    println!("Dashboard available at http://localhost:{}", port);
}

/// Runs multiple strategies on Deribit using a single WebSocket connection.
async fn run_deribit_multi_strategy(dashboard_port: Option<u16>) {
    println!("Starting Multi-Strategy Deribit Engine...");

    let api_key = std::env::var("DERIBIT_KEY").expect("DERIBIT_KEY required for live trading");

    // 1. Create the shared execution client
    let exec = deribit::DeribitExec::new(api_key).await.shared();

    // 2. Create strategies with their specific instruments
    let gamma_scalp_btc = GammaScalp::new(
        "GammaScalp-BTC",
        vec!["BTC-29MAR24-60000-C".to_string()],
        exec.clone(),
    );

    let momentum_eth = MomentumStrategy::new(
        "Momentum-ETH",
        vec!["ETH-29MAR24-4000-C".to_string()],
        exec.clone(),
        10,   // 10-tick lookback
        0.02, // 2% momentum threshold
    );

    // 3. Build the engine and aggregate subscriptions
    let strategies: Vec<Arc<dyn Strategy>> = vec![gamma_scalp_btc, momentum_eth];

    // 4. Start dashboard if requested
    if let Some(port) = dashboard_port {
        start_dashboard(strategies.clone(), port);
    }

    let all_instruments = MarketRouter::<deribit::DeribitStream>::aggregate_subscriptions(&strategies);
    println!("Aggregated subscriptions: {:?}", all_instruments);

    // 5. Create the stream with the superset of all instruments
    let stream = deribit::DeribitStream::new(all_instruments).await;

    // 6. Create and run the router
    let router = MarketRouter::new(stream, strategies);
    router.run().await;
}

/// Runs multiple strategies on Polymarket using a single WebSocket connection.
async fn run_polymarket_multi_strategy(dashboard_port: Option<u16>) {
    println!("Starting Multi-Strategy Polymarket Engine...");

    // Example token IDs
    let btc_100k_token =
        "21742633143463906290569050155826241533067272736897614950488156847949938836455".to_string();
    let eth_token = "some_other_token_id".to_string();

    // 1. Create shared execution client
    let exec = polymarket::PolymarketExec::new("dummy_key".into())
        .await
        .shared();

    // 2. Create strategies
    let gamma_scalp = GammaScalp::with_threshold(
        "GammaScalp-Poly",
        vec![btc_100k_token.clone()],
        exec.clone(),
        0.3, // Lower threshold for prediction markets
    );

    let momentum = MomentumStrategy::default_config(
        "Momentum-Poly",
        vec![eth_token.clone()],
        exec.clone(),
    );

    // 3. Build strategies list
    let strategies: Vec<Arc<dyn Strategy>> = vec![gamma_scalp, momentum];

    // 4. Start dashboard if requested
    if let Some(port) = dashboard_port {
        start_dashboard(strategies.clone(), port);
    }

    let all_tokens =
        MarketRouter::<polymarket::PolymarketStream>::aggregate_subscriptions(&strategies);
    println!("Aggregated token subscriptions: {:?}", all_tokens);

    // 5. Create stream with all tokens
    let stream = polymarket::PolymarketStream::new(all_tokens).await;

    // 6. Run
    let router = MarketRouter::new(stream, strategies);
    router.run().await;
}

/// Runs a backtest with multiple strategies.
async fn run_backtest(dashboard_port: Option<u16>) {
    println!("Starting Multi-Strategy Backtest...");

    // 1. Create mock data with events for different instruments
    let mock_data = vec![
        MarketEvent {
            timestamp: 1700000000,
            instrument: "BTC-29MAR24-60000-C".into(),
            best_bid: Some(100.0),
            best_ask: Some(101.0),
            delta: Some(0.6), // High delta - should trigger GammaScalp
        },
        MarketEvent {
            timestamp: 1700000001,
            instrument: "ETH-29MAR24-4000-C".into(),
            best_bid: Some(50.0),
            best_ask: Some(51.0),
            delta: Some(0.3),
        },
        MarketEvent {
            timestamp: 1700000002,
            instrument: "BTC-29MAR24-60000-C".into(),
            best_bid: Some(102.0),
            best_ask: Some(103.0),
            delta: Some(0.7),
        },
        MarketEvent {
            timestamp: 1700000003,
            instrument: "ETH-29MAR24-4000-C".into(),
            best_bid: Some(52.0),
            best_ask: Some(53.0),
            delta: Some(0.35),
        },
        // Add more events...
    ];

    // 2. Create shared mock execution client
    let exec = backtest::MockExec::new().shared();

    // 3. Create strategies
    let gamma_scalp = GammaScalp::new(
        "GammaScalp-BTC",
        vec!["BTC-29MAR24-60000-C".to_string()],
        exec.clone(),
    );

    let momentum = MomentumStrategy::new(
        "Momentum-ETH",
        vec!["ETH-29MAR24-4000-C".to_string()],
        exec.clone(),
        3, // Shorter lookback for backtest demo
        0.01,
    );

    let strategies: Vec<Arc<dyn Strategy>> = vec![gamma_scalp, momentum];

    // 4. Start dashboard if requested
    if let Some(port) = dashboard_port {
        start_dashboard(strategies.clone(), port);
    }

    // 5. Create backtest stream and run
    let stream = backtest::BacktestStream::new(mock_data);
    let router = MarketRouter::new(stream, strategies);
    router.run().await;

    println!("\nBacktest complete!");
    // In a real implementation, you would analyze exec.get_fills() here
}

/// Runs a backtest using historical data from a JSONL file.
async fn run_historical_backtest(file_path: &str, realtime: bool, speed: f64, dashboard_port: Option<u16>) {
    println!("Starting Historical Backtest from file: {}", file_path);

    // 1. Create playback configuration
    let config = if realtime {
        println!("Realtime playback enabled at {}x speed", speed);
        backtest::PlaybackConfig::realtime(speed)
    } else {
        backtest::PlaybackConfig::instant()
    };

    // 2. Create the historical stream
    let stream = match backtest::HistoricalStream::with_config(file_path, config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to open historical data file '{}': {}", file_path, e);
            return;
        }
    };

    // 3. Create shared mock execution client
    let exec = backtest::MockExec::new().shared();

    // 4. Create strategies (using same config as regular backtest)
    // In a production setup, you might want to configure these via CLI or config file
    let gamma_scalp = GammaScalp::new(
        "GammaScalp-BTC",
        vec!["BTC-29MAR24-60000-C".to_string()],
        exec.clone(),
    );

    let momentum = MomentumStrategy::new(
        "Momentum-ETH",
        vec!["ETH-29MAR24-4000-C".to_string()],
        exec.clone(),
        3,
        0.01,
    );

    let strategies: Vec<Arc<dyn Strategy>> = vec![gamma_scalp, momentum];

    // 5. Start dashboard if requested
    if let Some(port) = dashboard_port {
        start_dashboard(strategies.clone(), port);
    }

    // 6. Run the backtest
    let router = MarketRouter::new(stream, strategies);
    router.run().await;

    println!("\nHistorical backtest complete!");
}
