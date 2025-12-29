// src/main.rs
//
// Strategy-centric trading engine with multi-exchange support.
// Strategies declare which exchanges they need, and the engine auto-connects.

use chrono::Utc;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use trading_bot::config::{default_config_template, Config};
use trading_bot::connectors::{backtest, deribit, derive, polymarket};
use trading_bot::dashboard::DashboardServer;
use trading_bot::engine::Engine;
use trading_bot::models::{Exchange, Instrument, MarketEvent};
use trading_bot::strategy::{GammaScalp, MomentumStrategy};
use trading_bot::traits::{ExecutionRouter, SharedExecutionClient, Strategy};

#[derive(Parser)]
#[command(name = "trading-bot")]
#[command(about = "Multi-exchange trading bot with strategy-centric architecture")]
struct Args {
    /// Mode of operation
    #[arg(long, default_value = "live")]
    mode: String,

    /// Path to configuration file (TOML)
    #[arg(long, short)]
    config: Option<String>,

    /// Comma-separated list of strategies to run (for quick testing)
    #[arg(long)]
    strategies: Option<String>,

    /// Path to historical data file (JSONL) for backtest mode
    #[arg(long)]
    file: Option<String>,

    /// Timestamp for historical catalog state (backtest mode)
    #[arg(long)]
    as_of: Option<u64>,

    /// Enable realtime playback simulation for backtest mode
    #[arg(long, default_value = "false")]
    realtime: bool,

    /// Speed multiplier for realtime playback
    #[arg(long, default_value = "1.0")]
    speed: f64,

    /// Enable the web dashboard on the specified port
    #[arg(long)]
    dashboard: Option<u16>,

    /// Generate a default configuration file
    #[arg(long)]
    generate_config: bool,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    // Handle config generation
    if args.generate_config {
        println!("{}", default_config_template());
        return;
    }

    match args.mode.as_str() {
        "live" => run_live_mode(&args).await,
        "backtest" => run_backtest_mode(&args).await,
        "demo" => run_demo_mode(&args).await,
        _ => {
            eprintln!("Unknown mode: {}. Use: live, backtest, or demo", args.mode);
            std::process::exit(1);
        }
    }
}

// =============================================================================
// Live Mode: Config-driven multi-exchange trading
// =============================================================================

async fn run_live_mode(args: &Args) {
    println!("Starting Live Trading Mode...");

    // Load configuration
    let config = match &args.config {
        Some(path) => match Config::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                eprintln!("Use --generate-config to create a template.");
                std::process::exit(1);
            }
        },
        None => {
            eprintln!("No config file specified. Use --config <path>");
            eprintln!("Use --generate-config to create a template.");
            std::process::exit(1);
        }
    };

    // Determine required exchanges
    let required_exchanges = config.required_exchanges();
    println!("Required exchanges: {:?}", required_exchanges);

    // Build execution clients for each exchange
    let mut exec_clients: HashMap<Exchange, SharedExecutionClient> = HashMap::new();

    for exchange in &required_exchanges {
        match exchange {
            Exchange::Deribit => {
                let api_key = std::env::var("DERIBIT_KEY")
                    .expect("DERIBIT_KEY required for Deribit trading");
                let client = deribit::DeribitExec::new(api_key).await.shared();
                exec_clients.insert(Exchange::Deribit, client);
            }
            Exchange::Derive => {
                let api_key = std::env::var("DERIVE_KEY")
                    .unwrap_or_else(|_| "dummy_key".to_string());
                let client = derive::DeriveExec::new(api_key).await.shared();
                exec_clients.insert(Exchange::Derive, client);
            }
            Exchange::Polymarket => {
                let api_key = std::env::var("POLYMARKET_KEY")
                    .unwrap_or_else(|_| "dummy_key".to_string());
                let client = polymarket::PolymarketExec::new(api_key).await.shared();
                exec_clients.insert(Exchange::Polymarket, client);
            }
        }
    }

    // Build execution router
    let exec_router = Arc::new(ExecutionRouter::new(exec_clients));

    // Build strategies from config
    let strategies = config.build_strategies(exec_router.clone());
    println!("Loaded {} strategies", strategies.len());

    // Start dashboard if configured
    let dashboard_port = args.dashboard.or(config.global.dashboard_port);
    if let Some(port) = dashboard_port {
        start_dashboard(strategies.clone(), port);
    }

    // Aggregate subscriptions from all strategies
    let mut exchange_instruments: HashMap<Exchange, Vec<String>> = HashMap::new();
    for strategy in &strategies {
        for instrument in strategy.discover_subscriptions().await {
            exchange_instruments
                .entry(instrument.exchange())
                .or_default()
                .push(instrument.symbol().to_string());
        }
    }

    // Build engine with streams for each exchange
    let mut engine = Engine::new(strategies);
    engine = engine.with_exec_router(exec_router);

    // Create streams for each required exchange
    for exchange in &required_exchanges {
        let instruments = exchange_instruments
            .get(exchange)
            .cloned()
            .unwrap_or_default();

        if instruments.is_empty() {
            println!("No instruments for {:?}, skipping stream", exchange);
            continue;
        }

        println!("{:?}: Subscribing to {} instruments", exchange, instruments.len());

        match exchange {
            Exchange::Deribit => {
                let stream = deribit::DeribitStream::new(instruments).await;
                // Wrap with recording
                let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
                let path = format!("recordings/deribit_{}.jsonl", timestamp);
                std::fs::create_dir_all("recordings").ok();
                match backtest::RecordingStream::new(stream, &path) {
                    Ok(recording) => {
                        println!("Deribit: Recording to {}", path);
                        engine = engine.with_stream(Exchange::Deribit, Box::new(recording));
                    }
                    Err(_) => {
                        // Can't record, skip this exchange
                        println!("Deribit: Failed to create recording, skipping");
                    }
                }
            }
            Exchange::Derive => {
                let stream = derive::DeriveStream::new(instruments).await;
                let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
                let path = format!("recordings/derive_{}.jsonl", timestamp);
                std::fs::create_dir_all("recordings").ok();
                match backtest::RecordingStream::new(stream, &path) {
                    Ok(recording) => {
                        println!("Derive: Recording to {}", path);
                        engine = engine.with_stream(Exchange::Derive, Box::new(recording));
                    }
                    Err(_) => {
                        println!("Derive: Failed to create recording, skipping");
                    }
                }
            }
            Exchange::Polymarket => {
                let stream = polymarket::PolymarketStream::new(instruments).await;
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
                let path = format!("recordings/polymarket_{}.jsonl", timestamp);
                std::fs::create_dir_all("recordings").ok();
                match backtest::RecordingStream::new(stream, &path) {
                    Ok(recording) => {
                        println!("Polymarket: Recording to {}", path);
                        engine = engine.with_stream(Exchange::Polymarket, Box::new(recording));
                    }
                    Err(_) => {
                        println!("Polymarket: Failed to create recording, skipping");
                    }
                }
            }
        }
    }

    // Run the engine
    engine.run().await;
}

// =============================================================================
// Backtest Mode: Historical data playback
// =============================================================================

async fn run_backtest_mode(args: &Args) {
    let file_path = match &args.file {
        Some(p) => p,
        None => {
            eprintln!("--file is required for backtest mode");
            std::process::exit(1);
        }
    };

    println!("Starting Backtest from file: {}", file_path);

    // Create playback configuration
    let playback_config = if args.realtime {
        println!("Realtime playback enabled at {}x speed", args.speed);
        backtest::PlaybackConfig::realtime(args.speed)
    } else {
        backtest::PlaybackConfig::instant()
    };

    // Create the historical stream
    let stream = match backtest::HistoricalStream::with_config(file_path, playback_config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to open historical data file: {}", e);
            std::process::exit(1);
        }
    };

    // Create mock execution
    let mock_exec = backtest::MockExec::new().shared();
    let mut exec_clients = HashMap::new();
    exec_clients.insert(Exchange::Deribit, mock_exec.clone());
    exec_clients.insert(Exchange::Derive, mock_exec.clone());
    exec_clients.insert(Exchange::Polymarket, mock_exec.clone());
    let exec_router = Arc::new(ExecutionRouter::new(exec_clients));

    // Load strategies from config or use defaults
    let strategies: Vec<Arc<dyn Strategy>> = if let Some(config_path) = &args.config {
        match Config::from_file(config_path) {
            Ok(config) => config.build_strategies(exec_router.clone()),
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                default_backtest_strategies(exec_router.clone())
            }
        }
    } else {
        default_backtest_strategies(exec_router.clone())
    };

    println!("Running with {} strategies", strategies.len());

    // Start dashboard if requested
    if let Some(port) = args.dashboard {
        start_dashboard(strategies.clone(), port);
    }

    // Build and run engine
    // For backtest, we use a single stream
    #[allow(deprecated)]
    {
        use trading_bot::engine::MarketRouter;
    let router = MarketRouter::new(stream, strategies);
    router.run().await;
}

    println!("\nBacktest complete!");

    // Keep dashboard alive after backtest
    if args.dashboard.is_some() {
        wait_for_shutdown().await;
    }
}

fn default_backtest_strategies(exec_router: Arc<ExecutionRouter>) -> Vec<Arc<dyn Strategy>> {
    vec![
        GammaScalp::new(
            "GammaScalp-BTC",
            vec![Instrument::Deribit("BTC-29MAR24-60000-C".to_string())],
            exec_router.clone(),
        ),
        MomentumStrategy::new(
            "Momentum-ETH",
            vec![Instrument::Deribit("ETH-29MAR24-4000-C".to_string())],
            exec_router,
            3,
            0.01,
        ),
    ]
}

// =============================================================================
// Demo Mode: Quick test with mock data
// =============================================================================

async fn run_demo_mode(args: &Args) {
    println!("Starting Demo Mode with mock data...");

    // Create mock data
    let mock_data = vec![
        MarketEvent {
            timestamp: 1700000000,
            instrument: Instrument::Deribit("BTC-29MAR24-60000-C".to_string()),
            best_bid: Some(100.0),
            best_ask: Some(101.0),
            delta: Some(0.6),
        },
        MarketEvent {
            timestamp: 1700000001,
            instrument: Instrument::Deribit("ETH-29MAR24-4000-C".to_string()),
            best_bid: Some(50.0),
            best_ask: Some(51.0),
            delta: Some(0.3),
        },
        MarketEvent {
            timestamp: 1700000002,
            instrument: Instrument::Deribit("BTC-29MAR24-60000-C".to_string()),
            best_bid: Some(102.0),
            best_ask: Some(103.0),
            delta: Some(0.7),
        },
        MarketEvent {
            timestamp: 1700000003,
            instrument: Instrument::Deribit("ETH-29MAR24-4000-C".to_string()),
            best_bid: Some(52.0),
            best_ask: Some(53.0),
            delta: Some(0.35),
        },
    ];

    // Create mock execution
    let mock_exec = backtest::MockExec::new().shared();
    let mut exec_clients = HashMap::new();
    exec_clients.insert(Exchange::Deribit, mock_exec);
    let exec_router = Arc::new(ExecutionRouter::new(exec_clients));

    // Create strategies
    let strategies: Vec<Arc<dyn Strategy>> = vec![
        GammaScalp::new(
        "GammaScalp-BTC",
            vec![Instrument::Deribit("BTC-29MAR24-60000-C".to_string())],
            exec_router.clone(),
        ),
        MomentumStrategy::new(
        "Momentum-ETH",
            vec![Instrument::Deribit("ETH-29MAR24-4000-C".to_string())],
            exec_router,
            3,
        0.01,
        ),
    ];

    // Start dashboard if requested
    if let Some(port) = args.dashboard {
        start_dashboard(strategies.clone(), port);
    }

    // Run with backtest stream
    let stream = backtest::BacktestStream::new(mock_data);
    #[allow(deprecated)]
    {
        use trading_bot::engine::MarketRouter;
    let router = MarketRouter::new(stream, strategies);
    router.run().await;
    }

    println!("\nDemo complete!");

    if args.dashboard.is_some() {
        wait_for_shutdown().await;
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn start_dashboard(strategies: Vec<Arc<dyn Strategy>>, port: u16) {
    let dashboard = DashboardServer::new(strategies, port);
    tokio::spawn(async move {
        if let Err(e) = dashboard.run().await {
            eprintln!("Dashboard server error: {}", e);
        }
    });
    println!("Dashboard available at http://localhost:{}", port);
}

async fn wait_for_shutdown() {
    println!("Dashboard still running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    println!("\nShutting down...");
}
