// src/main.rs
//
// Strategy-centric trading engine with multi-exchange support.
// Strategies declare which exchanges they need, and the engine auto-connects.

use chrono::Utc;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use trading_bot::catalog::{DeribitCatalog, DeriveCatalog, PolymarketCatalog, SharedRefreshable};
use trading_bot::config::{default_config_template, Catalogs, Config};
use trading_bot::connectors::{backtest, deribit, derive, polymarket};
use trading_bot::dashboard::DashboardServer;
use trading_bot::engine::Engine;
use trading_bot::models::{Exchange, Instrument, MarketEvent};
use trading_bot::strategy::{CrossMarketStrategy, GammaScalp, MomentumStrategy};
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

    /// Path to historical data files (JSONL) for backtest mode
    #[arg(long)]
    file: Vec<String>,

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
    // API keys are optional for read-only mode (market data streaming)
    // They're only needed for actual trading
    let mut exec_clients: HashMap<Exchange, SharedExecutionClient> = HashMap::new();
    
    // Keep a reference to Polymarket exec for catalog injection later
    let mut polymarket_exec: Option<polymarket::PolymarketExec> = None;

    for exchange in &required_exchanges {
        match exchange {
            Exchange::Deribit => {
                let api_key = std::env::var("DERIBIT_KEY")
                    .unwrap_or_else(|_| {
                        println!("Note: DERIBIT_KEY not set - trading disabled, read-only mode");
                        "read_only".to_string()
                    });
                let client = deribit::DeribitExec::new(api_key).await.shared();
                exec_clients.insert(Exchange::Deribit, client);
            }
            Exchange::Derive => {
                let api_key = std::env::var("DERIVE_KEY")
                    .unwrap_or_else(|_| {
                        println!("Note: DERIVE_KEY not set - trading disabled, read-only mode");
                        "read_only".to_string()
                    });
                let client = derive::DeriveExec::new(api_key).await.shared();
                exec_clients.insert(Exchange::Derive, client);
            }
            Exchange::Polymarket => {
                // Polymarket requires a private key for order signing (EIP-712)
                // Use POLYMARKET_PRIVATE_KEY env var (hex string, with or without 0x prefix)
                let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
                    .unwrap_or_else(|_| {
                        println!("Note: POLYMARKET_PRIVATE_KEY not set - trading disabled, read-only mode");
                        "read_only".to_string()
                    });
                // Keep a reference to inject catalog later (shares inner Arc)
                let exec = polymarket::PolymarketExec::new(private_key).await;
                polymarket_exec = Some(exec.clone());
                let client = exec.shared();
                exec_clients.insert(Exchange::Polymarket, client);
            }
        }
    }

    // Build execution router
    let exec_router = Arc::new(ExecutionRouter::new(exec_clients));

    // Create catalogs for market discovery
    // These are shared between Engine (for refresh coordination) and strategies (for discovery)
    println!("Initializing catalogs...");
    let mut catalogs = Catalogs::default();
    
    if required_exchanges.contains(&Exchange::Polymarket) {
        let catalog = PolymarketCatalog::new(None, None).await;
        // Inject catalog into exec client for order validation
        if let Some(ref exec) = polymarket_exec {
            exec.set_catalog(catalog.clone()).await;
        }
        catalogs.polymarket = Some(catalog);
        println!("Polymarket catalog initialized");
    }
    if required_exchanges.contains(&Exchange::Deribit) {
        // DeribitCatalog needs currencies - extract from config or default to BTC
        let catalog = DeribitCatalog::new(vec!["BTC".to_string()], None, None).await;
        catalogs.deribit = Some(catalog);
        println!("Deribit catalog initialized");
    }
    if required_exchanges.contains(&Exchange::Derive) {
        let catalog = DeriveCatalog::new(vec!["BTC".to_string()], None, None).await;
        catalogs.derive = Some(catalog);
        println!("Derive catalog initialized");
    }

    // Build strategies from config with catalog references
    let strategies = config.build_strategies_with_catalogs(exec_router.clone(), Some(&catalogs));
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

    // Build engine with streams and catalogs for each exchange
    let mut engine = Engine::new(strategies);
    engine = engine.with_exec_router(exec_router);

    // Add all catalogs to engine for coordinated refresh
    // All catalogs implement Refreshable, so they can be passed as SharedRefreshable
    if let Some(catalog) = &catalogs.polymarket {
        engine = engine.with_catalog(Exchange::Polymarket, catalog.clone() as SharedRefreshable);
    }
    if let Some(catalog) = &catalogs.deribit {
        engine = engine.with_catalog(Exchange::Deribit, catalog.clone() as SharedRefreshable);
    }
    if let Some(catalog) = &catalogs.derive {
        engine = engine.with_catalog(Exchange::Derive, catalog.clone() as SharedRefreshable);
    }

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
    if args.file.is_empty() {
        eprintln!("--file is required for backtest mode");
        std::process::exit(1);
    }
    
    let file_paths = &args.file;

    println!("Starting Backtest from files: {:?}", file_paths);

    // Create playback configuration
    let playback_config = if args.realtime {
        println!("Realtime playback enabled at {}x speed", args.speed);
        backtest::PlaybackConfig::realtime(args.speed)
    } else {
        backtest::PlaybackConfig::instant()
    };

    // Create the historical stream
    let stream = match backtest::HistoricalStream::with_config(file_paths.clone(), playback_config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to open historical data file: {}", e);
            std::process::exit(1);
        }
    };

    // Create mock execution (keep a typed reference for reporting)
    let mock_exec_raw = backtest::MockExec::new();
    let mock_exec = mock_exec_raw.clone().shared(); // For engine use
    
    let mut exec_clients = HashMap::new();
    exec_clients.insert(Exchange::Deribit, mock_exec.clone());
    exec_clients.insert(Exchange::Derive, mock_exec.clone());
    exec_clients.insert(Exchange::Polymarket, mock_exec.clone());
    let exec_router = Arc::new(ExecutionRouter::new(exec_clients));

    // Initialize catalogs to allow strategies to discover markets
    // This uses the local cache files (e.g. cache/polymarket_markets.jsonl)
    let mut catalogs = Catalogs::default();
    
    // We instantiate all catalogs unconditionally for backtest flexibility
    let poly_catalog = PolymarketCatalog::new(None, None).await;
    catalogs.polymarket = Some(poly_catalog.clone());
    
    let deribit_catalog = DeribitCatalog::new(vec!["BTC".to_string()], None, None).await;
    catalogs.deribit = Some(deribit_catalog.clone());

    let derive_catalog = DeriveCatalog::new(vec!["BTC".to_string()], None, None).await;
    catalogs.derive = Some(derive_catalog.clone());

    // Load strategies from config or use defaults
    let strategies: Vec<Arc<dyn Strategy>> = if let Some(config_path) = &args.config {
        match Config::from_file(config_path) {
            Ok(config) => config.build_strategies_with_catalogs(exec_router.clone(), Some(&catalogs)),
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                default_backtest_strategies(exec_router.clone())
            }
        }
    } else {
        default_backtest_strategies(exec_router.clone())
    };

    let strategies_clone = strategies.clone(); // Keep for reporting
    println!("Running with {} strategies", strategies.len());

    // Start dashboard if requested
    if let Some(port) = args.dashboard {
        start_dashboard(strategies.clone(), port);
    }

    // Build and run engine
    let engine = Engine::new(strategies)
        .with_stream(Exchange::Deribit, Box::new(stream))
        .with_catalog(Exchange::Polymarket, poly_catalog as SharedRefreshable)
        .with_catalog(Exchange::Deribit, deribit_catalog as SharedRefreshable)
        .with_catalog(Exchange::Derive, derive_catalog as SharedRefreshable)
        .with_exec_router(exec_router);
    engine.run().await;

    println!("\n=== Backtest Results ===");

    // Report Strategy States (Positions & PnL)
    for strategy in strategies_clone {
        println!("\nStrategy: {}", strategy.name());
        let state = strategy.dashboard_state().await;
        
        if let Some(stats) = state.get("portfolio_stats") {
            if !stats.is_null() {
                println!("  Portfolio Stats:");
                if let Some(ret) = stats.get("expected_return") {
                    println!("    Expected Return: {}", ret);
                }
                if let Some(sharpe) = stats.get("expected_sharpe") {
                    println!("    Expected Sharpe: {}", sharpe);
                }
                if let Some(prob_loss) = stats.get("prob_loss") {
                    println!("    Prob. Loss:      {}", prob_loss);
                }
            }
        }

        if let Some(positions) = state.get("positions") {
            if let Some(pos_array) = positions.as_array() {
                if !pos_array.is_empty() {
                    println!("  Open Positions: {}", pos_array.len());
                    for pos in pos_array {
                        let opp_id = pos.get("opportunity_id").and_then(|v| v.as_str()).unwrap_or("?");
                        let size = pos.get("size").and_then(|v| v.as_str()).unwrap_or("?");
                        let value = pos.get("dollar_value").and_then(|v| v.as_str()).unwrap_or("?");
                        let profit = pos.get("expected_profit").and_then(|v| v.as_str()).unwrap_or("?");
                        
                        println!("    - {} : Size {} | Val {} | E[Profit] {}", 
                            opp_id, size, value, profit);
                    }
                } else {
                    println!("  No open positions.");
                }
            }
        }
    }

    // Report Execution Log
    let fills = mock_exec_raw.get_fills().await;
    println!("\nExecution Summary:");
    println!("  Total Trades: {}", fills.len());
    
    // Aggregate volume by exchange
    let mut volume_by_exchange: HashMap<String, f64> = HashMap::new();
    for fill in &fills {
        let exchange = format!("{:?}", fill.instrument.exchange());
        let val = fill.quantity * fill.price.unwrap_or(0.0);
        *volume_by_exchange.entry(exchange).or_default() += val;
    }
    
    for (exc, vol) in volume_by_exchange {
        println!("  {}: ${:.2} volume", exc, vol);
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

    // Create mock execution for all exchanges
    let mock_exec = backtest::MockExec::new().shared();
    let mut exec_clients = HashMap::new();
    exec_clients.insert(Exchange::Deribit, mock_exec.clone());
    exec_clients.insert(Exchange::Derive, mock_exec.clone());
    exec_clients.insert(Exchange::Polymarket, mock_exec.clone());
    let exec_router = Arc::new(ExecutionRouter::new(exec_clients));

    // Create CrossMarket strategy
    let cross_market = CrossMarketStrategy::with_defaults("CrossMarket-BTC", exec_router.clone());

    // Register a mock Polymarket market
    let expiry_ms = chrono::Utc::now().timestamp_millis() + (90 * 24 * 3600 * 1000); // 90 days from now
    cross_market.register_polymarket_market(
        "demo_condition_123",
        "Will BTC be above $100k by expiry?",
        100_000.0,
        expiry_ms,
        "demo_yes_token",
        "demo_no_token",
        Some(15.0),  // typical minimum_order_size
        Some(0.01),  // typical minimum_tick_size
    ).await;

    // Update with mock prices (underpriced YES = opportunity)
    cross_market.update_polymarket_prices(
        "demo_condition_123",
        0.35,   // YES price (market thinks 35%)
        0.65,   // NO price
        1000.0, // YES liquidity
        1000.0, // NO liquidity
    ).await;

    // Create mock market data simulating Deribit options with IV
    // NOTE: IVs must be in decimal form (0.55 = 55%), not percentage form (55.0),
    // because DeribitStream normalizes IVs via normalize_ivs() before creating MarketEvents,
    // and the vol surface filter rejects iv > 5.0 (500%).
    let mock_data = vec![
        MarketEvent {
            timestamp: 1700000000,
            instrument: Instrument::Deribit("BTC-29MAR24-60000-C".to_string()),
            best_bid: Some(0.05),
            best_ask: Some(0.055),
            delta: Some(0.6),
            mark_iv: Some(0.55),  // 55% IV (decimal form)
            bid_iv: Some(0.54),
            ask_iv: Some(0.56),
            underlying_price: Some(95000.0),
        },
        MarketEvent {
            timestamp: 1700000001,
            instrument: Instrument::Deribit("BTC-29MAR24-100000-C".to_string()),
            best_bid: Some(0.02),
            best_ask: Some(0.025),
            delta: Some(0.3),
            mark_iv: Some(0.60),  // 60% IV (OTM options have higher IV)
            bid_iv: Some(0.59),
            ask_iv: Some(0.61),
            underlying_price: Some(95000.0),
        },
        MarketEvent {
            timestamp: 1700000002,
            instrument: Instrument::Derive("BTC-20250329-100000-C".to_string()),
            best_bid: Some(0.018),
            best_ask: Some(0.022),
            delta: Some(0.28),
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            underlying_price: Some(95000.0),
        },
        MarketEvent {
            timestamp: 1700000003,
            instrument: Instrument::Polymarket("demo_yes_token".to_string()),
            best_bid: Some(0.35),
            best_ask: Some(0.36),
            delta: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            underlying_price: None,
        },
    ];

    // Create strategies including the new CrossMarket
    let strategies: Vec<Arc<dyn Strategy>> = vec![
        cross_market,
        GammaScalp::new(
            "GammaScalp-BTC",
            vec![Instrument::Deribit("BTC-29MAR24-60000-C".to_string())],
            exec_router.clone(),
        ),
        MomentumStrategy::new(
            "Momentum-ETH",
            vec![Instrument::Deribit("ETH-29MAR24-4000-C".to_string())],
            exec_router.clone(),
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
    let engine = Engine::new(strategies)
        .with_stream(Exchange::Deribit, Box::new(stream))
        .with_exec_router(exec_router);
    engine.run().await;

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
