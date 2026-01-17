// src/main.rs
//
// Strategy-centric trading engine with multi-exchange support.
// Strategies declare which exchanges they need, and the engine auto-connects.

use chrono::Utc;
use clap::Parser;
use log::{info, warn, error};
use std::collections::HashMap;
use std::sync::Arc;
use trading_bot::catalog::{DeribitCatalog, DeriveCatalog, PolymarketCatalog, SharedRefreshable};
use trading_bot::config::{default_config_template, Catalogs, Config};
use trading_bot::connectors::{backtest as backtest_connector, deribit, derive, polymarket};
use trading_bot::backtest; // This is the runner module
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
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
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
            error!("Unknown mode: {}. Use: live, backtest, or demo", args.mode);
            std::process::exit(1);
        }
    }
}

// =============================================================================
// Live Mode: Config-driven multi-exchange trading
// =============================================================================

async fn run_live_mode(args: &Args) {
    info!("Starting Live Trading Mode...");

    // Load configuration
    let config = match &args.config {
        Some(path) => match Config::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to load config: {}", e);
                error!("Use --generate-config to create a template.");
                std::process::exit(1);
            }
        },
        None => {
            error!("No config file specified. Use --config <path>");
            error!("Use --generate-config to create a template.");
            std::process::exit(1);
        }
    };

    // Determine required exchanges
    let required_exchanges = config.required_exchanges();
    info!("Required exchanges: {:?}", required_exchanges);

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
                        info!("Note: DERIBIT_KEY not set - trading disabled, read-only mode");
                        "read_only".to_string()
                    });
                let client = deribit::DeribitExec::new(api_key).await.shared();
                exec_clients.insert(Exchange::Deribit, client);
            }
            Exchange::Derive => {
                let api_key = std::env::var("DERIVE_KEY")
                    .unwrap_or_else(|_| {
                        info!("Note: DERIVE_KEY not set - trading disabled, read-only mode");
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
                        info!("Note: POLYMARKET_PRIVATE_KEY not set - trading disabled, read-only mode");
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
    info!("Initializing catalogs...");
    let mut catalogs = Catalogs::default();
    
    if required_exchanges.contains(&Exchange::Polymarket) {
        let catalog = PolymarketCatalog::new(None, None).await;
        // Inject catalog into exec client for order validation
        if let Some(ref exec) = polymarket_exec {
            exec.set_catalog(catalog.clone()).await;
        }
        catalogs.polymarket = Some(catalog);
        info!("Polymarket catalog initialized");
    }
    if required_exchanges.contains(&Exchange::Deribit) {
        // DeribitCatalog needs currencies - extract from config or default to BTC
        let catalog = DeribitCatalog::new(vec!["BTC".to_string()], None, None).await;
        catalogs.deribit = Some(catalog);
        info!("Deribit catalog initialized");
    }
    if required_exchanges.contains(&Exchange::Derive) {
        let catalog = DeriveCatalog::new(vec!["BTC".to_string()], None, None).await;
        catalogs.derive = Some(catalog);
        info!("Derive catalog initialized");
    }

    // Build strategies from config with catalog references
    let strategies = config.build_strategies_with_catalogs(exec_router.clone(), Some(&catalogs));
    info!("Loaded {} strategies", strategies.len());

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
            info!("No instruments for {:?}, skipping stream", exchange);
            continue;
        }

        info!("{:?}: Subscribing to {} instruments", exchange, instruments.len());

        match exchange {
            Exchange::Deribit => {
                let stream = deribit::DeribitStream::new(instruments).await;
                // Wrap with recording
                let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
                let path = format!("recordings/deribit_{}.jsonl", timestamp);
                std::fs::create_dir_all("recordings").ok();
                match backtest_connector::RecordingStream::new(stream, &path) {
                    Ok(recording) => {
                        info!("Deribit: Recording to {}", path);
                        engine = engine.with_stream(Exchange::Deribit, Box::new(recording));
                    }
                    Err(_) => {
                        // Can't record, skip this exchange
                        error!("Deribit: Failed to create recording, skipping");
                    }
                }
            }
            Exchange::Derive => {
                let stream = derive::DeriveStream::new(instruments).await;
                let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
                let path = format!("recordings/derive_{}.jsonl", timestamp);
                std::fs::create_dir_all("recordings").ok();
                match backtest_connector::RecordingStream::new(stream, &path) {
                    Ok(recording) => {
                        info!("Derive: Recording to {}", path);
                        engine = engine.with_stream(Exchange::Derive, Box::new(recording));
                    }
                    Err(_) => {
                        error!("Derive: Failed to create recording, skipping");
                    }
                }
            }
            Exchange::Polymarket => {
                let stream = polymarket::PolymarketStream::new(instruments).await;
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
                let path = format!("recordings/polymarket_{}.jsonl", timestamp);
                std::fs::create_dir_all("recordings").ok();
                match backtest_connector::RecordingStream::new(stream, &path) {
                    Ok(recording) => {
                        info!("Polymarket: Recording to {}", path);
                        engine = engine.with_stream(Exchange::Polymarket, Box::new(recording));
                    }
                    Err(_) => {
                        error!("Polymarket: Failed to create recording, skipping");
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
        error!("--file is required for backtest mode");
        std::process::exit(1);
    }
    
    // Config
    let config = backtest::BacktestConfig {
        initial_cash: 100_000.0,
        data_paths: args.file.clone(),
        start_time: None,
        end_time: None,
        realtime: args.realtime,
        speed: args.speed,
    };

    // Initialize Catalogs (Async setup required outside the closure)
    let mut catalogs = Catalogs::default();
    catalogs.polymarket = Some(PolymarketCatalog::new(None, None).await);
    catalogs.deribit = Some(DeribitCatalog::new(vec!["BTC".to_string()], None, None).await);
    catalogs.derive = Some(DeriveCatalog::new(vec!["BTC".to_string()], None, None).await);
    
    let config_path = args.config.clone();
    let dashboard_port = args.dashboard; // Capture dashboard port before move

    // Run Backtest
    let result = backtest::BacktestRunner::run(config, move |exec_router| {
        // Strategy Factory Closure
        let strategies = if let Some(path) = &config_path {
             match Config::from_file(path) {
                Ok(c) => c.build_strategies_with_catalogs(exec_router, Some(&catalogs)),
                Err(e) => {
                    error!("Failed to load config: {}", e);
                    default_backtest_strategies(exec_router)
                }
             }
        } else {
             default_backtest_strategies(exec_router)
        };
        strategies
    }).await;
    
    // Start Dashboard if requested (in separate task, as BacktestRunner is async blocking loop effectively)
    if let Some(port) = dashboard_port {
        // We need strategies for dashboard, but strategies are created INSIDE runner via factory.
        // The runner returns strategies at the END. 
        // If we want dashboard DURING backtest, we need a way to access them.
        // `BacktestRunner` doesn't expose them during run.
        // However, `simulated_execution` and `strategies` state would be updated.
        // If we want live updates, we'd need to spawn the runner in a task and pass strategies shared handle?
        // OR the runner should spawn the dashboard if configured?
        
        // For now, simpler fix: Just running dashboard AFTER backtest (as before?)
        // The previous comment says "keep the dashboard alive after backtest completion".
        // So we just need to start it AFTER (using returned strategies) and wait.
        // OR if we want it during, we have a problem structure. 
        // Let's assume Post-Backtest Dashboard for analysis.
    }

    match result {
        Ok((res, strategies)) => {
            info!("=== Backtest Completed ===");
            info!("Final Equity: ${:.2}", res.final_equity);
            info!("Total PnL:    ${:.2}", res.total_pnl);
            info!("Trades:       {}", res.trade_count);
            
            // Start Dashboard if requested to view final state
            if let Some(port) = dashboard_port {
                info!("Starting dashboard on port {} to view results...", port);
                start_dashboard(strategies.clone(), port);
            }
            
            // Export PnL History
            info!("\nExporting PnL History...");
            std::fs::create_dir_all("backtest_results").unwrap_or_default();
            
            // We now have history directly in result for the overall portfolio
            if !res.history.is_empty() {
                let filename = "backtest_results/portfolio_history.csv";
                let mut content = "timestamp,total_equity,unrealized_pnl,realized_pnl,positions_count\n".to_string();
                
                for point in &res.history {
                     content.push_str(&format!("{},{:.2},{:.2},{:.2},{}\n", 
                        point.timestamp, 
                        point.total_equity, 
                        point.unrealized_pnl, 
                        point.realized_pnl, 
                        point.positions_count
                     ));
                }
                
                if let Ok(_) = std::fs::write(filename, content) {
                    info!("  Saved portfolio history to: {}", filename);
                } else {
                    error!("  Failed to write history to: {}", filename);
                }
            } else {
                warn!("No history data collected.");
            }
        }
        Err(e) => {
            error!("Backtest Failed: {}", e);
        }
    }

    // Keep alive if dashboard is running
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
    info!("Starting Demo Mode with mock data...");

    // Create mock execution (keep a typed reference for reporting)
    let mock_exec = backtest_connector::MockExec::new().shared();
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
    let stream = backtest_connector::BacktestStream::new(mock_data);
    let engine = Engine::new(strategies)
        .with_stream(Exchange::Deribit, Box::new(stream))
        .with_exec_router(exec_router);
    engine.run().await;

    info!("\nDemo complete!");

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
            error!("Dashboard server error: {}", e);
        }
    });
    info!("Dashboard available at http://localhost:{}", port);
}

async fn wait_for_shutdown() {
    info!("Dashboard still running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    info!("\nShutting down...");
}
