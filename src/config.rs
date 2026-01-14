// src/config.rs
//
// Configuration file parsing for strategy-centric architecture.
// Supports TOML config files that specify strategies and their parameters.

use crate::catalog::{DeribitCatalog, DeriveCatalog, PolymarketCatalog};
use crate::engine::EngineConfig;
use crate::models::{Exchange, Instrument};
use crate::optimizer::KellyConfig;
use crate::strategy::{CrossMarketConfig, CrossMarketStrategy, GammaScalp, MomentumStrategy};
use crate::traits::{SharedExecutionRouter, Strategy};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Catalog References for Strategy Construction
// =============================================================================

/// Holds references to shared catalogs for strategy construction.
/// 
/// Catalogs are created once at startup and shared between the Engine
/// (for refresh coordination) and strategies (for market discovery).
/// 
/// Uses concrete types (`Arc<XxxCatalog>`) rather than trait objects
/// so catalogs can be used with both `Refreshable` (for Engine) and
/// type-specific methods (for strategies).
#[derive(Clone, Default)]
pub struct Catalogs {
    pub polymarket: Option<Arc<PolymarketCatalog>>,
    pub deribit: Option<Arc<DeribitCatalog>>,
    pub derive: Option<Arc<DeriveCatalog>>,
}

// =============================================================================
// Configuration Types
// =============================================================================

/// Root configuration structure.
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Global settings
    #[serde(default)]
    pub global: GlobalConfig,
    /// List of strategy configurations
    #[serde(default)]
    pub strategies: Vec<StrategyConfig>,
}

/// Global configuration settings.
#[derive(Debug, Default, Deserialize)]
pub struct GlobalConfig {
    /// Dashboard port (None = no dashboard)
    pub dashboard_port: Option<u16>,
    /// Log level
    pub log_level: Option<String>,
    /// Subscription refresh interval in seconds
    pub subscription_refresh_secs: Option<u64>,
}

/// Configuration for a single strategy.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StrategyConfig {
    GammaScalp(GammaScalpConfig),
    Momentum(MomentumConfig),
    CrossMarket(CrossMarketConfigFile),
}

/// Configuration for GammaScalp strategy.
#[derive(Debug, Deserialize)]
pub struct GammaScalpConfig {
    /// Unique name for this strategy instance
    pub name: String,
    /// Exchanges this strategy operates on
    #[serde(default)]
    pub exchanges: Vec<String>,
    /// Underlying asset (e.g., "BTC", "ETH")
    pub underlying: Option<String>,
    /// Specific instruments to subscribe to
    #[serde(default)]
    pub instruments: Vec<String>,
    /// Delta threshold for triggering signals
    #[serde(default = "default_delta_threshold")]
    pub threshold: f64,
}

fn default_delta_threshold() -> f64 {
    0.5
}

/// Configuration for Momentum strategy.
#[derive(Debug, Deserialize)]
pub struct MomentumConfig {
    /// Unique name for this strategy instance
    pub name: String,
    /// Exchanges this strategy operates on
    #[serde(default)]
    pub exchanges: Vec<String>,
    /// Underlying asset
    pub underlying: Option<String>,
    /// Specific instruments to subscribe to
    #[serde(default)]
    pub instruments: Vec<String>,
    /// Number of ticks for lookback period
    #[serde(default = "default_lookback_period")]
    pub lookback_period: usize,
    /// Minimum momentum percentage to trigger signal
    #[serde(default = "default_momentum_threshold")]
    pub momentum_threshold: f64,
}

fn default_lookback_period() -> usize {
    10
}

fn default_momentum_threshold() -> f64 {
    0.01
}

/// Configuration for CrossMarket strategy (from file).
#[derive(Debug, Deserialize)]
pub struct CrossMarketConfigFile {
    /// Unique name for this strategy instance
    pub name: String,
    /// Underlying currency (e.g., "BTC")
    #[serde(default = "default_currency")]
    pub currency: String,
    /// Minimum edge threshold for opportunities
    #[serde(default = "default_min_edge")]
    pub min_edge: f64,
    /// Maximum time to expiry in years
    #[serde(default = "default_max_time_to_expiry")]
    pub max_time_to_expiry: f64,
    /// Maximum expiry days for instrument discovery (default: 90)
    pub max_expiry_days: Option<u32>,
    /// Regex pattern for discovering Polymarket markets (default: "bitcoin-above-\\d+")
    #[serde(default = "default_polymarket_pattern")]
    pub polymarket_pattern: String,
    /// Initial wealth/bankroll for Kelly sizing
    #[serde(default = "default_initial_wealth")]
    pub initial_wealth: f64,
    /// Risk aversion parameter (1 = Kelly, >1 = more conservative)
    #[serde(default = "default_risk_aversion")]
    pub risk_aversion: f64,
    /// Number of Monte Carlo scenarios
    #[serde(default = "default_n_scenarios")]
    pub n_scenarios: usize,
    /// Polymarket markets to track (list of {condition_id, question, strike, expiry_timestamp, yes_token, no_token})
    #[serde(default)]
    pub polymarket_markets: Vec<PolymarketMarketConfig>,
    /// Vol time strategy: "calendar" (default) or "weighted"
    #[serde(default = "default_vol_time_strategy")]
    pub vol_time_strategy: String,
    /// Path to JSON file with 168 hourly volatility values (generated by generate_hourly_vols.py)
    /// Only used if vol_time_strategy is "weighted"
    pub hourly_vols_file: Option<String>,
    /// Regime scaler: recent volatility / long-term average (GARCH-lite)
    /// Only used if vol_time_strategy is "weighted"
    #[serde(default = "default_regime_scaler")]
    pub regime_scaler: f64,
}

/// Configuration for a Polymarket market to track.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketMarketConfig {
    pub condition_id: String,
    pub question: String,
    pub strike: f64,
    pub expiry_timestamp: i64,
    pub yes_token: String,
    pub no_token: String,
    /// Minimum shares for limit orders (typically 5 or 15)
    #[serde(default)]
    pub minimum_order_size: Option<f64>,
    /// Minimum price increment (typically 0.01 or 0.001)
    #[serde(default)]
    pub minimum_tick_size: Option<f64>,
}

fn default_currency() -> String {
    "BTC".to_string()
}

fn default_min_edge() -> f64 {
    0.02
}

fn default_polymarket_pattern() -> String {
    r"bitcoin-above-\d+".to_string()
}

fn default_max_time_to_expiry() -> f64 {
    0.5
}

fn default_initial_wealth() -> f64 {
    10000.0
}

fn default_risk_aversion() -> f64 {
    1.0
}

fn default_n_scenarios() -> usize {
    10000
}

fn default_vol_time_strategy() -> String {
    "calendar".to_string()
}

fn default_regime_scaler() -> f64 {
    1.0
}

// =============================================================================
// Configuration Loading
// =============================================================================

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let contents = fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;
        Self::from_str(&contents)
    }

    /// Parse configuration from a TOML string.
    pub fn from_str(s: &str) -> Result<Self, String> {
        toml::from_str(s).map_err(|e| format!("Failed to parse config: {}", e))
    }

    /// Builds an EngineConfig from global settings.
    pub fn engine_config(&self) -> EngineConfig {
        EngineConfig {
            subscription_refresh_interval: Duration::from_secs(
                self.global.subscription_refresh_secs.unwrap_or(60)
            ),
        }
    }

    /// Returns all unique exchanges required by configured strategies.
    pub fn required_exchanges(&self) -> HashSet<Exchange> {
        let mut exchanges = HashSet::new();
        for strategy in &self.strategies {
            match strategy {
                StrategyConfig::GammaScalp(cfg) => {
                    exchanges.extend(parse_exchanges(&cfg.exchanges));
                }
                StrategyConfig::Momentum(cfg) => {
                    exchanges.extend(parse_exchanges(&cfg.exchanges));
                }
                StrategyConfig::CrossMarket(_) => {
                    // CrossMarket always requires all three exchanges
                    exchanges.insert(Exchange::Deribit);
                    exchanges.insert(Exchange::Derive);
                    exchanges.insert(Exchange::Polymarket);
                }
            }
        }
        exchanges
    }

    /// Builds strategy instances from the configuration.
    /// 
    /// For strategies that need catalog access (like CrossMarket), pass catalogs.
    /// If catalogs is None, strategies will fall back to their legacy initialization.
    pub fn build_strategies(
        &self,
        exec_router: SharedExecutionRouter,
    ) -> Vec<Arc<dyn Strategy>> {
        self.build_strategies_with_catalogs(exec_router, None)
    }

    /// Builds strategy instances with optional catalog references.
    /// 
    /// When catalogs are provided, strategies use them for live market discovery
    /// instead of caching instruments at startup.
    pub fn build_strategies_with_catalogs(
        &self,
        exec_router: SharedExecutionRouter,
        catalogs: Option<&Catalogs>,
    ) -> Vec<Arc<dyn Strategy>> {
        self.strategies
            .iter()
            .filter_map(|cfg| match cfg {
                StrategyConfig::GammaScalp(c) => Some(build_gamma_scalp(c, exec_router.clone())),
                StrategyConfig::Momentum(c) => Some(build_momentum(c, exec_router.clone())),
                StrategyConfig::CrossMarket(c) => {
                    Some(build_cross_market(c, exec_router.clone(), catalogs))
                }
            })
            .collect()
    }
}

// =============================================================================
// Strategy Builders
// =============================================================================

fn parse_exchanges(exchanges: &[String]) -> Vec<Exchange> {
    exchanges
        .iter()
        .filter_map(|s| match s.to_lowercase().as_str() {
            "deribit" => Some(Exchange::Deribit),
            "polymarket" | "poly" => Some(Exchange::Polymarket),
            "derive" | "lyra" => Some(Exchange::Derive),
            _ => {
                log::warn!("Unknown exchange: {}", s);
                None
            }
        })
        .collect()
}

fn parse_instruments(instruments: &[String], exchanges: &[String]) -> Vec<Instrument> {
    let parsed_exchanges = parse_exchanges(exchanges);
    let default_exchange = parsed_exchanges.first().copied().unwrap_or(Exchange::Deribit);

    instruments
        .iter()
        .map(|s| {
            // Try to infer exchange from instrument format
            if s.chars().all(|c| c.is_numeric()) && s.len() > 30 {
                Instrument::Polymarket(s.clone())
            } else if parsed_exchanges.contains(&Exchange::Derive) {
                Instrument::Derive(s.clone())
            } else {
                match default_exchange {
                    Exchange::Deribit => Instrument::Deribit(s.clone()),
                    Exchange::Polymarket => Instrument::Polymarket(s.clone()),
                    Exchange::Derive => Instrument::Derive(s.clone()),
                }
            }
        })
        .collect()
}

fn build_gamma_scalp(
    config: &GammaScalpConfig,
    exec_router: SharedExecutionRouter,
) -> Arc<dyn Strategy> {
    let instruments = parse_instruments(&config.instruments, &config.exchanges);
    GammaScalp::with_threshold(&config.name, instruments, exec_router, config.threshold)
}

fn build_momentum(
    config: &MomentumConfig,
    exec_router: SharedExecutionRouter,
) -> Arc<dyn Strategy> {
    let instruments = parse_instruments(&config.instruments, &config.exchanges);
    MomentumStrategy::new(
        &config.name,
        instruments,
        exec_router,
        config.lookback_period,
        config.momentum_threshold,
    )
}

fn build_cross_market(
    config: &CrossMarketConfigFile,
    exec_router: SharedExecutionRouter,
    catalogs: Option<&Catalogs>,
) -> Arc<dyn Strategy> {
    use crate::optimizer::{ScannerConfig, UtilityFunction};

    let kelly_config = KellyConfig {
        utility: if (config.risk_aversion - 1.0).abs() < 0.01 {
            UtilityFunction::Log
        } else {
            UtilityFunction::CRRA { gamma: config.risk_aversion }
        },
        n_scenarios: config.n_scenarios,
        initial_wealth: config.initial_wealth,
        ..Default::default()
    };

    let scanner_config = ScannerConfig {
        min_edge: config.min_edge,
        max_time_to_expiry: config.max_time_to_expiry,
        ..Default::default()
    };

    // Load hourly volatility values from JSON file if specified
    let hourly_vols = if let Some(ref path) = config.hourly_vols_file {
        match fs::read_to_string(path) {
            Ok(contents) => {
                match serde_json::from_str::<Vec<f64>>(&contents) {
                    Ok(vols) => {
                        if vols.len() == 168 {
                            log::info!("Loaded {} hourly volatility values from {}", vols.len(), path);
                            vols
                        } else {
                            log::warn!("Warning: {} contains {} values, expected 168. Using calendar time.", path, vols.len());
                            Vec::new()
                        }
                    }
                    Err(e) => {
                        log::warn!("Warning: Failed to parse {}: {}. Using calendar time.", path, e);
                        Vec::new()
                    }
                }
            }
            Err(e) => {
                log::warn!("Warning: Failed to read {}: {}. Using calendar time.", path, e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let cross_market_config = CrossMarketConfig {
        currency: config.currency.clone(),
        min_edge: config.min_edge,
        max_time_to_expiry: config.max_time_to_expiry,
        max_expiry_days: config.max_expiry_days.unwrap_or(90),
        rate: 0.0,
        kelly_config,
        scanner_config,
        polymarket_pattern: config.polymarket_pattern.clone(),
        recalc_interval_secs: 60,
        vol_time_strategy: config.vol_time_strategy.clone(),
        hourly_vols,
        regime_scaler: config.regime_scaler,
    };

    // If catalogs are provided, use them for live discovery (preferred path)
    // Otherwise, fall back to legacy blocking initialization
    let strategy = if let Some(cats) = catalogs {
        CrossMarketStrategy::with_catalogs(
            &config.name,
            cross_market_config,
            exec_router,
            cats.polymarket.clone(),
            cats.deribit.clone(),
            cats.derive.clone(),
        )
    } else {
        // Legacy path: blocking initialization
        let max_expiry_days = cross_market_config.max_expiry_days;
        let strategy = CrossMarketStrategy::new(&config.name, cross_market_config, exec_router);

        // Initialize subscriptions from exchange APIs (blocking)
        let strategy_clone = strategy.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime");
            rt.block_on(async {
                strategy_clone.initialize_subscriptions(max_expiry_days).await;
            });
        }).join().expect("Failed to initialize subscriptions");

        strategy
    };

    // Register Polymarket markets from config (blocking to ensure registration
    // completes before the strategy starts receiving events)
    // Note: This is only for manually-specified markets in config, not catalog discovery
    if !config.polymarket_markets.is_empty() {
        let strategy_clone = strategy.clone();
        let markets = config.polymarket_markets.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime");
            rt.block_on(async {
                for market in &markets {
                    strategy_clone.register_polymarket_market(
                        &market.condition_id,
                        &market.question,
                        market.strike,
                        market.expiry_timestamp,
                        &market.yes_token,
                        &market.no_token,
                        market.minimum_order_size,
                        market.minimum_tick_size,
                    ).await;
                }
            });
        }).join().expect("Failed to register Polymarket markets");
    }

    strategy
}

// =============================================================================
// Default Configuration
// =============================================================================

/// Returns a default configuration string for documentation.
pub fn default_config_template() -> &'static str {
    r#"# Trading Bot Configuration
# 
# This file configures which strategies to run and their parameters.
# The engine will automatically connect to exchanges required by the strategies.

[global]
# Web dashboard port (optional)
dashboard_port = 8080

# Subscription refresh interval in seconds
subscription_refresh_secs = 60

# =============================================================================
# CROSS-MARKET STRATEGY (Recommended for BTC options arbitrage)
# Uses Deribit IV as source of truth, hunts mispricings on Polymarket & Derive
# =============================================================================

[[strategies]]
type = "cross_market"
name = "CrossMarket-BTC"
currency = "BTC"
min_edge = 0.02                # 2% minimum edge to consider
max_time_to_expiry = 0.5       # 6 months max
initial_wealth = 10000.0       # Bankroll for Kelly sizing
risk_aversion = 1.0            # 1.0 = Kelly, >1 = more conservative
n_scenarios = 10000            # Monte Carlo scenarios

# Polymarket markets to track
[[strategies.polymarket_markets]]
condition_id = "0x..."         # Replace with actual condition ID
question = "Will BTC be above $100k on Dec 31?"
strike = 100000.0
expiry_timestamp = 1735689600000  # Dec 31, 2024 00:00 UTC (ms)
yes_token = "yes_token_id..."
no_token = "no_token_id..."

# =============================================================================
# ALTERNATIVE STRATEGIES
# =============================================================================

# Example: GammaScalp strategy
# [[strategies]]
# type = "gamma_scalp"
# name = "GammaScalp-BTC"
# exchanges = ["deribit"]
# instruments = ["BTC-29MAR24-60000-C", "BTC-29MAR24-70000-C"]
# threshold = 0.5

# Example: Momentum strategy
# [[strategies]]
# type = "momentum"
# name = "Momentum-ETH"
# exchanges = ["deribit"]
# instruments = ["ETH-29MAR24-4000-C"]
# lookback_period = 10
# momentum_threshold = 0.02
"#
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_config() {
        let config_str = r#"
            [global]
            dashboard_port = 8080

            [[strategies]]
            type = "gamma_scalp"
            name = "Test-GammaScalp"
            exchanges = ["deribit"]
            instruments = ["BTC-29MAR24-60000-C"]
            threshold = 0.6
        "#;

        let config = Config::from_str(config_str).unwrap();
        assert_eq!(config.global.dashboard_port, Some(8080));
        assert_eq!(config.strategies.len(), 1);

        match &config.strategies[0] {
            StrategyConfig::GammaScalp(c) => {
                assert_eq!(c.name, "Test-GammaScalp");
                assert_eq!(c.threshold, 0.6);
            }
            _ => panic!("Expected GammaScalp strategy"),
        }
    }

    #[test]
    fn test_parse_exchanges() {
        let exchanges = parse_exchanges(&[
            "deribit".to_string(),
            "polymarket".to_string(),
            "derive".to_string(),
        ]);
        assert_eq!(exchanges.len(), 3);
        assert!(exchanges.contains(&Exchange::Deribit));
        assert!(exchanges.contains(&Exchange::Polymarket));
        assert!(exchanges.contains(&Exchange::Derive));
    }

    #[test]
    fn test_required_exchanges() {
        let config_str = r#"
            [[strategies]]
            type = "gamma_scalp"
            name = "Test1"
            exchanges = ["deribit"]
            instruments = []

            [[strategies]]
            type = "momentum"
            name = "Test2"
            exchanges = ["polymarket", "derive"]
            instruments = []
        "#;

        let config = Config::from_str(config_str).unwrap();
        let exchanges = config.required_exchanges();
        assert_eq!(exchanges.len(), 3);
    }
}

