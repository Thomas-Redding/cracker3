// src/config.rs
//
// Configuration file parsing for strategy-centric architecture.
// Supports TOML config files that specify strategies and their parameters.

use crate::models::{Exchange, Instrument};
use crate::strategy::{GammaScalp, MomentumStrategy};
use crate::traits::{SharedExecutionRouter, Strategy};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

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
            }
        }
        exchanges
    }

    /// Builds strategy instances from the configuration.
    pub fn build_strategies(
        &self,
        exec_router: SharedExecutionRouter,
    ) -> Vec<Arc<dyn Strategy>> {
        self.strategies
            .iter()
            .filter_map(|cfg| match cfg {
                StrategyConfig::GammaScalp(c) => Some(build_gamma_scalp(c, exec_router.clone())),
                StrategyConfig::Momentum(c) => Some(build_momentum(c, exec_router.clone())),
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

# Strategies to run
[[strategies]]
type = "gamma_scalp"
name = "GammaScalp-BTC"
exchanges = ["deribit"]
instruments = ["BTC-29MAR24-60000-C", "BTC-29MAR24-70000-C"]
threshold = 0.5

[[strategies]]
type = "momentum"
name = "Momentum-ETH"
exchanges = ["deribit"]
instruments = ["ETH-29MAR24-4000-C"]
lookback_period = 10
momentum_threshold = 0.02

# Example: Cross-exchange strategy
# [[strategies]]
# type = "gamma_scalp"
# name = "GammaScalp-Derive"
# exchanges = ["derive"]
# instruments = ["BTC-20251226-100000-C"]
# threshold = 0.3

# Example: Polymarket strategy
# [[strategies]]
# type = "momentum"
# name = "Momentum-Poly"
# exchanges = ["polymarket"]
# instruments = ["21742633143463906290569050155826241533067272736897614950488156847949938836455"]
# lookback_period = 5
# momentum_threshold = 0.01
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

