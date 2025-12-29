// src/strategy/momentum.rs

use crate::models::MarketEvent;
use crate::traits::{Dashboard, DashboardSchema, SharedExecutionClient, Strategy, Widget};
use async_trait::async_trait;
use log::info;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Maximum log entries to keep in memory for the dashboard.
const MAX_LOG_ENTRIES: usize = 100;

/// MomentumStrategy: a simple strategy that tracks price momentum.
///
/// It watches for sustained price movement in one direction and generates
/// signals when momentum is detected.
pub struct MomentumStrategy {
    name: String,
    instruments: Vec<String>,
    /// Execution client for placing orders (will be used when order logic is enabled)
    #[allow(dead_code)]
    exec: SharedExecutionClient,
    state: Mutex<MomentumState>,
}

struct MomentumState {
    /// Rolling window of mid prices
    price_history: VecDeque<f64>,
    /// Number of ticks to track
    lookback_period: usize,
    /// Minimum price change (%) to trigger a signal
    momentum_threshold: f64,
    /// Current calculated momentum
    current_momentum: Option<f64>,
    /// Recent log entries for the dashboard
    log: VecDeque<LogEntry>,
    /// Count of signals triggered
    signal_count: u64,
    /// Last signal direction
    last_signal: Option<String>,
}

#[derive(Clone, serde::Serialize)]
struct LogEntry {
    time: String,
    message: String,
    #[serde(rename = "type")]
    entry_type: String,
}

impl MomentumStrategy {
    /// Creates a new MomentumStrategy.
    ///
    /// # Arguments
    /// * `name` - Unique name for this strategy instance
    /// * `instruments` - List of instruments to watch
    /// * `exec` - Shared execution client
    /// * `lookback_period` - Number of ticks to use for momentum calculation
    /// * `momentum_threshold` - Minimum % change to trigger signal (e.g., 0.02 for 2%)
    pub fn new(
        name: impl Into<String>,
        instruments: Vec<String>,
        exec: SharedExecutionClient,
        lookback_period: usize,
        momentum_threshold: f64,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            instruments,
            exec,
            state: Mutex::new(MomentumState {
                price_history: VecDeque::with_capacity(lookback_period),
                lookback_period,
                momentum_threshold,
                current_momentum: None,
                log: VecDeque::with_capacity(MAX_LOG_ENTRIES),
                signal_count: 0,
                last_signal: None,
            }),
        })
    }

    /// Creates with default parameters (10 tick lookback, 1% threshold).
    pub fn default_config(
        name: impl Into<String>,
        instruments: Vec<String>,
        exec: SharedExecutionClient,
    ) -> Arc<Self> {
        Self::new(name, instruments, exec, 10, 0.01)
    }

    /// Adds a log entry to the dashboard state
    fn add_log(state: &mut MomentumState, message: String, entry_type: &str) {
        let now = chrono::Utc::now();
        let entry = LogEntry {
            time: now.format("%H:%M:%S").to_string(),
            message,
            entry_type: entry_type.to_string(),
        };
        state.log.push_back(entry);
        if state.log.len() > MAX_LOG_ENTRIES {
            state.log.pop_front();
        }
    }
}

// =============================================================================
// Dashboard Implementation
// =============================================================================

#[async_trait]
impl Dashboard for MomentumStrategy {
    fn dashboard_name(&self) -> &str {
        &self.name
    }

    async fn dashboard_state(&self) -> Value {
        let state = self.state.lock().await;
        json!({
            "lookback_period": state.lookback_period,
            "momentum_threshold": state.momentum_threshold * 100.0, // Display as percentage
            "current_momentum": state.current_momentum.map(|m| m * 100.0),
            "signal_count": state.signal_count,
            "last_signal": state.last_signal,
            "price_history": state.price_history.iter().collect::<Vec<_>>(),
            "log": state.log.iter().collect::<Vec<_>>(),
        })
    }

    fn dashboard_schema(&self) -> DashboardSchema {
        DashboardSchema {
            widgets: vec![
                Widget::KeyValue {
                    label: "Lookback Period".to_string(),
                    key: "lookback_period".to_string(),
                    format: Some("{} ticks".to_string()),
                },
                Widget::KeyValue {
                    label: "Momentum Threshold".to_string(),
                    key: "momentum_threshold".to_string(),
                    format: Some("{:.2}%".to_string()),
                },
                Widget::KeyValue {
                    label: "Current Momentum".to_string(),
                    key: "current_momentum".to_string(),
                    format: Some("{:.4}%".to_string()),
                },
                Widget::KeyValue {
                    label: "Signals Triggered".to_string(),
                    key: "signal_count".to_string(),
                    format: None,
                },
                Widget::KeyValue {
                    label: "Last Signal".to_string(),
                    key: "last_signal".to_string(),
                    format: None,
                },
                Widget::Chart {
                    title: "Price History".to_string(),
                    data_key: "price_history".to_string(),
                    chart_type: "line".to_string(),
                },
                Widget::Log {
                    title: "Activity Log".to_string(),
                    data_key: "log".to_string(),
                    max_lines: 50,
                },
            ],
        }
    }
}

// =============================================================================
// Strategy Implementation
// =============================================================================

#[async_trait]
impl Strategy for MomentumStrategy {
    fn name(&self) -> &str {
        &self.name
    }

    fn required_subscriptions(&self) -> Vec<String> {
        self.instruments.clone()
    }

    async fn on_event(&self, event: MarketEvent) {
        let mut state = self.state.lock().await;

        // Calculate mid price
        let mid = match (event.best_bid, event.best_ask) {
            (Some(bid), Some(ask)) => (bid + ask) / 2.0,
            (Some(bid), None) => bid,
            (None, Some(ask)) => ask,
            (None, None) => return, // No price data
        };

        // Add to history
        state.price_history.push_back(mid);

        // Trim to lookback period
        while state.price_history.len() > state.lookback_period {
            state.price_history.pop_front();
        }

        // Need full lookback period to calculate momentum
        if state.price_history.len() < state.lookback_period {
            let msg = format!(
                "{} Warming up: {}/{} ticks",
                event.instrument,
                state.price_history.len(),
                state.lookback_period
            );
            info!("[{}] {}", self.name, msg);
            Self::add_log(&mut state, msg, "info");
            return;
        }

        // Calculate momentum (simple: first vs last price)
        let first_price = state.price_history.front().unwrap();
        let last_price = state.price_history.back().unwrap();
        let momentum = (last_price - first_price) / first_price;
        state.current_momentum = Some(momentum);

        if momentum.abs() > state.momentum_threshold {
            let direction = if momentum > 0.0 { "BULLISH" } else { "BEARISH" };
            state.signal_count += 1;
            state.last_signal = Some(direction.to_string());

            let msg = format!(
                "{} Momentum Signal: {} ({:.2}%)",
                event.instrument,
                direction,
                momentum * 100.0
            );
            info!("[{}] {}", self.name, msg);
            Self::add_log(&mut state, msg, "signal");

            // In a real implementation, place an order:
            // let order = Order { ... };
            // let _ = self.exec.place_order(order).await;
        } else {
            let msg = format!(
                "{} Mid: {:.2}, Momentum: {:.4}%",
                event.instrument, mid, momentum * 100.0
            );
            info!("[{}] {}", self.name, msg);
            Self::add_log(&mut state, msg, "info");
        }
    }
}

