// src/strategy/gamma_scalp.rs

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

/// GammaScalp strategy: trades when delta exceeds a threshold.
///
/// Now implements the Strategy trait for use with the multi-strategy engine.
pub struct GammaScalp {
    name: String,
    instruments: Vec<String>,
    exec: SharedExecutionClient,
    /// Internal state protected by a mutex for thread-safe access
    state: Mutex<GammaScalpState>,
}

struct GammaScalpState {
    delta_threshold: f64,
    last_delta: Option<f64>,
    /// History of delta values for charting
    delta_history: VecDeque<f64>,
    /// Recent log entries for the dashboard
    log: VecDeque<LogEntry>,
    /// Count of signals triggered
    signal_count: u64,
}

#[derive(Clone, serde::Serialize)]
struct LogEntry {
    time: String,
    message: String,
    #[serde(rename = "type")]
    entry_type: String,
}

impl GammaScalp {
    /// Creates a new GammaScalp strategy.
    ///
    /// # Arguments
    /// * `name` - Unique name for this strategy instance
    /// * `instruments` - List of instruments to watch (e.g., ["BTC-29MAR24-60000-C"])
    /// * `exec` - Shared execution client for placing orders
    pub fn new(
        name: impl Into<String>,
        instruments: Vec<String>,
        exec: SharedExecutionClient,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            instruments,
            exec,
            state: Mutex::new(GammaScalpState {
                delta_threshold: 0.5,
                last_delta: None,
                delta_history: VecDeque::with_capacity(100),
                log: VecDeque::with_capacity(MAX_LOG_ENTRIES),
                signal_count: 0,
            }),
        })
    }

    /// Creates with a custom delta threshold.
    pub fn with_threshold(
        name: impl Into<String>,
        instruments: Vec<String>,
        exec: SharedExecutionClient,
        threshold: f64,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            instruments,
            exec,
            state: Mutex::new(GammaScalpState {
                delta_threshold: threshold,
                last_delta: None,
                delta_history: VecDeque::with_capacity(100),
                log: VecDeque::with_capacity(MAX_LOG_ENTRIES),
                signal_count: 0,
            }),
        })
    }

    /// Adds a log entry to the dashboard state
    fn add_log(state: &mut GammaScalpState, message: String, entry_type: &str) {
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
impl Dashboard for GammaScalp {
    fn dashboard_name(&self) -> &str {
        &self.name
    }

    async fn dashboard_state(&self) -> Value {
        let state = self.state.lock().await;
        json!({
            "delta_threshold": state.delta_threshold,
            "last_delta": state.last_delta,
            "signal_count": state.signal_count,
            "delta_history": state.delta_history.iter().collect::<Vec<_>>(),
            "log": state.log.iter().collect::<Vec<_>>(),
        })
    }

    fn dashboard_schema(&self) -> DashboardSchema {
        DashboardSchema {
            widgets: vec![
                Widget::KeyValue {
                    label: "Delta Threshold".to_string(),
                    key: "delta_threshold".to_string(),
                    format: Some("{:.2}".to_string()),
                },
                Widget::KeyValue {
                    label: "Last Delta".to_string(),
                    key: "last_delta".to_string(),
                    format: Some("{:.4}".to_string()),
                },
                Widget::KeyValue {
                    label: "Signals Triggered".to_string(),
                    key: "signal_count".to_string(),
                    format: None,
                },
                Widget::Chart {
                    title: "Delta History".to_string(),
                    data_key: "delta_history".to_string(),
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
impl Strategy for GammaScalp {
    fn name(&self) -> &str {
        &self.name
    }

    fn required_subscriptions(&self) -> Vec<String> {
        self.instruments.clone()
    }

    async fn on_event(&self, event: MarketEvent) {
        let mut state = self.state.lock().await;

        if let Some(delta) = event.delta {
            state.last_delta = Some(delta);

            // Track delta history for charting
            state.delta_history.push_back(delta);
            if state.delta_history.len() > 100 {
                state.delta_history.pop_front();
            }

            if delta.abs() > state.delta_threshold {
                state.signal_count += 1;
                let msg = format!(
                    "High Delta on {}: {:.4} (threshold: {:.2})",
                    event.instrument, delta, state.delta_threshold
                );
                info!("[{}] {}", self.name, msg);
                Self::add_log(&mut state, msg, "signal");

                // In a real implementation, we would place a hedge order here:
                // let order = Order { ... };
                // let _ = self.exec.place_order(order).await;
            }
        }

        // Log BBO for debugging
        if let (Some(bid), Some(ask)) = (event.best_bid, event.best_ask) {
            let msg = format!("{} BBO: {:.2} / {:.2}", event.instrument, bid, ask);
            info!("[{}] {}", self.name, msg);
            Self::add_log(&mut state, msg, "info");
        }
    }
}
