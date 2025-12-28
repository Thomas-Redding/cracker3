// src/strategy/momentum.rs

use crate::models::MarketEvent;
use crate::traits::{Strategy, SharedExecutionClient};
use async_trait::async_trait;
use log::info;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

/// MomentumStrategy: a simple strategy that tracks price momentum.
/// 
/// It watches for sustained price movement in one direction and generates
/// signals when momentum is detected.
pub struct MomentumStrategy {
    name: String,
    instruments: Vec<String>,
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
}

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
            info!(
                "[{}] {} Warming up: {}/{} ticks",
                self.name,
                event.instrument,
                state.price_history.len(),
                state.lookback_period
            );
            return;
        }

        // Calculate momentum (simple: first vs last price)
        let first_price = state.price_history.front().unwrap();
        let last_price = state.price_history.back().unwrap();
        let momentum = (last_price - first_price) / first_price;

        if momentum.abs() > state.momentum_threshold {
            let direction = if momentum > 0.0 { "BULLISH" } else { "BEARISH" };
            info!(
                "[{}] {} Momentum Signal: {} ({:.2}%)",
                self.name,
                event.instrument,
                direction,
                momentum * 100.0
            );

            // In a real implementation, place an order:
            // let order = Order { ... };
            // let _ = self.exec.place_order(order).await;
        } else {
            info!(
                "[{}] {} Mid: {:.2}, Momentum: {:.4}%",
                self.name, event.instrument, mid, momentum * 100.0
            );
        }
    }
}

