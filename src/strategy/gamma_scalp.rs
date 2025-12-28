// src/strategy/gamma_scalp.rs

use crate::models::MarketEvent;
use crate::traits::{Strategy, SharedExecutionClient};
use async_trait::async_trait;
use log::info;
use std::sync::Arc;
use tokio::sync::Mutex;

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
}

impl GammaScalp {
    /// Creates a new GammaScalp strategy.
    /// 
    /// # Arguments
    /// * `name` - Unique name for this strategy instance
    /// * `instruments` - List of instruments to watch (e.g., ["BTC-29MAR24-60000-C"])
    /// * `exec` - Shared execution client for placing orders
    pub fn new(name: impl Into<String>, instruments: Vec<String>, exec: SharedExecutionClient) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            instruments,
            exec,
            state: Mutex::new(GammaScalpState {
                delta_threshold: 0.5,
                last_delta: None,
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
            }),
        })
    }
}

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

            if delta.abs() > state.delta_threshold {
                info!(
                    "[{}] High Delta detected on {}: {} (threshold: {})",
                    self.name, event.instrument, delta, state.delta_threshold
                );
                
                // In a real implementation, we would place a hedge order here:
                // let order = Order { ... };
                // let _ = self.exec.place_order(order).await;
            }
        }

        // Log BBO for debugging
        if let (Some(bid), Some(ask)) = (event.best_bid, event.best_ask) {
            info!(
                "[{}] {} BBO: {:.2} / {:.2}",
                self.name, event.instrument, bid, ask
            );
        }
    }
}
