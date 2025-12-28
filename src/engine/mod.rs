// src/engine/mod.rs

use crate::models::MarketEvent;
use crate::traits::{MarketStream, Strategy, SharedExecutionClient};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Configuration for a strategy including its execution client.
pub struct StrategyConfig {
    pub strategy: Arc<dyn Strategy>,
    pub exec: SharedExecutionClient,
}

/// The MarketRouter (Engine) manages multiple strategies and a shared market data stream.
/// 
/// It:
/// 1. Aggregates subscriptions from all strategies
/// 2. Maintains one WebSocket connection per exchange
/// 3. Routes incoming events to interested strategies via broadcast channels
pub struct MarketRouter<S: MarketStream> {
    stream: S,
    strategies: Vec<Arc<dyn Strategy>>,
    /// Maps instrument -> list of strategy indices that are interested
    routing_table: HashMap<String, Vec<usize>>,
}

impl<S: MarketStream + 'static> MarketRouter<S> {
    /// Creates a new MarketRouter.
    /// 
    /// # Arguments
    /// * `stream` - The market data stream (should be initialized with the superset of all instruments)
    /// * `strategies` - List of strategies to run
    pub fn new(stream: S, strategies: Vec<Arc<dyn Strategy>>) -> Self {
        // Build the routing table
        let mut routing_table: HashMap<String, Vec<usize>> = HashMap::new();
        
        for (idx, strategy) in strategies.iter().enumerate() {
            for instrument in strategy.required_subscriptions() {
                routing_table
                    .entry(instrument)
                    .or_insert_with(Vec::new)
                    .push(idx);
            }
        }

        info!(
            "MarketRouter initialized with {} strategies, {} unique instruments",
            strategies.len(),
            routing_table.len()
        );

        Self {
            stream,
            strategies,
            routing_table,
        }
    }

    /// Aggregates all required subscriptions from a list of strategies.
    /// Call this BEFORE creating the stream to know what instruments to subscribe to.
    pub fn aggregate_subscriptions(strategies: &[Arc<dyn Strategy>]) -> Vec<String> {
        let mut unique: HashSet<String> = HashSet::new();
        for strategy in strategies {
            for sub in strategy.required_subscriptions() {
                unique.insert(sub);
            }
        }
        unique.into_iter().collect()
    }

    /// Runs the engine, distributing market events to interested strategies.
    /// This spawns each strategy's event handler concurrently.
    pub async fn run(mut self) {
        info!("MarketRouter starting event loop...");

        while let Some(event) = self.stream.next().await {
            let instrument = &event.instrument;

            // Find which strategies are interested in this instrument
            if let Some(strategy_indices) = self.routing_table.get(instrument) {
                // Clone the event for each interested strategy and spawn handlers
                let mut handles = Vec::new();

                for &idx in strategy_indices {
                    let strategy = Arc::clone(&self.strategies[idx]);
                    let event_clone = event.clone();

                    // Spawn each strategy's handler concurrently
                    let handle = tokio::spawn(async move {
                        strategy.on_event(event_clone).await;
                    });
                    handles.push(handle);
                }

                // Optionally wait for all handlers to complete before processing next event
                // This ensures ordering within a tick. Remove if you want full parallelism.
                for handle in handles {
                    if let Err(e) = handle.await {
                        warn!("Strategy handler panicked: {:?}", e);
                    }
                }
            }
        }

        info!("MarketRouter: Stream ended, shutting down.");
    }
}

/// Builder for constructing a multi-strategy engine with proper subscription aggregation.
pub struct EngineBuilder {
    strategies: Vec<Arc<dyn Strategy>>,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    /// Adds a strategy to the engine.
    pub fn add_strategy(mut self, strategy: Arc<dyn Strategy>) -> Self {
        self.strategies.push(strategy);
        self
    }

    /// Returns the aggregated list of all instruments needed by all strategies.
    pub fn get_required_instruments(&self) -> Vec<String> {
        MarketRouter::<DummyStream>::aggregate_subscriptions(&self.strategies)
    }

    /// Consumes the builder and returns the list of strategies.
    /// Use this to then create the appropriate stream and MarketRouter.
    pub fn build(self) -> Vec<Arc<dyn Strategy>> {
        self.strategies
    }
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Dummy stream type for the aggregate_subscriptions function signature
// (never actually instantiated)
struct DummyStream;

#[async_trait::async_trait]
impl MarketStream for DummyStream {
    async fn next(&mut self) -> Option<MarketEvent> {
        None
    }
}

impl std::marker::Unpin for DummyStream {}

