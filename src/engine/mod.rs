// src/engine/mod.rs

use crate::catalog::SharedRefreshable;
use crate::models::{Exchange, Instrument, MarketEvent};
use crate::traits::{ExecutionRouter, MarketStream, SharedExecutionRouter, Strategy};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// =============================================================================
// Unified Multi-Exchange Engine
// =============================================================================

/// Configuration for the Engine.
pub struct EngineConfig {
    /// Interval for refreshing strategy subscriptions.
    pub subscription_refresh_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            subscription_refresh_interval: Duration::from_secs(60),
        }
    }
}

/// Shared stream reference for thread-safe access.
/// Uses Arc<dyn MarketStream> since the trait uses interior mutability.
pub type SharedStream = Arc<dyn MarketStream>;

/// The unified multi-exchange Engine.
/// 
/// Manages multiple strategies across multiple exchanges:
/// 1. Aggregates `required_exchanges()` from all strategies to determine connections
/// 2. Periodically refreshes catalogs, then calls `discover_subscriptions()` to update subscriptions
/// 3. Routes market events to interested strategies
/// 4. Provides an `ExecutionRouter` for strategies to place orders
/// 5. Uses refcounting to manage subscriptions - instruments stay subscribed while any strategy needs them
pub struct Engine {
    strategies: Vec<Arc<dyn Strategy>>,
    streams: HashMap<Exchange, SharedStream>,
    exec_router: SharedExecutionRouter,
    /// Catalogs for market discovery, keyed by exchange
    catalogs: HashMap<Exchange, SharedRefreshable>,
    /// Current subscription refcounts per exchange: HashMap<Exchange, HashMap<Instrument, usize>>
    current_subs: HashMap<Exchange, HashMap<Instrument, usize>>,
    /// Routing table: instrument -> strategy indices
    routing_table: HashMap<Instrument, Vec<usize>>,
    config: EngineConfig,
}

impl Engine {
    /// Creates a new Engine with the given strategies.
    /// 
    /// Call `with_stream()` for each exchange to add market data streams,
    /// then call `run()` to start processing events.
    pub fn new(strategies: Vec<Arc<dyn Strategy>>) -> Self {
        // Build initial routing table based on static subscriptions
        let mut routing_table: HashMap<Instrument, Vec<usize>> = HashMap::new();
        let mut current_subs: HashMap<Exchange, HashMap<Instrument, usize>> = HashMap::new();

        for (idx, strategy) in strategies.iter().enumerate() {
            // Use deprecated method for backwards compatibility
            #[allow(deprecated)]
            let legacy_subs = strategy.required_subscriptions();
            
            // For now, we can't know which exchange these belong to without parsing
            // In a full implementation, strategies would use discover_subscriptions()
            for sub in legacy_subs {
                // Try to infer exchange from format (this is a heuristic)
                let instrument = if sub.contains('-') && sub.len() > 20 {
                    // Looks like an option: BTC-29MAR24-60000-C
                    // Could be Deribit or Derive, default to Deribit
                    Instrument::Deribit(sub)
                } else if sub.chars().all(|c| c.is_numeric()) && sub.len() > 30 {
                    // Looks like a Polymarket token ID
                    Instrument::Polymarket(sub)
                } else {
                    // Default to Deribit
                    Instrument::Deribit(sub)
                };

                routing_table
                    .entry(instrument.clone())
                    .or_default()
                    .push(idx);
                
                // Initialize with refcount of 1 for legacy subscriptions
                *current_subs
                    .entry(instrument.exchange())
                    .or_default()
                    .entry(instrument)
                    .or_insert(0) += 1;
            }
        }

        info!(
            "Engine initialized with {} strategies, {} unique instruments",
            strategies.len(),
            routing_table.len()
        );

        Self {
            strategies,
            streams: HashMap::new(),
            exec_router: Arc::new(ExecutionRouter::empty()),
            catalogs: HashMap::new(),
            current_subs,
            routing_table,
            config: EngineConfig::default(),
        }
    }

    /// Adds a market data stream for an exchange.
    pub fn with_stream(mut self, exchange: Exchange, stream: Box<dyn MarketStream>) -> Self {
        self.streams.insert(exchange, Arc::from(stream));
        self
    }

    /// Adds a catalog for market discovery on an exchange.
    /// 
    /// Catalogs are refreshed before each subscription discovery cycle,
    /// ensuring strategies see newly listed markets.
    /// 
    /// Accepts any type implementing `Refreshable` (via `SharedRefreshable`).
    /// This includes `DeribitCatalog`, `DeriveCatalog`, and `PolymarketCatalog`.
    pub fn with_catalog(mut self, exchange: Exchange, catalog: SharedRefreshable) -> Self {
        self.catalogs.insert(exchange, catalog);
        self
    }

    /// Sets the execution router.
    pub fn with_exec_router(mut self, router: SharedExecutionRouter) -> Self {
        self.exec_router = router;
        self
    }

    /// Sets the engine configuration.
    pub fn with_config(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the execution router for use by strategies.
    pub fn exec_router(&self) -> SharedExecutionRouter {
        self.exec_router.clone()
    }

    /// Returns the set of exchanges that strategies require.
    pub fn required_exchanges(&self) -> HashSet<Exchange> {
        let mut exchanges = HashSet::new();
        for strategy in &self.strategies {
            exchanges.extend(strategy.required_exchanges());
        }
        exchanges
    }

    /// Refreshes all catalogs by fetching latest market data from exchanges.
    /// 
    /// This is called before `refresh_subscriptions()` to ensure strategies
    /// can discover newly listed markets.
    pub async fn refresh_catalogs(&self) {
        for (exchange, catalog) in &self.catalogs {
            debug!("Engine: Refreshing {:?} catalog...", exchange);
            match catalog.refresh().await {
                Ok(count) => {
                    info!("Engine: {:?} catalog refreshed, {} markets", exchange, count);
                }
                Err(e) => {
                    warn!("Engine: Failed to refresh {:?} catalog: {}", exchange, e);
                }
            }
        }
    }

    /// Refreshes subscriptions by calling discover_subscriptions() on all strategies.
    /// 
    /// Uses refcounting to manage overlapping subscriptions:
    /// - Subscribes when new_count > 0 and old_count == 0
    /// - Unsubscribes when new_count == 0 and old_count > 0
    /// - No-op otherwise (refcount changes don't require WS messages)
    pub async fn refresh_subscriptions(&mut self) {
        debug!("Engine: Refreshing subscriptions...");
        
        // Build new refcounts from all strategies
        let mut new_refcounts: HashMap<Exchange, HashMap<Instrument, usize>> = HashMap::new();
        let mut new_routing: HashMap<Instrument, Vec<usize>> = HashMap::new();

        for (idx, strategy) in self.strategies.iter().enumerate() {
            let instruments = strategy.discover_subscriptions().await;
            for instrument in instruments {
                new_routing.entry(instrument.clone()).or_default().push(idx);
                *new_refcounts
                    .entry(instrument.exchange())
                    .or_default()
                    .entry(instrument)
                    .or_insert(0) += 1;
            }
        }

        // Calculate diff and update subscriptions for each exchange
        for (exchange, stream) in &self.streams {
            let old_counts = self.current_subs.get(exchange).cloned().unwrap_or_default();
            let new_counts = new_refcounts.get(exchange).cloned().unwrap_or_default();

            // Find instruments to subscribe (new_count > 0, old_count == 0)
            let to_add: Vec<Instrument> = new_counts
                .keys()
                .filter(|inst| {
                    let new_count = new_counts.get(*inst).copied().unwrap_or(0);
                    let old_count = old_counts.get(*inst).copied().unwrap_or(0);
                    new_count > 0 && old_count == 0
                })
                .cloned()
                .collect();

            // Find instruments to unsubscribe (new_count == 0, old_count > 0)
            let to_remove: Vec<Instrument> = old_counts
                .keys()
                .filter(|inst| {
                    let new_count = new_counts.get(*inst).copied().unwrap_or(0);
                    let old_count = old_counts.get(*inst).copied().unwrap_or(0);
                    new_count == 0 && old_count > 0
                })
                .cloned()
                .collect();

            if !to_add.is_empty() {
                info!("Engine: Subscribing to {} new instruments on {:?}", to_add.len(), exchange);
                if let Err(e) = stream.subscribe(&to_add).await {
                    warn!("Engine: Failed to subscribe on {:?}: {}", exchange, e);
                }
            }

            if !to_remove.is_empty() {
                info!("Engine: Unsubscribing from {} instruments on {:?}", to_remove.len(), exchange);
                if let Err(e) = stream.unsubscribe(&to_remove).await {
                    warn!("Engine: Failed to unsubscribe on {:?}: {}", exchange, e);
                }
            }
        }

        self.current_subs = new_refcounts;
        self.routing_table = new_routing;
        
        debug!("Engine: Subscription refresh complete. {} total instruments across {} exchanges",
            self.routing_table.len(), self.current_subs.len());
    }

    /// Runs the engine, processing events from all streams concurrently.
    /// Uses tokio::select! to handle both market events and periodic subscription refresh.
    pub async fn run(mut self) {
        info!(
            "Engine: Starting event loop with {} exchanges, {} catalogs",
            self.streams.len(),
            self.catalogs.len()
        );

        // Initial catalog refresh + subscription discovery
        self.refresh_catalogs().await;
        self.refresh_subscriptions().await;

        // Create a channel for all streams to send events to
        let (tx, mut rx) = mpsc::channel::<(Exchange, MarketEvent)>(1000);

        // Spawn a task for each stream to forward events to the channel
        // Streams use interior mutability, so we can call next() through &self
        let mut stream_handles = Vec::new();
        for (exchange, stream) in &self.streams {
            let tx = tx.clone();
            let stream_clone = Arc::clone(stream);
            let exchange = *exchange;
            let handle = tokio::spawn(async move {
                loop {
                    // Call next() directly - stream uses interior mutability
                    match stream_clone.next().await {
                        Some(evt) => {
                            if tx.send((exchange, evt)).await.is_err() {
                                // Channel closed, stop this stream
                                break;
                            }
                        }
                        None => {
                            // Stream ended
                            break;
                        }
                    }
                }
                info!("Engine: Stream for {:?} ended", exchange);
            });
            stream_handles.push(handle);
        }

        // Drop our sender so the channel closes when all stream tasks finish
        drop(tx);

        // Create interval for periodic subscription refresh
        let mut refresh_interval = tokio::time::interval(self.config.subscription_refresh_interval);
        // Don't tick immediately - we just did an initial refresh
        refresh_interval.reset();

        // Process events as they arrive from any stream, with periodic refresh
        loop {
            tokio::select! {
                // Handle incoming market events
                event_result = rx.recv() => {
                    match event_result {
                        Some((_exchange, event)) => {
                            self.route_event(event).await;
                        }
                        None => {
                            // Channel closed, all streams have ended
                            break;
                        }
                    }
                }
                // Handle periodic catalog + subscription refresh
                _ = refresh_interval.tick() => {
                    debug!("Engine: Periodic refresh triggered");
                    self.refresh_catalogs().await;
                    self.refresh_subscriptions().await;
                }
            }
        }

        // Wait for all stream tasks to complete
        for handle in stream_handles {
            let _ = handle.await;
        }

        info!("Engine: Event loop ended.");
    }

    /// Routes an event to interested strategies.
    async fn route_event(&self, event: MarketEvent) {
        let instrument = &event.instrument;

        if let Some(strategy_indices) = self.routing_table.get(instrument) {
            let mut handles = Vec::new();

            for &idx in strategy_indices {
                let strategy = Arc::clone(&self.strategies[idx]);
                let event_clone = event.clone();

                let handle = tokio::spawn(async move {
                    strategy.on_event(event_clone).await;
                });
                handles.push(handle);
            }

            for handle in handles {
                if let Err(e) = handle.await {
                    warn!("Engine: Strategy handler panicked: {:?}", e);
                }
            }
        }
    }
}

// =============================================================================
// Legacy MarketRouter (for backwards compatibility)
// =============================================================================

/// The MarketRouter manages multiple strategies and a shared market data stream.
/// 
/// DEPRECATED: Use `Engine` instead for multi-exchange support.
#[deprecated(note = "Use Engine instead for multi-exchange support")]
pub struct MarketRouter<S: MarketStream> {
    stream: S,
    strategies: Vec<Arc<dyn Strategy>>,
    /// Maps instrument -> list of strategy indices that are interested
    routing_table: HashMap<Instrument, Vec<usize>>,
}

#[allow(deprecated)]
impl<S: MarketStream + 'static> MarketRouter<S> {
    /// Creates a new MarketRouter.
    pub fn new(stream: S, strategies: Vec<Arc<dyn Strategy>>) -> Self {
        let mut routing_table: HashMap<Instrument, Vec<usize>> = HashMap::new();
        
        for (idx, strategy) in strategies.iter().enumerate() {
            #[allow(deprecated)]
            for sub in strategy.required_subscriptions() {
                // Infer instrument type (simplified)
                let instrument = if sub.chars().all(|c| c.is_numeric()) && sub.len() > 30 {
                    Instrument::Polymarket(sub)
                } else {
                    Instrument::Deribit(sub)
                };
                routing_table.entry(instrument).or_default().push(idx);
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
    #[allow(deprecated)]
    pub fn aggregate_subscriptions(strategies: &[Arc<dyn Strategy>]) -> Vec<String> {
        let mut unique: HashSet<String> = HashSet::new();
        for strategy in strategies {
            for sub in strategy.required_subscriptions() {
                unique.insert(sub);
            }
        }
        unique.into_iter().collect()
    }

    /// Runs the router, distributing market events to interested strategies.
    pub async fn run(self) {
        info!("MarketRouter starting event loop...");

        while let Some(event) = self.stream.next().await {
            let instrument = &event.instrument;

            if let Some(strategy_indices) = self.routing_table.get(instrument) {
                let mut handles = Vec::new();

                for &idx in strategy_indices {
                    let strategy = Arc::clone(&self.strategies[idx]);
                    let event_clone = event.clone();

                    let handle = tokio::spawn(async move {
                        strategy.on_event(event_clone).await;
                    });
                    handles.push(handle);
                }

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

/// Builder for constructing a multi-strategy engine.
#[deprecated(note = "Use Engine::new() instead")]
pub struct EngineBuilder {
    strategies: Vec<Arc<dyn Strategy>>,
}

#[allow(deprecated)]
impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    pub fn add_strategy(mut self, strategy: Arc<dyn Strategy>) -> Self {
        self.strategies.push(strategy);
        self
    }

    #[allow(deprecated)]
    pub fn get_required_instruments(&self) -> Vec<String> {
        MarketRouter::<DummyStream>::aggregate_subscriptions(&self.strategies)
    }

    pub fn build(self) -> Vec<Arc<dyn Strategy>> {
        self.strategies
    }
}

#[allow(deprecated)]
impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Dummy stream type for the aggregate_subscriptions function signature
struct DummyStream;

#[async_trait::async_trait]
impl MarketStream for DummyStream {
    async fn next(&self) -> Option<MarketEvent> {
        None
    }
}
