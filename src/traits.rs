// src/traits.rs

use async_trait::async_trait;
use crate::models::{MarketEvent, Order, OrderId};
use std::sync::Arc;

/// A stream of market events from an exchange.
/// This is still used internally by the Engine to receive raw events.
#[async_trait]
pub trait MarketStream: Send + Unpin {
    async fn next(&mut self) -> Option<MarketEvent>;
}

/// Execution client for placing orders.
/// Implementations should be Clone + Send + Sync so they can be shared across strategies.
#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn place_order(&self, order: Order) -> Result<OrderId, String>;
    // ... cancel, get_positions, etc.
}

/// The core Strategy trait for the multi-strategy engine.
/// Each strategy declares its interests and reacts to market events.
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Returns the name of this strategy (for logging).
    fn name(&self) -> &str;

    /// Returns the list of instruments/tokens this strategy needs to receive events for.
    /// The engine will aggregate these across all strategies to build the subscription list.
    fn required_subscriptions(&self) -> Vec<String>;

    /// Called by the engine when a market event arrives for an instrument this strategy watches.
    /// The strategy should process the event and optionally place orders via the exec client.
    async fn on_event(&self, event: MarketEvent);
}

/// Wrapper to make any ExecutionClient shareable across strategies.
/// Strategies receive an Arc<dyn ExecutionClient> so multiple can share one connection.
pub type SharedExecutionClient = Arc<dyn ExecutionClient>;
