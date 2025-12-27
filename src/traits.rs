// src/traits.rs

use async_trait::async_trait;
use crate::models::{MarketEvent, Order, OrderId};

#[async_trait]
pub trait MarketStream: Send + Unpin {
    async fn next(&mut self) -> Option<MarketEvent>;
}

#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn place_order(&mut self, order: Order) -> Result<OrderId, String>;
    // ... cancel, get_positions, etc.
}
