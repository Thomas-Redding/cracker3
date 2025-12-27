// src/connectors/backtest.rs

use crate::models::{MarketEvent, Order, OrderId};
use crate::traits::{ExecutionClient, MarketStream, SharedExecutionClient};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

// --- 1. The Mock Stream ---
pub struct BacktestStream {
    // We use VecDeque to pop events off one by one
    events: VecDeque<MarketEvent>,
}

impl BacktestStream {
    pub fn new(data: Vec<MarketEvent>) -> Self {
        Self {
            events: VecDeque::from(data),
        }
    }
}

#[async_trait]
impl MarketStream for BacktestStream {
    async fn next(&mut self) -> Option<MarketEvent> {
        // Instantly returns the next event from memory.
        // In a complex backtester, you might simulate "time" delays here.
        self.events.pop_front()
    }
}

// --- 2. The Mock Execution ---

/// Mock execution client for backtesting.
/// 
/// Clone + thread-safe, tracks all filled orders for PnL calculation.
#[derive(Clone)]
pub struct MockExec {
    inner: Arc<MockExecInner>,
}

struct MockExecInner {
    fills: Mutex<Vec<Order>>,
}

impl MockExec {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockExecInner {
                fills: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Wraps this client in an Arc for use as SharedExecutionClient.
    pub fn shared(self) -> SharedExecutionClient {
        Arc::new(self)
    }

    /// Returns a copy of all filled orders (for PnL analysis after backtest).
    pub async fn get_fills(&self) -> Vec<Order> {
        self.inner.fills.lock().await.clone()
    }
}

impl Default for MockExec {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionClient for MockExec {
    async fn place_order(&self, order: Order) -> Result<OrderId, String> {
        println!("BACKTEST: Filled order for {:?}", order);
        self.inner.fills.lock().await.push(order);
        Ok("mock_ord_1".to_string())
    }
}
