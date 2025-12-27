// src/connectors/backtest.rs
use crate::models::{MarketEvent, Order, OrderId};
use crate::traits::{MarketStream, ExecutionClient};
use async_trait::async_trait;
use std::collections::VecDeque;

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
pub struct MockExec {
    pub fills: Vec<Order>, // Track what we "traded" to calculate PnL later
}

impl MockExec {
    pub fn new() -> Self {
        Self { fills: Vec::new() }
    }
}

#[async_trait]
impl ExecutionClient for MockExec {
    async fn place_order(&mut self, order: Order) -> Result<OrderId, String> {
        println!("BACKTEST: Filled order for {:?}", order);
        self.fills.push(order);
        Ok("mock_ord_1".to_string())
    }
}
