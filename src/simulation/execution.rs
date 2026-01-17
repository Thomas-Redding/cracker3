use crate::models::{Instrument, Order, OrderSide, OrderId, Position, OrderType};
use crate::traits::ExecutionClient;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Internal state for simulation.
#[derive(Debug, Default)]
struct SimState {
    cash: f64,
    positions: HashMap<Instrument, Position>,
    /// Latest known prices for instruments (used for filling market orders and valuation)
    prices: HashMap<Instrument, f64>,
    /// Active orders (not really used yet, assuming immediate fill)
    active_orders: HashMap<OrderId, Order>,
    /// Order history
    fills: Vec<(OrderId, f64, f64)>, // (id, qty, price)
}

/// A simulated execution client that fills orders immediately at the current price.
pub struct SimulatedExecutionClient {
    state: Arc<Mutex<SimState>>,
}

impl SimulatedExecutionClient {
    /// Creates a new simulated execution client with initial cash.
    pub fn new(initial_cash: f64) -> Self {
        let mut state = SimState::default();
        state.cash = initial_cash;
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Updates the current price for an instrument.
    /// Call this before processing events/strategies to ensure fills are accurate.
    pub fn update_price(&self, instrument: Instrument, price: f64) {
        let mut state = self.state.lock().unwrap();
        state.prices.insert(instrument, price);
    }

    /// Calculates the total equity (cash + market value of positions).
    pub async fn get_equity(&self) -> f64 {
        let state = self.state.lock().unwrap();
        let mut equity = state.cash;
        for (instrument, position) in state.positions.iter() {
            if position.quantity.abs() > f64::EPSILON { // Only consider non-zero positions
                if let Some(price) = state.prices.get(instrument) {
                    equity += position.quantity * price;
                }
            }
        }
        equity
    }
}

#[async_trait]
impl ExecutionClient for SimulatedExecutionClient {
    async fn place_order(&self, order: Order) -> Result<OrderId, String> {
        let mut state = self.state.lock().unwrap();
        
        // Determine fill price
        let fill_price = match order.order_type {
            OrderType::Limit => {
                // For simplicity in this first version of refactor,
                // we assume limit orders fill immediately if price is set.
                // In a real backtester, we'd check if the market price crosses the limit.
                // But CrossMarketStrategy mostly uses "optimization" targets which implies
                // it wants to trade NOW.
                // If the order has a price, use it.
                order.price.unwrap() // Limit orders must have price
            }
            OrderType::Market => {
                // Use current market price
                *state.prices.get(&order.instrument).ok_or_else(|| 
                    format!("No price available for instrument: {:?}", order.instrument)
                )?
            }
        };

        // Calculate cost/proceeds
        let cost = fill_price * order.quantity;
        
        // Update Cash
        match order.side {
            OrderSide::Buy => {
                state.cash -= cost;
            }
            OrderSide::Sell => {
                state.cash += cost;
            }
        }

        // Update Position
        let entry = state.positions.entry(order.instrument.clone()).or_insert_with(|| Position {
            instrument: order.instrument.clone(),
            quantity: 0.0,
            average_price: Some(0.0),
            unrealized_pnl: Some(0.0),
        });

        // Update average entry price logic (simplified)
        // If changing direction or increasing size... this is complex to do perfectly.
        // For now: weighted average if adding to position. Reset if flipping.
        
        let old_qty = entry.quantity;
        let signed_qty = match order.side {
            OrderSide::Buy => order.quantity,
            OrderSide::Sell => -order.quantity,
        };
        
        let new_qty = old_qty + signed_qty;
        
        // Update avg price only if increasing position in same direction
        if old_qty == 0.0 {
            entry.average_price = Some(fill_price);
        } else if (old_qty > 0.0 && signed_qty > 0.0) || (old_qty < 0.0 && signed_qty < 0.0) {
           // Weighted average
           let old_val = old_qty.abs() * entry.average_price.unwrap_or(0.0);
           let new_val = signed_qty.abs() * fill_price;
           entry.average_price = Some((old_val + new_val) / new_qty.abs());
        }
        // If reducing, avg price doesn't change (FIFO/LIFO assumption irrelevant for avg price usually)
        // If flipping (e.g. 10 long, sell 20 -> 10 short)
        else if (old_qty > 0.0 && new_qty < 0.0) || (old_qty < 0.0 && new_qty > 0.0) {
            // The portion that closed uses old avg price. The new portion uses new price.
            entry.average_price = Some(fill_price);
        }
        
        entry.quantity = new_qty;

        // Generate ID
        let order_id = uuid::Uuid::new_v4().to_string();
        state.fills.push((order_id.clone(), signed_qty, fill_price));
        
        Ok(order_id)
    }

    async fn cancel_order(&self, _order_id: &OrderId, _instrument: &Instrument) -> Result<(), String> {
        // Immediate fills imply no cancellation needed for now
        Ok(())
    }

    async fn get_position(&self, instrument: &Instrument) -> Result<Position, String> {
        let state = self.state.lock().unwrap();
        if let Some(pos) = state.positions.get(instrument) {
            Ok(pos.clone())
        } else {
             Ok(Position {
                instrument: instrument.clone(),
                quantity: 0.0,
                average_price: Some(0.0),
                unrealized_pnl: Some(0.0),
            })
        }
    }

    async fn get_positions(&self) -> Result<Vec<Position>, String> {
        let state = self.state.lock().unwrap();
        Ok(state.positions.values().cloned().collect())
    }

    async fn get_balance(&self) -> Result<f64, String> {
        let state = self.state.lock().unwrap();
        Ok(state.cash)
    }
}
