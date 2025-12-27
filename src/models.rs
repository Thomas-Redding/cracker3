// src/models.rs

use serde::Deserialize;

// The standardized event your Strategy listens to
#[derive(Debug, Clone)]
pub struct MarketEvent {
    pub timestamp: i64,
    pub instrument: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub delta: Option<f64>,
    // Add other fields as needed
}

// --- Raw Deribit Types (Used for JSON parsing only) ---
#[derive(Debug, Deserialize, Clone)]
pub struct DeribitResponse {
    pub method: Option<String>,
    pub params: Option<DeribitParams>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeribitParams {
    pub channel: String,
    pub data: DeribitTickerData,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeribitTickerData {
    pub instrument_name: String,
    pub timestamp: i64,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub greeks: Option<Greeks>,
    pub mark_iv: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Greeks {
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub theta: Option<f64>,
    pub vega: Option<f64>,
}

// Placeholder for Order types used in traits
#[derive(Debug, Clone)]
pub struct Order {}
pub type OrderId = String;
