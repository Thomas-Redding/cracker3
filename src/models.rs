// src/models.rs

use serde::{Deserialize, Serialize};

// The standardized event your Strategy listens to
#[derive(Debug, Clone, Serialize, Deserialize)]
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

// --- Raw Derive Types (Used for JSON parsing only) ---
// Derive uses minified keys in WebSocket ticker_slim messages

#[derive(Debug, Deserialize, Clone)]
pub struct DeriveResponse {
    pub method: Option<String>,
    pub params: Option<DeriveParams>,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeriveParams {
    pub channel: Option<String>,
    pub data: Option<DeriveTickerData>,
}

/// Ticker data from Derive WebSocket.
/// Supports both verbose (REST) and minified (WebSocket ticker_slim) keys.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct DeriveTickerData {
    // Basic Info
    #[serde(alias = "t")]
    pub timestamp: Option<i64>,
    pub instrument_name: Option<String>,
    #[serde(default)]
    pub state: Option<String>,

    // Prices - minified keys: b=bid, a=ask, B=bid_amount, A=ask_amount, m=mark, i=index
    #[serde(alias = "b")]
    pub best_bid_price: Option<f64>,
    #[serde(alias = "a")]
    pub best_ask_price: Option<f64>,
    #[serde(alias = "B")]
    pub best_bid_amount: Option<f64>,
    #[serde(alias = "A")]
    pub best_ask_amount: Option<f64>,
    #[serde(alias = "m")]
    pub mark_price: Option<f64>,
    #[serde(alias = "i")]
    pub index_price: Option<f64>,

    // Option pricing nested object (WebSocket format)
    pub option_pricing: Option<DeriveOptionPricing>,

    // Stats nested object
    pub stats: Option<DeriveStats>,

    // Direct fields (REST format fallbacks)
    pub underlying_price: Option<f64>,
    pub mark_iv: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    pub delta: Option<f64>,
    pub vega: Option<f64>,
    pub open_interest: Option<f64>,
}

/// Option pricing data from Derive (nested in ticker_slim).
/// Uses minified keys: f=forward/underlying, i=mark_iv, bi=bid_iv, ai=ask_iv, d=delta, v=vega
#[derive(Debug, Deserialize, Clone, Default)]
pub struct DeriveOptionPricing {
    #[serde(alias = "f")]
    pub underlying_price: Option<f64>,
    #[serde(alias = "i")]
    pub mark_iv: Option<f64>,
    #[serde(alias = "bi")]
    pub bid_iv: Option<f64>,
    #[serde(alias = "ai")]
    pub ask_iv: Option<f64>,
    #[serde(alias = "d")]
    pub delta: Option<f64>,
    #[serde(alias = "v")]
    pub vega: Option<f64>,
}

/// Stats data from Derive (nested in ticker_slim).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct DeriveStats {
    #[serde(alias = "oi")]
    pub open_interest: Option<f64>,
}
