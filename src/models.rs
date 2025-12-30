// src/models.rs

use serde::{Deserialize, Serialize};
use std::fmt;

// =============================================================================
// Exchange and Instrument Types
// =============================================================================

/// Supported exchanges.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Deribit,
    Polymarket,
    Derive,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Deribit => write!(f, "deribit"),
            Exchange::Polymarket => write!(f, "polymarket"),
            Exchange::Derive => write!(f, "derive"),
        }
    }
}

/// A typed instrument identifier that encapsulates the exchange and symbol.
/// 
/// # Examples
/// ```
/// use trading_bot::models::Instrument;
/// 
/// let deribit_opt = Instrument::Deribit("BTC-29MAR24-60000-C".to_string());
/// let poly_token = Instrument::Polymarket("21742633...".to_string());
/// let derive_opt = Instrument::Derive("BTC-20251226-100000-C".to_string());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "exchange", content = "symbol")]
pub enum Instrument {
    /// Deribit instrument (e.g., "BTC-29MAR24-60000-C", "ETH-PERPETUAL")
    Deribit(String),
    /// Polymarket token ID (numeric string)
    Polymarket(String),
    /// Derive instrument (e.g., "BTC-20251226-100000-C")
    Derive(String),
}

impl Instrument {
    /// Returns which exchange this instrument belongs to.
    pub fn exchange(&self) -> Exchange {
        match self {
            Instrument::Deribit(_) => Exchange::Deribit,
            Instrument::Polymarket(_) => Exchange::Polymarket,
            Instrument::Derive(_) => Exchange::Derive,
        }
    }

    /// Returns the symbol/identifier portion of the instrument.
    pub fn symbol(&self) -> &str {
        match self {
            Instrument::Deribit(s) => s,
            Instrument::Polymarket(s) => s,
            Instrument::Derive(s) => s,
        }
    }

    /// Creates a Deribit instrument.
    pub fn deribit(symbol: impl Into<String>) -> Self {
        Instrument::Deribit(symbol.into())
    }

    /// Creates a Polymarket instrument.
    pub fn polymarket(token_id: impl Into<String>) -> Self {
        Instrument::Polymarket(token_id.into())
    }

    /// Creates a Derive instrument.
    pub fn derive(symbol: impl Into<String>) -> Self {
        Instrument::Derive(symbol.into())
    }
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Instrument::Deribit(s) => write!(f, "deribit:{}", s),
            Instrument::Polymarket(s) => write!(f, "polymarket:{}", s),
            Instrument::Derive(s) => write!(f, "derive:{}", s),
        }
    }
}

// =============================================================================
// Market Events
// =============================================================================

/// The standardized event your Strategy listens to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub timestamp: i64,
    pub instrument: Instrument,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub delta: Option<f64>,
    /// Implied volatility fields (Deribit options)
    pub mark_iv: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    /// Underlying price (for options, the spot/index price)
    pub underlying_price: Option<f64>,
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
    /// Underlying index price (e.g., BTC-USD index)
    pub underlying_price: Option<f64>,
    /// Alternative name for underlying price in some responses
    pub index_price: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Greeks {
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub theta: Option<f64>,
    pub vega: Option<f64>,
}

// =============================================================================
// Order Types
// =============================================================================

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Market,
    Limit,
}

/// An order to be placed on an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// The instrument to trade.
    pub instrument: Instrument,
    /// Buy or sell.
    pub side: OrderSide,
    /// Order type (market or limit).
    pub order_type: OrderType,
    /// Quantity to trade.
    pub quantity: f64,
    /// Price for limit orders.
    pub price: Option<f64>,
    /// Client order ID (optional).
    pub client_order_id: Option<String>,
}

impl Order {
    /// Creates a new market order.
    pub fn market(instrument: Instrument, side: OrderSide, quantity: f64) -> Self {
        Self {
            instrument,
            side,
            order_type: OrderType::Market,
            quantity,
            price: None,
            client_order_id: None,
        }
    }

    /// Creates a new limit order.
    pub fn limit(instrument: Instrument, side: OrderSide, quantity: f64, price: f64) -> Self {
        Self {
            instrument,
            side,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            client_order_id: None,
        }
    }

    /// Sets a client order ID.
    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_order_id = Some(id.into());
        self
    }
}

/// Order ID returned by the exchange.
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
