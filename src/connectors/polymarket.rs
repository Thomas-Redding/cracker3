// src/connectors/polymarket.rs

use crate::catalog::{validate_polymarket_order, MarketCatalog, PolymarketCatalog};
use crate::models::{Instrument, MarketEvent, Order, OrderId, Position};
use crate::traits::{ExecutionClient, MarketStream, SharedExecutionClient};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const POLY_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Commands sent from the stream to the actor for subscription management.
/// Polymarket doesn't support incremental sub/unsub, so we use reconnect strategy.
#[derive(Debug, Clone)]
pub enum PolymarketCommand {
    /// Update the subscription set and reconnect
    UpdateSubscriptions(HashSet<String>),
}

// --- Data Models (Internal to Polymarket) ---

/// Message types from Polymarket WebSocket.
/// 
/// Note: We use `#[serde(untagged)]` with explicit `event_type` matching
/// because Polymarket also sends messages without `asset_id` (like acks,
/// heartbeats, tick updates) that would fail to parse with the tagged approach.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
enum PolyMessage {
    Book(PolyBook),
    PriceChange(PolyPriceChange),
    /// Catch-all for messages we don't need (acks, heartbeats, tick data, etc.)
    #[allow(dead_code)]
    Unknown(serde_json::Value),
}

#[derive(Deserialize, Debug, Clone)]
struct PolyBook {
    event_type: String, // "book"
    asset_id: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    timestamp: String, // Poly sends strings "123456789"
}

#[derive(Deserialize, Debug, Clone)]
struct PolyPriceChange {
    event_type: String, // "price_change"
    asset_id: String,
    price_changes: Vec<PriceChangeItem>,
    timestamp: String,
}

#[derive(Deserialize, Debug, Clone)]
struct PriceChangeItem {
    side: String, // "BUY" or "SELL"
    price: String,
    size: String,
}

// Custom deserializer because Poly sends mixed types or lists for levels
#[derive(Deserialize, Debug, Clone)]
struct PriceLevel {
    price: String,
    size: String,
}

// --- The Local OrderBook ---
// We need this to reconstruct state from diffs
struct LocalOrderBook {
    // Price (int keys) -> Size (float)
    // We use BTreeMap so it is always sorted.
    // Poly prices are 0.0 - 1.0. We multiply by 1000 to get keys 0-1000.
    bids: BTreeMap<u64, f64>,
    asks: BTreeMap<u64, f64>,
}

impl LocalOrderBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Parses a price string to an integer key (price * 1000).
    /// Returns None if parsing fails.
    fn price_to_key(p: &str) -> Option<u64> {
        p.parse::<f64>().ok().map(|f| (f * 1000.0).round() as u64)
    }

    fn set_snapshot(&mut self, data: PolyBook) {
        self.bids.clear();
        self.asks.clear();

        for item in data.bids {
            let Some(key) = Self::price_to_key(&item.price) else {
                warn!("Malformed bid price in snapshot: {:?}", item.price);
                continue;
            };
            let Ok(size) = item.size.parse::<f64>() else {
                warn!("Malformed bid size in snapshot: {:?}", item.size);
                continue;
            };
            self.bids.insert(key, size);
        }
        for item in data.asks {
            let Some(key) = Self::price_to_key(&item.price) else {
                warn!("Malformed ask price in snapshot: {:?}", item.price);
                continue;
            };
            let Ok(size) = item.size.parse::<f64>() else {
                warn!("Malformed ask size in snapshot: {:?}", item.size);
                continue;
            };
            self.asks.insert(key, size);
        }
    }

    /// Applies a price level delta to the order book.
    /// Returns false if the delta was malformed and could not be applied.
    fn apply_delta(&mut self, change: PriceChangeItem) -> bool {
        let Some(key) = Self::price_to_key(&change.price) else {
            warn!("Malformed price in delta: {:?}", change.price);
            return false;
        };
        let Ok(size) = change.size.parse::<f64>() else {
            warn!("Malformed size in delta: {:?} - skipping update to preserve book integrity", change.size);
            return false;
        };

        let map = match change.side.as_str() {
            "BUY" => &mut self.bids,
            "SELL" => &mut self.asks,
            _ => {
                warn!("Unknown side in delta: {:?} - expected BUY or SELL", change.side);
                return false;
            }
        };

        if size == 0.0 {
            map.remove(&key);
        } else {
            map.insert(key, size);
        }
        true
    }

    fn get_best_bid(&self) -> Option<f64> {
        // Bids: keys are 0..1000. Best bid is the HIGHEST key (last).
        self.bids.keys().last().map(|&k| k as f64 / 1000.0)
    }

    fn get_best_ask(&self) -> Option<f64> {
        // Asks: keys are 0..1000. Best ask is the LOWEST key (first).
        self.asks.keys().next().map(|&k| k as f64 / 1000.0)
    }
}

// --- The Public Stream ---

pub struct PolymarketStream {
    /// Receiver wrapped in Mutex for interior mutability (allows &self in next())
    receiver: tokio::sync::Mutex<mpsc::Receiver<MarketEvent>>,
    /// Channel to send commands to the actor
    cmd_tx: mpsc::Sender<PolymarketCommand>,
    /// Track current subscriptions to compute diffs (wrapped for interior mutability)
    current_subs: tokio::sync::Mutex<HashSet<String>>,
}

impl PolymarketStream {
    pub async fn new(token_ids: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        
        let current_subs: HashSet<String> = token_ids.iter().cloned().collect();

        tokio::spawn(async move {
            let actor = PolymarketActor {
                token_ids,
                tx,
                cmd_rx,
                books: HashMap::new(),
            };
            actor.run().await;
        });

        Self { 
            receiver: tokio::sync::Mutex::new(rx), 
            cmd_tx, 
            current_subs: tokio::sync::Mutex::new(current_subs),
        }
    }
}


// --- The Actor ---

struct PolymarketActor {
    token_ids: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    cmd_rx: mpsc::Receiver<PolymarketCommand>,
    books: HashMap<String, LocalOrderBook>,
}

impl PolymarketActor {
    async fn run(mut self) {
        let url = Url::parse(POLY_WS_URL).unwrap();
        
        // Track current subscriptions
        let mut current_subs: HashSet<String> = self.token_ids.iter().cloned().collect();

        loop {
            // Get the current set of tokens to subscribe to
            let tokens_to_subscribe: Vec<String> = current_subs.iter().cloned().collect();
            
            if tokens_to_subscribe.is_empty() {
                // No subscriptions - just wait for commands
                debug!("Polymarket: No subscriptions, waiting for commands...");
                match self.cmd_rx.recv().await {
                    Some(PolymarketCommand::UpdateSubscriptions(new_subs)) => {
                        info!("Polymarket: Received subscription update ({} tokens)", new_subs.len());
                        current_subs = new_subs;
                        continue;
                    }
                    None => {
                        info!("Polymarket: Command channel closed, shutting down actor");
                        return;
                    }
                }
            }
            
            // Flag to indicate we should reconnect with new subscriptions (skip backoff)
            let mut skip_backoff;
            
            info!("Polymarket: Connecting with {} tokens...", tokens_to_subscribe.len());
            match connect_async(&url).await {
                Ok((ws_stream, _)) => {
                    info!("Polymarket: Connected.");
                    let (mut write, mut read) = ws_stream.split();
                    skip_backoff = false;

                    // 1. Subscribe
                    let sub_msg = json!({
                        "type": "Market",
                        "assets_ids": tokens_to_subscribe
                    });

                    if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                        error!("Polymarket: Sub failed: {}", e);
                        continue;
                    }
                    
                    info!("Polymarket: Subscribed to {} tokens", tokens_to_subscribe.len());

                    // 2. Process loop with command handling
                    loop {
                        tokio::select! {
                            // Handle incoming WebSocket messages
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        // Poly sends an array of events usually
                                        let events: Vec<PolyMessage> = match serde_json::from_str(&text) {
                                            Ok(e) => e,
                                            Err(_) => {
                                                // Sometimes it sends a single object, not array
                                                match serde_json::from_str::<PolyMessage>(&text) {
                                                    Ok(e) => vec![e],
                                                    Err(e) => {
                                                        warn!("Polymarket: Parse error: {}", e);
                                                        continue;
                                                    }
                                                }
                                            }
                                        };

                                        for event in events {
                                            self.process_event(event).await;
                                        }
                                    }
                                    Some(Err(e)) => {
                                        error!("Polymarket: WS Error: {}", e);
                                        break;
                                    }
                                    None => {
                                        info!("Polymarket: Stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            // Handle subscription commands
                            cmd = self.cmd_rx.recv() => {
                                match cmd {
                                    Some(PolymarketCommand::UpdateSubscriptions(new_subs)) => {
                                        if new_subs != current_subs {
                                            info!("Polymarket: Subscription update received ({} -> {} tokens), reconnecting...", 
                                                current_subs.len(), new_subs.len());
                                            current_subs = new_subs;
                                            skip_backoff = true;
                                            // Clear order books for removed tokens
                                            self.books.retain(|k, _| current_subs.contains(k));
                                            break; // Break to reconnect with new subscriptions
                                        }
                                    }
                                    None => {
                                        info!("Polymarket: Command channel closed, shutting down actor");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Polymarket: Connect Error: {}", e);
                    skip_backoff = false;
                }
            }

            if !skip_backoff {
                warn!("Polymarket: Reconnecting in 5s...");
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    async fn process_event(&mut self, event: PolyMessage) {
        let mut dirty_token = None;

        match event {
            PolyMessage::Book(b) if b.event_type == "book" => {
                let book = self
                    .books
                    .entry(b.asset_id.clone())
                    .or_insert_with(LocalOrderBook::new);
                book.set_snapshot(b.clone());
                dirty_token = Some((b.asset_id, b.timestamp));
            }
            PolyMessage::PriceChange(pc) if pc.event_type == "price_change" => {
                let book = self
                    .books
                    .entry(pc.asset_id.clone())
                    .or_insert_with(LocalOrderBook::new);

                for change in pc.price_changes {
                    book.apply_delta(change);
                }
                dirty_token = Some((pc.asset_id, pc.timestamp));
            }
            _ => {} // Ignore acks, heartbeats, tick data, etc.
        }

        // If book changed, emit MarketEvent
        if let Some((token_id, ts_str)) = dirty_token {
            if let Some(book) = self.books.get(&token_id) {
                let event = MarketEvent {
                    timestamp: ts_str.parse().unwrap_or(0),
                    instrument: Instrument::Polymarket(token_id),
                    best_bid: book.get_best_bid(),
                    best_ask: book.get_best_ask(),
                    delta: None, // Poly doesn't give greeks directly
                    mark_iv: None,
                    bid_iv: None,
                    ask_iv: None,
                    underlying_price: None,
                };

                let _ = self.tx.send(event).await;
            }
        }
    }
}

#[async_trait]
impl MarketStream for PolymarketStream {
    async fn next(&self) -> Option<MarketEvent> {
        self.receiver.lock().await.recv().await
    }

    async fn subscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        let poly_tokens: Vec<String> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Polymarket(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        if poly_tokens.is_empty() {
            return Ok(());
        }
        
        // Add new tokens to our local tracking
        let mut current_subs = self.current_subs.lock().await;
        let mut new_subs = current_subs.clone();
        let mut changed = false;
        for token in poly_tokens {
            if new_subs.insert(token) {
                changed = true;
            }
        }
        
        if !changed {
            return Ok(());
        }
        
        debug!("PolymarketStream: Updating subscriptions, now {} tokens", new_subs.len());
        
        // Send the full new set to actor (it will reconnect with the new set)
        self.cmd_tx
            .send(PolymarketCommand::UpdateSubscriptions(new_subs.clone()))
            .await
            .map_err(|e| format!("Failed to send subscribe command: {}", e))?;
        
        *current_subs = new_subs;
        Ok(())
    }

    async fn unsubscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        let poly_tokens: Vec<String> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Polymarket(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        if poly_tokens.is_empty() {
            return Ok(());
        }
        
        // Remove tokens from our local tracking
        let mut current_subs = self.current_subs.lock().await;
        let mut new_subs = current_subs.clone();
        let mut changed = false;
        for token in poly_tokens {
            if new_subs.remove(&token) {
                changed = true;
            }
        }
        
        if !changed {
            return Ok(());
        }
        
        debug!("PolymarketStream: Updating subscriptions, now {} tokens", new_subs.len());
        
        // Send the full new set to actor (it will reconnect with the new set)
        self.cmd_tx
            .send(PolymarketCommand::UpdateSubscriptions(new_subs.clone()))
            .await
            .map_err(|e| format!("Failed to send unsubscribe command: {}", e))?;
        
        *current_subs = new_subs;
        Ok(())
    }
}

// --- Execution Client using Official Polymarket SDK ---

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{Amount, OrderType as PolyOrderType, Side as PolySide};
use polymarket_client_sdk::clob::{Client as PolyClient, Config as PolyConfig};
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::POLYGON;
use rust_decimal::prelude::FromPrimitive;
use std::str::FromStr;

/// Type alias for the authenticated Polymarket client.
type AuthenticatedPolyClient = PolyClient<Authenticated<Normal>>;

/// Polymarket execution client using the official SDK.
/// 
/// Clone + thread-safe for sharing across strategies.
/// If no private key is provided, trading is disabled and orders will fail.
#[derive(Clone)]
pub struct PolymarketExec {
    inner: Arc<PolymarketExecInner>,
}

/// Inner state - uses RwLock because we need interior mutability for the async client.
struct PolymarketExecInner {
    /// The authenticated client (None if trading is disabled)
    client: Option<AuthenticatedPolyClient>,
    /// The signer for signing orders (None if trading is disabled)
    signer: Option<PrivateKeySigner>,
    /// Whether trading is enabled
    trading_enabled: bool,
    /// Optional catalog for order validation (validates constraints if set)
    /// Uses RwLock to allow setting after construction
    catalog: RwLock<Option<Arc<PolymarketCatalog>>>,
}

impl PolymarketExec {
    /// Creates a new Polymarket execution client.
    /// 
    /// # Arguments
    /// * `private_key` - Ethereum private key (hex string with or without 0x prefix).
    ///                   If empty or "read_only", trading will be disabled.
    /// 
    /// # Trading Disabled Mode
    /// When no valid private key is provided, the client operates in read-only mode:
    /// - All `place_order` calls will return an error
    /// - Market data streaming (via PolymarketStream) still works normally
    pub async fn new(private_key: String) -> Self {
        // Check if trading should be disabled
        if private_key.is_empty() || private_key == "read_only" {
            info!("PolymarketExec: Trading disabled (no private key)");
            return Self {
                inner: Arc::new(PolymarketExecInner {
                    client: None,
                    signer: None,
                    trading_enabled: false,
                    catalog: RwLock::new(None),
                }),
            };
        }

        // Try to create signer from private key
        let signer = match PrivateKeySigner::from_str(&private_key) {
            Ok(s) => s.with_chain_id(Some(POLYGON)),
            Err(e) => {
                error!("PolymarketExec: Invalid private key: {}. Trading disabled.", e);
                return Self {
                    inner: Arc::new(PolymarketExecInner {
                        client: None,
                        signer: None,
                        trading_enabled: false,
                        catalog: RwLock::new(None),
                    }),
                };
            }
        };

        // Create the CLOB client
        let config = PolyConfig::builder().use_server_time(true).build();
        let base_client = match PolyClient::new("https://clob.polymarket.com", config) {
            Ok(c) => c,
            Err(e) => {
                error!("PolymarketExec: Failed to create client: {}. Trading disabled.", e);
                return Self {
                    inner: Arc::new(PolymarketExecInner {
                        client: None,
                        signer: None,
                        trading_enabled: false,
                        catalog: RwLock::new(None),
                    }),
                };
            }
        };

        // Authenticate with Polymarket
        let authenticated_client = match base_client
            .authentication_builder(&signer)
            .authenticate()
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("PolymarketExec: Authentication failed: {}. Trading disabled.", e);
                return Self {
                    inner: Arc::new(PolymarketExecInner {
                        client: None,
                        signer: None,
                        trading_enabled: false,
                        catalog: RwLock::new(None),
                    }),
                };
            }
        };

        info!("PolymarketExec: Authenticated successfully, trading enabled");
        
        Self {
            inner: Arc::new(PolymarketExecInner {
                client: Some(authenticated_client),
                signer: Some(signer),
                trading_enabled: true,
                catalog: RwLock::new(None),
            }),
        }
    }

    /// Wraps this client in an Arc for use as SharedExecutionClient.
    pub fn shared(self) -> SharedExecutionClient {
        Arc::new(self)
    }

    /// Returns whether trading is enabled.
    pub fn is_trading_enabled(&self) -> bool {
        self.inner.trading_enabled
    }

    /// Sets the catalog for order validation.
    /// 
    /// When a catalog is set, `place_order` will validate orders against
    /// market constraints (minimum_order_size, minimum_tick_size) before
    /// submitting them to the exchange.
    /// 
    /// This should be called after the catalog is initialized, typically
    /// during application startup.
    pub async fn set_catalog(&self, catalog: Arc<PolymarketCatalog>) {
        let mut guard = self.inner.catalog.write().await;
        *guard = Some(catalog);
        info!("PolymarketExec: Catalog set for order validation");
    }

    /// Validates an order against market constraints.
    /// 
    /// Returns Ok(()) if valid, or an error message if invalid.
    /// If no catalog is set, logs a warning and returns Ok.
    async fn validate_order(&self, order: &Order, estimated_price: Option<f64>) -> Result<(), String> {
        let catalog_guard = self.inner.catalog.read().await;
        let catalog = match catalog_guard.as_ref() {
            Some(c) => c,
            None => {
                warn!("PolymarketExec: No catalog set, skipping order validation");
                return Ok(());
            }
        };

        // Extract token_id from instrument
        let token_id = match &order.instrument {
            Instrument::Polymarket(id) => id,
            _ => return Err(format!("Invalid instrument for Polymarket: {:?}", order.instrument)),
        };

        // Look up market by token_id
        let market = catalog.find_by_token_id(token_id)
            .ok_or_else(|| format!("Market not found for token_id: {}", token_id))?;

        // Validate using the catalog validation function
        validate_polymarket_order(order, &market, estimated_price)
            .map_err(|e| e.to_string())
    }
}

#[async_trait]
impl ExecutionClient for PolymarketExec {
    async fn place_order(&self, order: Order) -> Result<OrderId, String> {
        // Check if trading is enabled
        if !self.inner.trading_enabled {
            return Err("Trading disabled: no private key configured".to_string());
        }

        // Validate order against market constraints (if catalog is available)
        // For limit orders, use the order price for tick size validation.
        // For market orders, we cannot validate the $1 minimum here because we don't
        // have access to current market prices. The exchange will reject invalid orders.
        let estimated_price = order.price;
        if order.order_type == crate::models::OrderType::Market && estimated_price.is_none() {
            warn!("PolymarketExec: Cannot validate market order minimum ($1) without price estimate");
        }
        self.validate_order(&order, estimated_price).await?;

        let signer = self.inner.signer.as_ref()
            .ok_or_else(|| "Signer not available".to_string())?;

        let client = self.inner.client.as_ref()
            .ok_or_else(|| "Client not initialized".to_string())?;

        // Extract token_id from instrument
        let token_id = match &order.instrument {
            Instrument::Polymarket(id) => id.clone(),
            _ => return Err(format!("Invalid instrument for Polymarket: {:?}", order.instrument)),
        };

        // Convert order side
        let side = match order.side {
            crate::models::OrderSide::Buy => PolySide::Buy,
            crate::models::OrderSide::Sell => PolySide::Sell,
        };

        // Convert quantity to Decimal
        let size = PolyDecimal::from_f64(order.quantity)
            .ok_or_else(|| format!("Invalid quantity: {}", order.quantity))?;

        // Build and sign the order based on type
        let signed_order = match order.order_type {
            crate::models::OrderType::Market => {
                // Market orders use shares (token quantity), not USDC amount.
                // Note: Polymarket SDK requires sell orders to use shares, not USDC.
                let amount = Amount::shares(size)
                    .map_err(|e| format!("Invalid amount: {}", e))?;
                
                let market_order = client
                    .market_order()
                    .token_id(&token_id)
                    .amount(amount)
                    .side(side)
                    .build()
                    .await
                    .map_err(|e| format!("Failed to build market order: {}", e))?;
                
                client.sign(signer, market_order).await
                    .map_err(|e| format!("Failed to sign market order: {}", e))?
            }
            crate::models::OrderType::Limit => {
                let price = order.price
                    .ok_or_else(|| "Limit order requires price".to_string())?;
                
                let price_decimal = rust_decimal::Decimal::from_f64(price)
                    .ok_or_else(|| format!("Invalid price: {}", price))?;

                let limit_order = client
                    .limit_order()
                    .token_id(&token_id)
                    .order_type(PolyOrderType::GTC) // Good-til-cancelled
                    .price(price_decimal)
                    .size(size)
                    .side(side)
                    .build()
                    .await
                    .map_err(|e| format!("Failed to build limit order: {}", e))?;
                
                client.sign(signer, limit_order).await
                    .map_err(|e| format!("Failed to sign limit order: {}", e))?
            }
        };

        // Submit the order
        let response = client.post_order(signed_order).await
            .map_err(|e| format!("Failed to post order: {}", e))?;

        info!("PolymarketExec: Order placed successfully: {:?}", response);
        
        // Return the order ID from the response
        Ok(response.order_id.to_string())
    }

    async fn cancel_order(&self, _order_id: &OrderId, _instrument: &Instrument) -> Result<(), String> {
        Err("Not implemented: cancel_order".to_string())
    }

    async fn get_position(&self, _instrument: &Instrument) -> Result<Position, String> {
        Err("Not implemented: get_position".to_string())
    }

    async fn get_positions(&self) -> Result<Vec<Position>, String> {
        Err("Not implemented: get_positions".to_string())
    }

    async fn get_balance(&self) -> Result<f64, String> {
        Err("Not implemented: get_balance".to_string())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // LocalOrderBook Tests
    // -------------------------------------------------------------------------

    mod local_order_book {
        use super::*;

        #[test]
        fn new_creates_empty_book() {
            let book = LocalOrderBook::new();
            assert!(book.bids.is_empty());
            assert!(book.asks.is_empty());
            assert_eq!(book.get_best_bid(), None);
            assert_eq!(book.get_best_ask(), None);
        }

        #[test]
        fn price_to_key_converts_correctly() {
            // Standard prices
            assert_eq!(LocalOrderBook::price_to_key("0.5"), Some(500));
            assert_eq!(LocalOrderBook::price_to_key("0.123"), Some(123));
            assert_eq!(LocalOrderBook::price_to_key("1.0"), Some(1000));
            assert_eq!(LocalOrderBook::price_to_key("0.0"), Some(0));
            assert_eq!(LocalOrderBook::price_to_key("0.001"), Some(1));

            // Rounding behavior
            assert_eq!(LocalOrderBook::price_to_key("0.5555"), Some(556)); // rounds up
            assert_eq!(LocalOrderBook::price_to_key("0.5554"), Some(555)); // rounds down
        }

        #[test]
        fn price_to_key_handles_invalid_input() {
            assert_eq!(LocalOrderBook::price_to_key(""), None);
            assert_eq!(LocalOrderBook::price_to_key("not_a_number"), None);
            assert_eq!(LocalOrderBook::price_to_key("abc123"), None);
        }

        #[test]
        fn set_snapshot_populates_book() {
            let mut book = LocalOrderBook::new();

            let snapshot = PolyBook {
                event_type: "book".to_string(),
                asset_id: "token123".to_string(),
                bids: vec![
                    PriceLevel { price: "0.45".to_string(), size: "100.0".to_string() },
                    PriceLevel { price: "0.44".to_string(), size: "200.0".to_string() },
                ],
                asks: vec![
                    PriceLevel { price: "0.46".to_string(), size: "150.0".to_string() },
                    PriceLevel { price: "0.47".to_string(), size: "250.0".to_string() },
                ],
                timestamp: "1234567890".to_string(),
            };

            book.set_snapshot(snapshot);

            assert_eq!(book.bids.len(), 2);
            assert_eq!(book.asks.len(), 2);
            assert_eq!(book.get_best_bid(), Some(0.45));
            assert_eq!(book.get_best_ask(), Some(0.46));
        }

        #[test]
        fn set_snapshot_clears_previous_data() {
            let mut book = LocalOrderBook::new();

            // First snapshot
            let snapshot1 = PolyBook {
                event_type: "book".to_string(),
                asset_id: "token123".to_string(),
                bids: vec![
                    PriceLevel { price: "0.40".to_string(), size: "100.0".to_string() },
                ],
                asks: vec![
                    PriceLevel { price: "0.60".to_string(), size: "100.0".to_string() },
                ],
                timestamp: "1".to_string(),
            };
            book.set_snapshot(snapshot1);
            assert_eq!(book.get_best_bid(), Some(0.40));

            // Second snapshot should replace, not append
            let snapshot2 = PolyBook {
                event_type: "book".to_string(),
                asset_id: "token123".to_string(),
                bids: vec![
                    PriceLevel { price: "0.50".to_string(), size: "200.0".to_string() },
                ],
                asks: vec![
                    PriceLevel { price: "0.55".to_string(), size: "200.0".to_string() },
                ],
                timestamp: "2".to_string(),
            };
            book.set_snapshot(snapshot2);

            assert_eq!(book.bids.len(), 1);
            assert_eq!(book.asks.len(), 1);
            assert_eq!(book.get_best_bid(), Some(0.50));
            assert_eq!(book.get_best_ask(), Some(0.55));
        }

        #[test]
        fn set_snapshot_skips_malformed_prices() {
            let mut book = LocalOrderBook::new();

            let snapshot = PolyBook {
                event_type: "book".to_string(),
                asset_id: "token123".to_string(),
                bids: vec![
                    PriceLevel { price: "0.45".to_string(), size: "100.0".to_string() },
                    PriceLevel { price: "invalid".to_string(), size: "200.0".to_string() },
                ],
                asks: vec![
                    PriceLevel { price: "0.55".to_string(), size: "150.0".to_string() },
                ],
                timestamp: "1234567890".to_string(),
            };

            book.set_snapshot(snapshot);

            // Should have only the valid bid
            assert_eq!(book.bids.len(), 1);
            assert_eq!(book.get_best_bid(), Some(0.45));
        }

        #[test]
        fn set_snapshot_skips_malformed_sizes() {
            let mut book = LocalOrderBook::new();

            let snapshot = PolyBook {
                event_type: "book".to_string(),
                asset_id: "token123".to_string(),
                bids: vec![
                    PriceLevel { price: "0.45".to_string(), size: "not_a_size".to_string() },
                ],
                asks: vec![
                    PriceLevel { price: "0.55".to_string(), size: "150.0".to_string() },
                ],
                timestamp: "1234567890".to_string(),
            };

            book.set_snapshot(snapshot);

            assert_eq!(book.bids.len(), 0);
            assert_eq!(book.asks.len(), 1);
        }

        #[test]
        fn apply_delta_adds_buy_level() {
            let mut book = LocalOrderBook::new();

            let delta = PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "100.0".to_string(),
            };

            assert!(book.apply_delta(delta));
            assert_eq!(book.bids.get(&500), Some(&100.0));
            assert_eq!(book.get_best_bid(), Some(0.50));
        }

        #[test]
        fn apply_delta_adds_sell_level() {
            let mut book = LocalOrderBook::new();

            let delta = PriceChangeItem {
                side: "SELL".to_string(),
                price: "0.55".to_string(),
                size: "200.0".to_string(),
            };

            assert!(book.apply_delta(delta));
            assert_eq!(book.asks.get(&550), Some(&200.0));
            assert_eq!(book.get_best_ask(), Some(0.55));
        }

        #[test]
        fn apply_delta_updates_existing_level() {
            let mut book = LocalOrderBook::new();

            // Add initial level
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "100.0".to_string(),
            });

            // Update same price level
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "250.0".to_string(),
            });

            assert_eq!(book.bids.len(), 1);
            assert_eq!(book.bids.get(&500), Some(&250.0));
        }

        #[test]
        fn apply_delta_removes_level_when_size_zero() {
            let mut book = LocalOrderBook::new();

            // Add a level
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "100.0".to_string(),
            });
            assert_eq!(book.bids.len(), 1);

            // Remove with size 0
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "0.0".to_string(),
            });

            assert_eq!(book.bids.len(), 0);
            assert_eq!(book.get_best_bid(), None);
        }

        #[test]
        fn apply_delta_rejects_invalid_side() {
            let mut book = LocalOrderBook::new();

            let delta = PriceChangeItem {
                side: "UNKNOWN".to_string(),
                price: "0.50".to_string(),
                size: "100.0".to_string(),
            };

            assert!(!book.apply_delta(delta));
            assert!(book.bids.is_empty());
            assert!(book.asks.is_empty());
        }

        #[test]
        fn apply_delta_rejects_invalid_price() {
            let mut book = LocalOrderBook::new();

            let delta = PriceChangeItem {
                side: "BUY".to_string(),
                price: "not_valid".to_string(),
                size: "100.0".to_string(),
            };

            assert!(!book.apply_delta(delta));
            assert!(book.bids.is_empty());
        }

        #[test]
        fn apply_delta_rejects_invalid_size() {
            let mut book = LocalOrderBook::new();

            let delta = PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "not_valid".to_string(),
            };

            assert!(!book.apply_delta(delta));
            assert!(book.bids.is_empty());
        }

        #[test]
        fn get_best_bid_returns_highest_price() {
            let mut book = LocalOrderBook::new();

            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.40".to_string(),
                size: "100.0".to_string(),
            });
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.50".to_string(),
                size: "100.0".to_string(),
            });
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.45".to_string(),
                size: "100.0".to_string(),
            });

            // Best bid is highest price
            assert_eq!(book.get_best_bid(), Some(0.50));
        }

        #[test]
        fn get_best_ask_returns_lowest_price() {
            let mut book = LocalOrderBook::new();

            book.apply_delta(PriceChangeItem {
                side: "SELL".to_string(),
                price: "0.60".to_string(),
                size: "100.0".to_string(),
            });
            book.apply_delta(PriceChangeItem {
                side: "SELL".to_string(),
                price: "0.55".to_string(),
                size: "100.0".to_string(),
            });
            book.apply_delta(PriceChangeItem {
                side: "SELL".to_string(),
                price: "0.70".to_string(),
                size: "100.0".to_string(),
            });

            // Best ask is lowest price
            assert_eq!(book.get_best_ask(), Some(0.55));
        }
    }

    // -------------------------------------------------------------------------
    // JSON Deserialization Tests
    // -------------------------------------------------------------------------

    mod deserialization {
        use super::*;

        #[test]
        fn parse_book_message() {
            let json = r#"{
                "event_type": "book",
                "asset_id": "0x123abc",
                "bids": [
                    {"price": "0.45", "size": "1000"},
                    {"price": "0.44", "size": "2000"}
                ],
                "asks": [
                    {"price": "0.46", "size": "1500"},
                    {"price": "0.47", "size": "2500"}
                ],
                "timestamp": "1703782800000"
            }"#;

            let msg: PolyMessage = serde_json::from_str(json).unwrap();
            match msg {
                PolyMessage::Book(book) => {
                    assert_eq!(book.asset_id, "0x123abc");
                    assert_eq!(book.bids.len(), 2);
                    assert_eq!(book.asks.len(), 2);
                    assert_eq!(book.bids[0].price, "0.45");
                    assert_eq!(book.bids[0].size, "1000");
                    assert_eq!(book.timestamp, "1703782800000");
                }
                _ => panic!("Expected Book variant"),
            }
        }

        #[test]
        fn parse_price_change_message() {
            let json = r#"{
                "event_type": "price_change",
                "asset_id": "0x456def",
                "price_changes": [
                    {"side": "BUY", "price": "0.50", "size": "500"},
                    {"side": "SELL", "price": "0.51", "size": "0"}
                ],
                "timestamp": "1703782801000"
            }"#;

            let msg: PolyMessage = serde_json::from_str(json).unwrap();
            match msg {
                PolyMessage::PriceChange(pc) => {
                    assert_eq!(pc.asset_id, "0x456def");
                    assert_eq!(pc.price_changes.len(), 2);
                    assert_eq!(pc.price_changes[0].side, "BUY");
                    assert_eq!(pc.price_changes[0].price, "0.50");
                    assert_eq!(pc.price_changes[0].size, "500");
                    assert_eq!(pc.price_changes[1].side, "SELL");
                    assert_eq!(pc.price_changes[1].size, "0");
                    assert_eq!(pc.timestamp, "1703782801000");
                }
                _ => panic!("Expected PriceChange variant"),
            }
        }

        #[test]
        fn parse_unknown_event_type() {
            let json = r#"{
                "event_type": "last_trade_price",
                "asset_id": "0x789",
                "price": "0.52"
            }"#;

            let msg: PolyMessage = serde_json::from_str(json).unwrap();
            assert!(matches!(msg, PolyMessage::Unknown(_)));
        }

        #[test]
        fn parse_array_of_messages() {
            let json = r#"[
                {
                    "event_type": "book",
                    "asset_id": "0xaaa",
                    "bids": [{"price": "0.40", "size": "100"}],
                    "asks": [{"price": "0.60", "size": "100"}],
                    "timestamp": "1000"
                },
                {
                    "event_type": "price_change",
                    "asset_id": "0xbbb",
                    "price_changes": [{"side": "BUY", "price": "0.41", "size": "50"}],
                    "timestamp": "1001"
                }
            ]"#;

            let msgs: Vec<PolyMessage> = serde_json::from_str(json).unwrap();
            assert_eq!(msgs.len(), 2);
            assert!(matches!(msgs[0], PolyMessage::Book(_)));
            assert!(matches!(msgs[1], PolyMessage::PriceChange(_)));
        }

        #[test]
        fn parse_empty_bids_asks() {
            let json = r#"{
                "event_type": "book",
                "asset_id": "0xempty",
                "bids": [],
                "asks": [],
                "timestamp": "999"
            }"#;

            let msg: PolyMessage = serde_json::from_str(json).unwrap();
            match msg {
                PolyMessage::Book(book) => {
                    assert!(book.bids.is_empty());
                    assert!(book.asks.is_empty());
                }
                _ => panic!("Expected Book variant"),
            }
        }

        #[test]
        fn parse_empty_price_changes() {
            let json = r#"{
                "event_type": "price_change",
                "asset_id": "0xempty",
                "price_changes": [],
                "timestamp": "999"
            }"#;

            let msg: PolyMessage = serde_json::from_str(json).unwrap();
            match msg {
                PolyMessage::PriceChange(pc) => {
                    assert!(pc.price_changes.is_empty());
                }
                _ => panic!("Expected PriceChange variant"),
            }
        }

        #[test]
        fn price_level_parses_string_values() {
            // Polymarket sends prices and sizes as strings
            let json = r#"{"price": "0.123456789", "size": "9999999.99"}"#;

            let level: PriceLevel = serde_json::from_str(json).unwrap();
            assert_eq!(level.price, "0.123456789");
            assert_eq!(level.size, "9999999.99");
        }
    }

    // -------------------------------------------------------------------------
    // Integration: Book + Delta Flow
    // -------------------------------------------------------------------------

    mod integration {
        use super::*;

        #[test]
        fn snapshot_then_deltas_produces_correct_bbo() {
            let mut book = LocalOrderBook::new();

            // Initial snapshot
            let snapshot = PolyBook {
                event_type: "book".to_string(),
                asset_id: "token".to_string(),
                bids: vec![
                    PriceLevel { price: "0.45".to_string(), size: "100.0".to_string() },
                    PriceLevel { price: "0.44".to_string(), size: "200.0".to_string() },
                ],
                asks: vec![
                    PriceLevel { price: "0.55".to_string(), size: "100.0".to_string() },
                    PriceLevel { price: "0.56".to_string(), size: "200.0".to_string() },
                ],
                timestamp: "1".to_string(),
            };
            book.set_snapshot(snapshot);
            assert_eq!(book.get_best_bid(), Some(0.45));
            assert_eq!(book.get_best_ask(), Some(0.55));

            // Delta: Add a better bid
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.46".to_string(),
                size: "50.0".to_string(),
            });
            assert_eq!(book.get_best_bid(), Some(0.46));

            // Delta: Remove the best ask
            book.apply_delta(PriceChangeItem {
                side: "SELL".to_string(),
                price: "0.55".to_string(),
                size: "0.0".to_string(),
            });
            assert_eq!(book.get_best_ask(), Some(0.56));

            // Delta: Update a level size (doesn't change BBO)
            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.44".to_string(),
                size: "500.0".to_string(),
            });
            assert_eq!(book.bids.get(&440), Some(&500.0));
            assert_eq!(book.get_best_bid(), Some(0.46)); // Still 0.46
        }

        #[test]
        fn crossed_book_scenario() {
            // In a real market this shouldn't happen, but test we handle it
            let mut book = LocalOrderBook::new();

            book.apply_delta(PriceChangeItem {
                side: "BUY".to_string(),
                price: "0.60".to_string(),
                size: "100.0".to_string(),
            });
            book.apply_delta(PriceChangeItem {
                side: "SELL".to_string(),
                price: "0.50".to_string(),
                size: "100.0".to_string(),
            });

            // Book is crossed (bid > ask)
            assert_eq!(book.get_best_bid(), Some(0.60));
            assert_eq!(book.get_best_ask(), Some(0.50));
        }
    }
}
