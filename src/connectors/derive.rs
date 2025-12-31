// src/connectors/derive.rs
//
// Derive (formerly Lyra) exchange connector.
// WebSocket streaming for options market data.

use crate::models::{DeriveResponse, DeriveTickerData, Instrument, MarketEvent, Order, OrderId};
use crate::traits::{ExecutionClient, MarketStream, SharedExecutionClient};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

/// Batch size for subscription requests (avoid flooding the socket).
const BATCH_SIZE: usize = 50;

/// Update interval for ticker_slim subscriptions.
/// See: https://docs.derive.xyz/reference/ticker_slim-instrument_name-interval
const INTERVAL: &str = "1000";

/// Commands sent from the stream to the actor for dynamic subscription management.
#[derive(Debug, Clone)]
pub enum DeriveCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

/// Derive WebSocket market data stream.
///
/// Implements `MarketStream` to provide standardized `MarketEvent`s.
/// Spawns a background actor to handle reconnection and parsing.
pub struct DeriveStream {
    /// Receiver wrapped in Mutex for interior mutability (allows &self in next())
    receiver: tokio::sync::Mutex<mpsc::Receiver<DeriveTickerData>>,
    /// Channel to send commands to the actor
    cmd_tx: mpsc::Sender<DeriveCommand>,
}

impl DeriveStream {
    /// Creates a new DeriveStream subscribed to the given instruments.
    ///
    /// # Arguments
    /// * `instruments` - List of instrument names (e.g., "BTC-20251226-100000-C")
    pub async fn new(instruments: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        let (cmd_tx, cmd_rx) = mpsc::channel(100);

        tokio::spawn(async move {
            let actor = DeriveActor {
                initial_instruments: instruments,
                tx,
                cmd_rx,
                url: Url::parse("wss://api.lyra.finance/ws").unwrap(),
            };
            actor.run().await;
        });

        Self { 
            receiver: tokio::sync::Mutex::new(rx), 
            cmd_tx,
        }
    }
}

#[async_trait]
impl MarketStream for DeriveStream {
    async fn next(&self) -> Option<MarketEvent> {
        // 1. Receive raw data from actor (lock the receiver)
        let raw = self.receiver.lock().await.recv().await?;

        // 2. Extract fields from option_pricing (WebSocket format) or direct fields (REST format)
        // WebSocket data uses nested option_pricing object, REST uses direct fields.
        // Check option_pricing first, then fall back to direct fields.
        let op = raw.option_pricing.as_ref();
        
        let delta = op.and_then(|o| o.delta).or(raw.delta);
        let mark_iv = op.and_then(|o| o.mark_iv).or(raw.mark_iv);
        let bid_iv = op.and_then(|o| o.bid_iv).or(raw.bid_iv);
        let ask_iv = op.and_then(|o| o.ask_iv).or(raw.ask_iv);
        let underlying_price = op.and_then(|o| o.underlying_price).or(raw.underlying_price);

        // 3. Convert to standardized MarketEvent with typed Instrument
        Some(MarketEvent {
            timestamp: raw.timestamp.unwrap_or(0),
            instrument: Instrument::Derive(raw.instrument_name.unwrap_or_default()),
            best_bid: raw.best_bid_price,
            best_ask: raw.best_ask_price,
            delta,
            mark_iv,
            bid_iv,
            ask_iv,
            underlying_price,
        })
    }

    async fn subscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        let derive_instruments: Vec<String> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Derive(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        if derive_instruments.is_empty() {
            return Ok(());
        }
        
        debug!("DeriveStream: Sending subscribe command for {} instruments", derive_instruments.len());
        self.cmd_tx
            .send(DeriveCommand::Subscribe(derive_instruments))
            .await
            .map_err(|e| format!("Failed to send subscribe command: {}", e))
    }

    async fn unsubscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        let derive_instruments: Vec<String> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Derive(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        if derive_instruments.is_empty() {
            return Ok(());
        }
        
        debug!("DeriveStream: Sending unsubscribe command for {} instruments", derive_instruments.len());
        self.cmd_tx
            .send(DeriveCommand::Unsubscribe(derive_instruments))
            .await
            .map_err(|e| format!("Failed to send unsubscribe command: {}", e))
    }
}

// --- The Private Actor (Background Task) ---
// Handles WebSocket connection, reconnects, JSON parsing, subscriptions, and dynamic subscription commands.

struct DeriveActor {
    initial_instruments: Vec<String>,
    tx: mpsc::Sender<DeriveTickerData>,
    cmd_rx: mpsc::Receiver<DeriveCommand>,
    url: Url,
}

impl DeriveActor {
    pub async fn run(mut self) {
        // Track current subscriptions for reconnect resubscription
        let mut current_subs: HashSet<String> = self.initial_instruments.iter().cloned().collect();
        
        loop {
            info!("Derive: Connecting...");
            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    info!("Derive: Connected.");
                    let (mut write, mut read) = ws_stream.split();

                    // 1. Subscribe to all current instruments on connect/reconnect
                    if !current_subs.is_empty() {
                        let channels: Vec<String> = current_subs
                            .iter()
                            .map(|i| format!("ticker_slim.{}.{}", i, INTERVAL))
                            .collect();

                        for batch in channels.chunks(BATCH_SIZE) {
                            let subscribe_msg = json!({
                                "jsonrpc": "2.0",
                                "method": "subscribe",
                                "id": 42,
                                "params": {
                                    "channels": batch
                                }
                            });

                            if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                                error!("Derive: Failed to send subscription: {}", e);
                                break;
                            }
                            // Small delay between batches to avoid flooding
                            sleep(Duration::from_millis(100)).await;
                        }
                        info!("Derive: Subscribed to {} instruments on connect", current_subs.len());
                    }

                    // 2. Process incoming messages with command handling
                    loop {
                        tokio::select! {
                            // Handle incoming WebSocket messages
                            msg_result = read.next() => {
                                match msg_result {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            warn!("Derive: Error handling message: {}", e);
                                        }
                                    }
                                    Some(Ok(Message::Ping(_))) => {
                                        // Tungstenite handles Pong automatically
                                    }
                                    Some(Ok(Message::Close(_))) => {
                                        info!("Derive: Received close frame.");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        error!("Derive: WebSocket error: {}", e);
                                        break;
                                    }
                                    None => {
                                        info!("Derive: Stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            // Handle subscription commands from the stream
                            cmd = self.cmd_rx.recv() => {
                                match cmd {
                                    Some(DeriveCommand::Subscribe(instruments)) => {
                                        // Filter out already subscribed instruments
                                        let new_instruments: Vec<String> = instruments
                                            .into_iter()
                                            .filter(|i| !current_subs.contains(i))
                                            .collect();
                                        
                                        if new_instruments.is_empty() {
                                            continue;
                                        }
                                        
                                        let channels: Vec<String> = new_instruments
                                            .iter()
                                            .map(|i| format!("ticker_slim.{}.{}", i, INTERVAL))
                                            .collect();
                                        
                                        // Subscribe in batches
                                        let mut batch_failed = false;
                                        for batch in channels.chunks(BATCH_SIZE) {
                                            let msg = json!({
                                                "jsonrpc": "2.0",
                                                "method": "subscribe",
                                                "id": 43,
                                                "params": { "channels": batch }
                                            });
                                            
                                            if let Err(e) = write.send(Message::Text(msg.to_string())).await {
                                                warn!("Derive: Failed to send subscribe: {}", e);
                                                batch_failed = true;
                                                break;
                                            }
                                            sleep(Duration::from_millis(100)).await;
                                        }
                                        
                                        if batch_failed {
                                            // Break to outer loop to trigger reconnect
                                            // Don't update current_subs - reconnect will resubscribe properly
                                            break;
                                        }
                                        
                                        info!("Derive: Subscribed to {} new instruments", new_instruments.len());
                                        current_subs.extend(new_instruments);
                                    }
                                    Some(DeriveCommand::Unsubscribe(instruments)) => {
                                        // Filter to only currently subscribed instruments
                                        let to_unsub: Vec<String> = instruments
                                            .into_iter()
                                            .filter(|i| current_subs.contains(i))
                                            .collect();
                                        
                                        if to_unsub.is_empty() {
                                            continue;
                                        }
                                        
                                        let channels: Vec<String> = to_unsub
                                            .iter()
                                            .map(|i| format!("ticker_slim.{}.{}", i, INTERVAL))
                                            .collect();
                                        
                                        // Unsubscribe in batches
                                        let mut batch_failed = false;
                                        for batch in channels.chunks(BATCH_SIZE) {
                                            let msg = json!({
                                                "jsonrpc": "2.0",
                                                "method": "unsubscribe",
                                                "id": 44,
                                                "params": { "channels": batch }
                                            });
                                            
                                            if let Err(e) = write.send(Message::Text(msg.to_string())).await {
                                                warn!("Derive: Failed to send unsubscribe: {}", e);
                                                batch_failed = true;
                                                break;
                                            }
                                            sleep(Duration::from_millis(100)).await;
                                        }
                                        
                                        if batch_failed {
                                            // Break to outer loop to trigger reconnect
                                            // Don't update current_subs - reconnect will resubscribe properly
                                            break;
                                        }
                                        
                                        info!("Derive: Unsubscribed from {} instruments", to_unsub.len());
                                        for inst in to_unsub {
                                            current_subs.remove(&inst);
                                        }
                                    }
                                    None => {
                                        // Command channel closed, stream is being dropped
                                        info!("Derive: Command channel closed, shutting down actor");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Derive: Connection failed: {}", e);
                }
            }

            // Reconnection Backoff
            warn!("Derive: Disconnected. Reconnecting in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn handle_message(&self, text: &str) -> Result<(), String> {
        let parsed: DeriveResponse =
            serde_json::from_str(text).map_err(|e| format!("Parse error: {}", e))?;

        // Check for errors
        if let Some(err) = &parsed.error {
            error!("Derive API error: {:?}", err);
            return Err(format!("API error: {:?}", err));
        }

        // Handle subscription confirmations
        if parsed.method.as_deref() == Some("subscription_confirmed") {
            debug!("Subscription confirmed: {:?}", parsed.result);
            return Ok(());
        }

        // Handle ticker subscription data
        if parsed.method.as_deref() == Some("subscription") {
            if let Some(params) = parsed.params {
                if let Some(channel) = &params.channel {
                    if !channel.starts_with("ticker") {
                        return Ok(()); // Not a ticker update
                    }
                }

                if let Some(mut data) = params.data {
                    // Extract instrument_name from channel if not in payload
                    if data.instrument_name.is_none() {
                        if let Some(channel) = &params.channel {
                            // Channel format: ticker_slim.BTC-20251226-100000-C.1000
                            let parts: Vec<&str> = channel.split('.').collect();
                            if parts.len() > 1 {
                                data.instrument_name = Some(parts[1].to_string());
                            }
                        }
                    }

                    // Send to stream consumer
                    if self.tx.send(data).await.is_err() {
                        warn!("Receiver dropped, closing connection.");
                        return Err("Receiver dropped".to_string());
                    }
                }
            }
        }

        Ok(())
    }
}

// --- Derive Execution Client ---

/// Derive execution client for placing orders.
///
/// This is Clone + thread-safe, allowing multiple strategies to share one connection.
#[derive(Clone)]
pub struct DeriveExec {
    inner: Arc<DeriveExecInner>,
}

struct DeriveExecInner {
    api_key: String,
    // In a real implementation:
    // http_client: reqwest::Client,
    // wallet: ethers::Wallet,
}

impl DeriveExec {
    pub async fn new(api_key: String) -> Self {
        Self {
            inner: Arc::new(DeriveExecInner { api_key }),
        }
    }

    /// Wraps this client in an Arc for use as SharedExecutionClient.
    pub fn shared(self) -> SharedExecutionClient {
        Arc::new(self)
    }
}

#[async_trait]
impl ExecutionClient for DeriveExec {
    async fn place_order(&self, _order: Order) -> Result<OrderId, String> {
        // In real code: Sign order with wallet, POST to Derive API
        info!(
            "LIVE TRADING: Order placed on Derive with key {}",
            self.inner.api_key
        );
        Ok("derive_ord_12345".to_string())
    }
}

// --- Static Helpers ---

/// Response from Derive's get_instruments REST endpoint.
#[derive(Debug, Deserialize)]
struct GetInstrumentsResponse {
    result: Option<Vec<InstrumentInfo>>,
}

#[derive(Debug, Deserialize)]
struct InstrumentInfo {
    instrument_name: String,
}

impl DeriveStream {
    /// Fetches all active options from Derive REST API.
    ///
    /// # Arguments
    /// * `currency` - The underlying currency (e.g., "BTC")
    /// * `expired` - Whether to include expired instruments
    /// * `max_expiry_days` - Optional: only include instruments expiring within this many days
    ///
    /// # Returns
    /// Vector of instrument names (e.g., ["BTC-20251226-100000-C", ...])
    pub async fn get_active_options(
        currency: &str,
        expired: bool,
        max_expiry_days: Option<u32>,
    ) -> Result<Vec<String>, String> {
        let url = format!(
            "https://api.lyra.finance/public/get_instruments?currency={}&instrument_type=option&expired={}",
            currency,
            expired.to_string().to_lowercase()
        );

        let response = reqwest::get(&url)
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let data: GetInstrumentsResponse = response
            .json()
            .await
            .map_err(|e| format!("Parse failed: {}", e))?;

        let instruments = data.result.unwrap_or_default();
        let now = chrono::Utc::now();

        let filtered: Vec<String> = instruments
            .into_iter()
            .filter_map(|info| {
                let name = info.instrument_name;

                // Validate format: BTC-YYYYMMDD-STRIKE-TYPE
                let parts: Vec<&str> = name.split('-').collect();
                if parts.len() < 4 {
                    return None;
                }

                // Check expiry if max_expiry_days is specified
                if let Some(max_days) = max_expiry_days {
                    if let Ok(expiry) = chrono::NaiveDate::parse_from_str(parts[1], "%Y%m%d") {
                        let expiry_dt = expiry.and_hms_opt(8, 0, 0)?;
                        let expiry_utc = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                            expiry_dt,
                            chrono::Utc,
                        );
                        let days_until = (expiry_utc - now).num_days();
                        if days_until < 0 || days_until > max_days as i64 {
                            return None;
                        }
                    }
                }

                Some(name)
            })
            .collect();

        info!(
            "Derive: Found {} instruments for {} (max_expiry_days: {:?})",
            filtered.len(),
            currency,
            max_expiry_days
        );

        Ok(filtered)
    }

    /// Filters instruments by strike range and option type.
    ///
    /// # Arguments
    /// * `instruments` - List of instrument names to filter
    /// * `min_strike` - Minimum strike price (inclusive)
    /// * `max_strike` - Maximum strike price (inclusive)
    /// * `type_limit` - Optional: "C" for calls only, "P" for puts only
    pub fn filter_instruments(
        instruments: &[String],
        min_strike: f64,
        max_strike: f64,
        type_limit: Option<&str>,
    ) -> Vec<String> {
        instruments
            .iter()
            .filter(|inst| {
                let parts: Vec<&str> = inst.split('-').collect();
                if parts.len() < 4 {
                    return false;
                }

                // Strike is the 3rd element (index 2)
                let strike: f64 = match parts[2].parse() {
                    Ok(s) => s,
                    Err(_) => return false,
                };

                let option_type = parts[3];

                if strike < min_strike || strike > max_strike {
                    return false;
                }

                if let Some(limit) = type_limit {
                    if option_type != limit {
                        return false;
                    }
                }

                true
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_instruments() {
        let instruments = vec![
            "BTC-20251226-90000-C".to_string(),
            "BTC-20251226-100000-C".to_string(),
            "BTC-20251226-110000-P".to_string(),
            "BTC-20251226-120000-C".to_string(),
        ];

        // Filter by strike range
        let filtered = DeriveStream::filter_instruments(&instruments, 95000.0, 115000.0, None);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&"BTC-20251226-100000-C".to_string()));
        assert!(filtered.contains(&"BTC-20251226-110000-P".to_string()));

        // Filter by type
        let calls_only =
            DeriveStream::filter_instruments(&instruments, 0.0, f64::INFINITY, Some("C"));
        assert_eq!(calls_only.len(), 3);

        let puts_only =
            DeriveStream::filter_instruments(&instruments, 0.0, f64::INFINITY, Some("P"));
        assert_eq!(puts_only.len(), 1);
    }
}

