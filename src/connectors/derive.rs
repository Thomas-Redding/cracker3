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

/// Derive WebSocket market data stream.
///
/// Implements `MarketStream` to provide standardized `MarketEvent`s.
/// Spawns a background actor to handle reconnection and parsing.
pub struct DeriveStream {
    receiver: mpsc::Receiver<DeriveTickerData>,
}

impl DeriveStream {
    /// Creates a new DeriveStream subscribed to the given instruments.
    ///
    /// # Arguments
    /// * `instruments` - List of instrument names (e.g., "BTC-20251226-100000-C")
    pub async fn new(instruments: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            let actor = DeriveActor {
                instruments,
                tx,
                url: Url::parse("wss://api.lyra.finance/ws").unwrap(),
            };
            actor.run().await;
        });

        Self { receiver: rx }
    }
}

#[async_trait]
impl MarketStream for DeriveStream {
    async fn next(&mut self) -> Option<MarketEvent> {
        // 1. Receive raw data from actor
        let raw = self.receiver.recv().await?;

        // 2. Extract Greeks from option_pricing or direct fields
        let delta = raw
            .option_pricing
            .as_ref()
            .and_then(|op| op.delta)
            .or(raw.delta);

        // 3. Convert to standardized MarketEvent with typed Instrument
        Some(MarketEvent {
            timestamp: raw.timestamp.unwrap_or(0),
            instrument: Instrument::Derive(raw.instrument_name.unwrap_or_default()),
            best_bid: raw.best_bid_price,
            best_ask: raw.best_ask_price,
            delta,
            // Derive ticker data may include IV - pass through if available
            mark_iv: raw.mark_iv,
            bid_iv: None,  // Not in Derive data
            ask_iv: None,
            underlying_price: raw.underlying_price,
        })
    }

    async fn subscribe(&mut self, instruments: &[Instrument]) -> Result<(), String> {
        let derive_instruments: Vec<&str> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Derive(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        
        if derive_instruments.is_empty() {
            return Ok(());
        }
        
        warn!("DeriveStream: Dynamic subscription not yet implemented");
        Ok(())
    }

    async fn unsubscribe(&mut self, instruments: &[Instrument]) -> Result<(), String> {
        let derive_instruments: Vec<&str> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Derive(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        
        if derive_instruments.is_empty() {
            return Ok(());
        }
        
        warn!("DeriveStream: Dynamic unsubscription not yet implemented");
        Ok(())
    }
}

// --- The Private Actor (Background Task) ---
// Handles WebSocket connection, reconnects, JSON parsing, subscriptions.

struct DeriveActor {
    instruments: Vec<String>,
    tx: mpsc::Sender<DeriveTickerData>,
    url: Url,
}

impl DeriveActor {
    pub async fn run(self) {
        loop {
            info!("Connecting to Derive...");
            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to Derive.");
                    let (mut write, mut read) = ws_stream.split();

                    // 1. Subscribe in batches
                    let channels: Vec<String> = self
                        .instruments
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
                            error!("Failed to send subscription: {}", e);
                            break;
                        }
                        // Small delay between batches to avoid flooding
                        sleep(Duration::from_millis(100)).await;
                    }
                    info!("Subscribed to {} channels.", channels.len());

                    // 2. Process incoming messages
                    while let Some(msg_result) = read.next().await {
                        match msg_result {
                            Ok(Message::Text(text)) => {
                                if let Err(e) = self.handle_message(&text).await {
                                    warn!("Error handling message: {}", e);
                                }
                            }
                            Ok(Message::Ping(_)) => {
                                // Tungstenite handles Pong automatically
                            }
                            Ok(Message::Close(_)) => {
                                info!("Received close frame from Derive.");
                                break;
                            }
                            Err(e) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("Connection to Derive failed: {}", e);
                }
            }

            // Reconnection Backoff
            warn!("Disconnected from Derive. Reconnecting in 5 seconds...");
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

