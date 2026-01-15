// src/connectors/deribit.rs

use crate::models::{DeribitResponse, DeribitTickerData, Instrument, MarketEvent, Order, OrderId};
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

const DERIBIT_REST_URL: &str = "https://www.deribit.com/api/v2";

/// Commands sent from the stream to the actor for dynamic subscription management.
#[derive(Debug, Clone)]
pub enum DeribitCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

// =============================================================================
// REST API types for initial snapshot
// =============================================================================

/// Response from Deribit's get_book_summary_by_currency endpoint.
#[derive(Debug, Deserialize)]
struct BookSummaryResponse {
    result: Vec<BookSummaryItem>,
}

/// Individual item from book summary (option ticker data).
#[derive(Debug, Deserialize)]
struct BookSummaryItem {
    instrument_name: String,
    #[serde(default)]
    mark_iv: Option<f64>,
    #[serde(default)]
    bid_iv: Option<f64>,
    #[serde(default)]
    ask_iv: Option<f64>,
    #[serde(default)]
    underlying_price: Option<f64>,
    #[serde(default)]
    best_bid_price: Option<f64>,
    #[serde(default)]
    best_ask_price: Option<f64>,
}

pub struct DeribitStream {
    /// Receiver wrapped in Mutex for interior mutability (allows &self in next())
    receiver: tokio::sync::Mutex<mpsc::Receiver<DeribitTickerData>>,
    /// Channel to send commands to the actor
    cmd_tx: mpsc::Sender<DeribitCommand>,
}

impl DeribitStream {
    pub async fn new(instruments: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        let (cmd_tx, cmd_rx) = mpsc::channel(100);

        tokio::spawn(async move {
            let actor = DeribitActor {
                initial_instruments: instruments,
                tx,
                cmd_rx,
                url: Url::parse("wss://www.deribit.com/ws/api/v2").unwrap(),
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
impl MarketStream for DeribitStream {
    async fn next(&self) -> Option<MarketEvent> {
        // 1. Receive raw data (lock the receiver)
        let raw = self.receiver.lock().await.recv().await?;

        // 2. Convert to standardized MarketEvent with typed Instrument
        Some(MarketEvent {
            timestamp: raw.timestamp,
            instrument: Instrument::Deribit(raw.instrument_name),
            best_bid: raw.best_bid_price,
            best_ask: raw.best_ask_price,
            delta: raw.greeks.and_then(|g| g.delta),
            // Pass through IV data for vol surface calibration
            mark_iv: raw.mark_iv,
            bid_iv: raw.bid_iv,
            ask_iv: raw.ask_iv,
            // Use underlying_price or index_price
            underlying_price: raw.underlying_price.or(raw.index_price),
        })
    }

    async fn subscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        // Filter to only Deribit instruments
        let deribit_instruments: Vec<String> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Deribit(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        if deribit_instruments.is_empty() {
            return Ok(());
        }
        
        debug!("DeribitStream: Sending subscribe command for {} instruments", deribit_instruments.len());
        self.cmd_tx
            .send(DeribitCommand::Subscribe(deribit_instruments))
            .await
            .map_err(|e| format!("Failed to send subscribe command: {}", e))
    }

    async fn unsubscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        // Filter to only Deribit instruments
        let deribit_instruments: Vec<String> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Deribit(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        
        if deribit_instruments.is_empty() {
            return Ok(());
        }
        
        debug!("DeribitStream: Sending unsubscribe command for {} instruments", deribit_instruments.len());
        self.cmd_tx
            .send(DeribitCommand::Unsubscribe(deribit_instruments))
            .await
            .map_err(|e| format!("Failed to send unsubscribe command: {}", e))
    }
}

// --- 3. The Private Actor (Background Task) ---
// This handles the dirty work: Reconnects, JSON parsing, Pings, and dynamic subscriptions.
struct DeribitActor {
    initial_instruments: Vec<String>,
    tx: mpsc::Sender<DeribitTickerData>,
    cmd_rx: mpsc::Receiver<DeribitCommand>,
    url: Url,
}

impl DeribitActor {
    pub async fn run(mut self) {
        // Track current subscriptions for reconnect resubscription
        let mut current_subs: HashSet<String> = self.initial_instruments.iter().cloned().collect();
        
        loop {
            info!("Deribit: Connecting...");
            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    info!("Deribit: Connected.");
                    let (mut write, mut read) = ws_stream.split();

                    // 1. Subscribe to all current instruments on connect/reconnect
                    if !current_subs.is_empty() {
                        let channels: Vec<String> = current_subs
                            .iter()
                            .map(|i| format!("ticker.{}.100ms", i))
                            .collect();

                        let subscribe_msg = json!({
                            "jsonrpc": "2.0",
                            "method": "public/subscribe",
                            "id": 1,
                            "params": {
                                "channels": channels
                            }
                        });

                        if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                            error!("Deribit: Failed to send subscription: {}", e);
                            continue; // Reconnect loop
                        }
                        info!("Deribit: Subscribed to {} instruments on connect", current_subs.len());

                        // 2. Fetch initial snapshot via REST to populate data immediately
                        Self::fetch_initial_snapshot(&self.tx, &current_subs).await;
                    }

                    // 2. Process Loop with command handling
                    loop {
                        tokio::select! {
                            // Handle incoming WebSocket messages
                            msg_result = read.next() => {
                        match msg_result {
                                    Some(Ok(Message::Text(text))) => {
                                // Parse and forward
                                if let Ok(parsed) = serde_json::from_str::<DeribitResponse>(&text) {
                                    if let Some(params) = parsed.params {
                                        let mut data = params.data;
                                        Self::normalize_ivs(&mut data);
                                        if self.tx.send(data).await.is_err() {
                                                    warn!("Deribit: Receiver dropped, closing connection.");
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(_))) => {
                                        // Tungstenite handles Pong automatically
                                    }
                                    Some(Err(e)) => {
                                        error!("Deribit: WebSocket error: {}", e);
                                        break; // Break to trigger reconnect
                                    }
                                    None => {
                                        info!("Deribit: Stream ended");
                                        break; // Break to trigger reconnect
                                    }
                                    _ => {}
                                }
                            }
                            // Handle subscription commands from the stream
                            cmd = self.cmd_rx.recv() => {
                                match cmd {
                                    Some(DeribitCommand::Subscribe(instruments)) => {
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
                                            .map(|i| format!("ticker.{}.100ms", i))
                                            .collect();
                                        
                                        let msg = json!({
                                            "jsonrpc": "2.0",
                                            "method": "public/subscribe",
                                            "id": 2,
                                            "params": { "channels": channels }
                                        });
                                        
                                        if let Err(e) = write.send(Message::Text(msg.to_string())).await {
                                            warn!("Deribit: Failed to send subscribe: {}", e);
                                            break; // Reconnect
                                        }
                                        
                                        // Add to current subs before fetching snapshot
                                        let new_subs_set: HashSet<String> = new_instruments.iter().cloned().collect();
                                        current_subs.extend(new_instruments.clone());
                                        
                                        info!("Deribit: Subscribed to {} new instruments", new_instruments.len());
                                        
                                        // Fetch initial snapshot for newly subscribed instruments
                                        Self::fetch_initial_snapshot(&self.tx, &new_subs_set).await;
                                    }
                                    Some(DeribitCommand::Unsubscribe(instruments)) => {
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
                                            .map(|i| format!("ticker.{}.100ms", i))
                                            .collect();
                                        
                                        let msg = json!({
                                            "jsonrpc": "2.0",
                                            "method": "public/unsubscribe",
                                            "id": 3,
                                            "params": { "channels": channels }
                                        });
                                        
                                        if let Err(e) = write.send(Message::Text(msg.to_string())).await {
                                            warn!("Deribit: Failed to send unsubscribe: {}", e);
                                            break; // Reconnect
                                        }
                                        
                                        info!("Deribit: Unsubscribed from {} instruments", to_unsub.len());
                                        for inst in to_unsub {
                                            current_subs.remove(&inst);
                                        }
                                    }
                                    None => {
                                        // Command channel closed, stream is being dropped
                                        info!("Deribit: Command channel closed, shutting down actor");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Deribit: Connection failed: {}", e);
                }
            }

            // Reconnection Backoff
            warn!("Deribit: Disconnected. Reconnecting in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    fn normalize_ivs(data: &mut DeribitTickerData) {
        // Python: return None if iv is None else iv / 100.0
        if let Some(iv) = data.mark_iv {
            data.mark_iv = Some(iv / 100.0);
        }
        if let Some(iv) = data.bid_iv {
            data.bid_iv = Some(iv / 100.0);
        }
        if let Some(iv) = data.ask_iv {
            data.ask_iv = Some(iv / 100.0);
        }
    }

    /// Fetches initial ticker snapshot via REST API for all BTC options.
    /// This provides immediate data on connect instead of waiting for WebSocket updates.
    async fn fetch_initial_snapshot(
        tx: &mpsc::Sender<DeribitTickerData>,
        subscribed_instruments: &HashSet<String>,
    ) {
        let url = format!(
            "{}/public/get_book_summary_by_currency?currency=BTC&kind=option",
            DERIBIT_REST_URL
        );

        let client = reqwest::Client::new();
        let response = match client.get(&url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("Deribit: Failed to fetch initial snapshot: {}", e);
                return;
            }
        };

        if !response.status().is_success() {
            warn!("Deribit: Snapshot API returned status {}", response.status());
            return;
        }

        let data: BookSummaryResponse = match response.json().await {
            Ok(d) => d,
            Err(e) => {
                warn!("Deribit: Failed to parse snapshot response: {}", e);
                return;
            }
        };

        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut sent_count = 0;

        for item in data.result {
            // Only send data for instruments we're subscribed to
            if !subscribed_instruments.contains(&item.instrument_name) {
                continue;
            }

            // Convert to DeribitTickerData format
            let mut ticker = DeribitTickerData {
                instrument_name: item.instrument_name,
                timestamp: now_ms,
                best_bid_price: item.best_bid_price,
                best_ask_price: item.best_ask_price,
                greeks: None, // Book summary doesn't include greeks
                mark_iv: item.mark_iv,
                bid_iv: item.bid_iv,
                ask_iv: item.ask_iv,
                underlying_price: item.underlying_price,
                index_price: None,
            };

            // Normalize IVs (Deribit returns percentages)
            Self::normalize_ivs(&mut ticker);

            if tx.send(ticker).await.is_err() {
                warn!("Deribit: Receiver dropped during snapshot");
                return;
            }
            sent_count += 1;
        }

        info!("Deribit: Loaded {} tickers from REST snapshot", sent_count);
    }
}

/// Deribit execution client.
/// 
/// This is Clone + thread-safe, allowing multiple strategies to share one connection.
/// Internal state is protected by a Mutex.
#[derive(Clone)]
pub struct DeribitExec {
    inner: Arc<DeribitExecInner>,
}

struct DeribitExecInner {
    api_key: String,
    // In a real implementation:
    // http_client: reqwest::Client,
    // order_count: Mutex<u64>,
}

impl DeribitExec {
    pub async fn new(api_key: String) -> Self {
        Self {
            inner: Arc::new(DeribitExecInner { api_key }),
        }
    }

    /// Wraps this client in an Arc for use as SharedExecutionClient.
    pub fn shared(self) -> SharedExecutionClient {
        Arc::new(self)
    }
}

#[async_trait]
impl ExecutionClient for DeribitExec {
    async fn place_order(&self, _order: Order) -> Result<OrderId, String> {
        // In real code: self.inner.http_client.post(".../buy")...
        info!(
            "LIVE TRADING: Order placed on Deribit with key {}",
            self.inner.api_key
        );
        Ok("ord_12345".to_string())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Greeks;

    // -------------------------------------------------------------------------
    // IV Normalization Tests
    // -------------------------------------------------------------------------

    mod normalize_ivs {
        use super::*;

        #[test]
        fn normalizes_all_iv_fields() {
            let mut data = DeribitTickerData {
                instrument_name: "BTC-29MAR24-60000-C".to_string(),
                timestamp: 1703782800000,
                best_bid_price: Some(0.05),
                best_ask_price: Some(0.06),
                greeks: None,
                mark_iv: Some(65.0),
                bid_iv: Some(62.0),
                ask_iv: Some(68.0),
                underlying_price: Some(95000.0),
                index_price: None,
            };

            DeribitActor::normalize_ivs(&mut data);

            assert_eq!(data.mark_iv, Some(0.65));
            assert_eq!(data.bid_iv, Some(0.62));
            assert_eq!(data.ask_iv, Some(0.68));
        }

        #[test]
        fn handles_none_iv_fields() {
            let mut data = DeribitTickerData {
                instrument_name: "BTC-PERPETUAL".to_string(),
                timestamp: 1703782800000,
                best_bid_price: Some(43000.0),
                best_ask_price: Some(43001.0),
                greeks: None,
                mark_iv: None,
                bid_iv: None,
                ask_iv: None,
                underlying_price: None,
                index_price: None,
            };

            DeribitActor::normalize_ivs(&mut data);

            assert_eq!(data.mark_iv, None);
            assert_eq!(data.bid_iv, None);
            assert_eq!(data.ask_iv, None);
        }

        #[test]
        fn handles_partial_iv_fields() {
            let mut data = DeribitTickerData {
                instrument_name: "BTC-29MAR24-60000-C".to_string(),
                timestamp: 1703782800000,
                best_bid_price: None,
                best_ask_price: None,
                greeks: None,
                mark_iv: Some(50.0),
                bid_iv: None,
                ask_iv: Some(55.0),
                underlying_price: Some(95000.0),
                index_price: None,
            };

            DeribitActor::normalize_ivs(&mut data);

            assert_eq!(data.mark_iv, Some(0.50));
            assert_eq!(data.bid_iv, None);
            assert_eq!(data.ask_iv, Some(0.55));
        }

        #[test]
        fn handles_zero_iv() {
            let mut data = DeribitTickerData {
                instrument_name: "BTC-29MAR24-60000-C".to_string(),
                timestamp: 1703782800000,
                best_bid_price: None,
                best_ask_price: None,
                greeks: None,
                mark_iv: Some(0.0),
                bid_iv: Some(0.0),
                ask_iv: Some(0.0),
                underlying_price: None,
                index_price: None,
            };

            DeribitActor::normalize_ivs(&mut data);

            assert_eq!(data.mark_iv, Some(0.0));
            assert_eq!(data.bid_iv, Some(0.0));
            assert_eq!(data.ask_iv, Some(0.0));
        }
    }

    // -------------------------------------------------------------------------
    // JSON Deserialization Tests
    // -------------------------------------------------------------------------

    mod deserialization {
        use super::*;

        #[test]
        fn parse_ticker_subscription_response() {
            let json = r#"{
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "ticker.BTC-29MAR24-60000-C.100ms",
                    "data": {
                        "instrument_name": "BTC-29MAR24-60000-C",
                        "timestamp": 1703782800000,
                        "best_bid_price": 0.0520,
                        "best_ask_price": 0.0540,
                        "mark_iv": 65.5,
                        "bid_iv": 64.0,
                        "ask_iv": 67.0,
                        "greeks": {
                            "delta": 0.55,
                            "gamma": 0.0001,
                            "theta": -50.0,
                            "vega": 120.0
                        }
                    }
                }
            }"#;

            let response: DeribitResponse = serde_json::from_str(json).unwrap();
            assert_eq!(response.method, Some("subscription".to_string()));

            let params = response.params.unwrap();
            assert_eq!(params.channel, "ticker.BTC-29MAR24-60000-C.100ms");

            let data = params.data;
            assert_eq!(data.instrument_name, "BTC-29MAR24-60000-C");
            assert_eq!(data.timestamp, 1703782800000);
            assert_eq!(data.best_bid_price, Some(0.0520));
            assert_eq!(data.best_ask_price, Some(0.0540));
            assert_eq!(data.mark_iv, Some(65.5));
            assert_eq!(data.bid_iv, Some(64.0));
            assert_eq!(data.ask_iv, Some(67.0));

            let greeks = data.greeks.unwrap();
            assert_eq!(greeks.delta, Some(0.55));
            assert_eq!(greeks.gamma, Some(0.0001));
            assert_eq!(greeks.theta, Some(-50.0));
            assert_eq!(greeks.vega, Some(120.0));
        }

        #[test]
        fn parse_perpetual_ticker_no_greeks() {
            let json = r#"{
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "ticker.BTC-PERPETUAL.100ms",
                    "data": {
                        "instrument_name": "BTC-PERPETUAL",
                        "timestamp": 1703782800000,
                        "best_bid_price": 43250.5,
                        "best_ask_price": 43251.0
                    }
                }
            }"#;

            let response: DeribitResponse = serde_json::from_str(json).unwrap();
            let data = response.params.unwrap().data;

            assert_eq!(data.instrument_name, "BTC-PERPETUAL");
            assert_eq!(data.best_bid_price, Some(43250.5));
            assert_eq!(data.best_ask_price, Some(43251.0));
            assert!(data.greeks.is_none());
            assert!(data.mark_iv.is_none());
        }

        #[test]
        fn parse_response_without_params() {
            // Subscription confirmation response (no params.data)
            let json = r#"{
                "jsonrpc": "2.0",
                "id": 1,
                "result": ["ticker.BTC-PERPETUAL.100ms"]
            }"#;

            let response: DeribitResponse = serde_json::from_str(json).unwrap();
            assert!(response.params.is_none());
            assert!(response.method.is_none());
        }

        #[test]
        fn parse_greeks_with_nulls() {
            let json = r#"{
                "delta": 0.45,
                "gamma": null,
                "theta": -25.0,
                "vega": null
            }"#;

            let greeks: Greeks = serde_json::from_str(json).unwrap();
            assert_eq!(greeks.delta, Some(0.45));
            assert_eq!(greeks.gamma, None);
            assert_eq!(greeks.theta, Some(-25.0));
            assert_eq!(greeks.vega, None);
        }

        #[test]
        fn parse_ticker_with_null_prices() {
            let json = r#"{
                "instrument_name": "BTC-29MAR24-100000-C",
                "timestamp": 1703782800000,
                "best_bid_price": null,
                "best_ask_price": null,
                "mark_iv": 80.0
            }"#;

            let data: DeribitTickerData = serde_json::from_str(json).unwrap();
            assert_eq!(data.instrument_name, "BTC-29MAR24-100000-C");
            assert!(data.best_bid_price.is_none());
            assert!(data.best_ask_price.is_none());
            assert_eq!(data.mark_iv, Some(80.0));
        }

        #[test]
        fn parse_negative_theta() {
            // Options typically have negative theta (time decay)
            let json = r#"{
                "delta": 0.50,
                "gamma": 0.0002,
                "theta": -125.5,
                "vega": 200.0
            }"#;

            let greeks: Greeks = serde_json::from_str(json).unwrap();
            assert_eq!(greeks.theta, Some(-125.5));
        }

        #[test]
        fn parse_deep_itm_option() {
            // Deep in-the-money call with delta near 1.0
            let json = r#"{
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {
                    "channel": "ticker.BTC-29MAR24-30000-C.100ms",
                    "data": {
                        "instrument_name": "BTC-29MAR24-30000-C",
                        "timestamp": 1703782800000,
                        "best_bid_price": 0.3100,
                        "best_ask_price": 0.3150,
                        "mark_iv": 45.0,
                        "greeks": {
                            "delta": 0.95,
                            "gamma": 0.00005,
                            "theta": -10.0,
                            "vega": 25.0
                        }
                    }
                }
            }"#;

            let response: DeribitResponse = serde_json::from_str(json).unwrap();
            let greeks = response.params.unwrap().data.greeks.unwrap();
            assert_eq!(greeks.delta, Some(0.95));
        }
    }
}
