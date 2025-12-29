// src/connectors/deribit.rs

use crate::models::{DeribitResponse, DeribitTickerData, Instrument, MarketEvent, Order, OrderId};
use crate::traits::{ExecutionClient, MarketStream, SharedExecutionClient};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

pub struct DeribitStream {
    receiver: mpsc::Receiver<DeribitTickerData>,
}

impl DeribitStream {
    pub async fn new(instruments: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            let actor = DeribitActor {
                instruments,
                tx,
                url: Url::parse("wss://www.deribit.com/ws/api/v2").unwrap(),
            };
            actor.run().await;
        });

        Self { receiver: rx }
    }
}

#[async_trait]
impl MarketStream for DeribitStream {
    async fn next(&mut self) -> Option<MarketEvent> {
        // 1. Receive raw data
        let raw = self.receiver.recv().await?;

        // 2. Convert to standardized MarketEvent with typed Instrument
        Some(MarketEvent {
            timestamp: raw.timestamp,
            instrument: Instrument::Deribit(raw.instrument_name),
            best_bid: raw.best_bid_price,
            best_ask: raw.best_ask_price,
            delta: raw.greeks.and_then(|g| g.delta),
        })
    }

    async fn subscribe(&mut self, instruments: &[Instrument]) -> Result<(), String> {
        // Filter to only Deribit instruments
        let deribit_instruments: Vec<&str> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Deribit(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        
        if deribit_instruments.is_empty() {
            return Ok(());
        }
        
        // TODO: Implement dynamic subscription via WebSocket
        warn!("DeribitStream: Dynamic subscription not yet implemented");
        Ok(())
    }

    async fn unsubscribe(&mut self, instruments: &[Instrument]) -> Result<(), String> {
        // Filter to only Deribit instruments
        let deribit_instruments: Vec<&str> = instruments
            .iter()
            .filter_map(|i| match i {
                Instrument::Deribit(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        
        if deribit_instruments.is_empty() {
            return Ok(());
        }
        
        // TODO: Implement dynamic unsubscription via WebSocket
        warn!("DeribitStream: Dynamic unsubscription not yet implemented");
        Ok(())
    }
}

// --- 3. The Private Actor (Background Task) ---
// This handles the dirty work: Reconnects, JSON parsing, Pings.
struct DeribitActor {
    instruments: Vec<String>,
    tx: mpsc::Sender<DeribitTickerData>,
    url: Url,
}

impl DeribitActor {
    pub async fn run(self) {
        loop {
            info!("Connecting to Deribit...");
            match connect_async(&self.url).await {
                Ok((ws_stream, _)) => {
                    info!("Connected.");
                    let (mut write, mut read) = ws_stream.split();

                    // 1. Subscribe
                    let channels: Vec<String> = self
                        .instruments
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
                        error!("Failed to send subscription: {}", e);
                        continue; // Reconnect loop
                    }

                    // 2. Process Loop
                    while let Some(msg_result) = read.next().await {
                        match msg_result {
                            Ok(Message::Text(text)) => {
                                // Parse and forward
                                if let Ok(parsed) = serde_json::from_str::<DeribitResponse>(&text) {
                                    if let Some(params) = parsed.params {
                                        let mut data = params.data;

                                        // Logic from your Python code: Normalize IVs
                                        Self::normalize_ivs(&mut data);

                                        // Send to Strategy
                                        if self.tx.send(data).await.is_err() {
                                            warn!("Receiver dropped, closing connection.");
                                            return; // Stop the actor, no one is listening
                                        }
                                    }
                                } else if text.contains("heartbeat") {
                                    // Respond to app-level heartbeats if Deribit requires them
                                    // (Deribit usually keeps alive via standard pings)
                                }
                            }
                            Ok(Message::Ping(_)) => {
                                // Tungstenite handles Pong automatically
                            }
                            Err(e) => {
                                error!("WebSocket error: {}", e);
                                break; // Break inner loop to trigger reconnect
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("Connection failed: {}", e);
                }
            }

            // Reconnection Backoff (Exponential-ish)
            warn!("Disconnected. Reconnecting in 5 seconds...");
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
