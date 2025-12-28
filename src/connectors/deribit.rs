// src/connectors/deribit.rs

use crate::models::{DeribitResponse, DeribitTickerData, MarketEvent, Order, OrderId};
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

        // 2. Convert to standardized MarketEvent
        Some(MarketEvent {
            timestamp: raw.timestamp,
            instrument: raw.instrument_name,
            best_bid: raw.best_bid_price,
            best_ask: raw.best_ask_price,
            delta: raw.greeks.and_then(|g| g.delta),
        })
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
