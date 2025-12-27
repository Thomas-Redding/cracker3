use crate::models::{MarketEvent, Order, OrderId};
use crate::traits::{ExecutionClient, MarketStream};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const POLY_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

// --- Data Models (Internal to Polymarket) ---

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum PolyMessage {
    Book(PolyBook),
    PriceChange(PolyPriceChange),
    // We ignore 'last_trade_price' for now as strategy likely relies on BBO
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Debug, Clone)]
struct PolyBook {
    asset_id: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    timestamp: String, // Poly sends strings "123456789"
}

#[derive(Deserialize, Debug, Clone)]
struct PolyPriceChange {
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

    fn price_to_key(p: &str) -> u64 {
        let f: f64 = p.parse().unwrap_or(0.0);
        (f * 1000.0).round() as u64
    }

    fn set_snapshot(&mut self, data: PolyBook) {
        self.bids.clear();
        self.asks.clear();

        for item in data.bids {
            let key = Self::price_to_key(&item.price);
            let size: f64 = item.size.parse().unwrap_or(0.0);
            self.bids.insert(key, size);
        }
        for item in data.asks {
            let key = Self::price_to_key(&item.price);
            let size: f64 = item.size.parse().unwrap_or(0.0);
            self.asks.insert(key, size);
        }
    }

    fn apply_delta(&mut self, change: PriceChangeItem) {
        let key = Self::price_to_key(&change.price);
        let size: f64 = change.size.parse().unwrap_or(0.0);
        let map = if change.side == "BUY" {
            &mut self.bids
        } else {
            &mut self.asks
        };

        if size == 0.0 {
            map.remove(&key);
        } else {
            map.insert(key, size);
        }
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
    receiver: mpsc::Receiver<MarketEvent>,
}

impl PolymarketStream {
    pub async fn new(token_ids: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            let actor = PolymarketActor {
                token_ids,
                tx,
                books: HashMap::new(),
            };
            actor.run().await;
        });

        Self { receiver: rx }
    }
}

#[async_trait]
impl MarketStream for PolymarketStream {
    async fn next(&mut self) -> Option<MarketEvent> {
        self.receiver.recv().await
    }
}

// --- The Actor ---

struct PolymarketActor {
    token_ids: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    books: HashMap<String, LocalOrderBook>,
}

impl PolymarketActor {
    async fn run(mut self) {
        let url = Url::parse(POLY_WS_URL).unwrap();

        loop {
            info!("Polymarket: Connecting...");
            match connect_async(&url).await {
                Ok((ws_stream, _)) => {
                    info!("Polymarket: Connected.");
                    let (mut write, mut read) = ws_stream.split();

                    // 1. Subscribe
                    let sub_msg = json!({
                        "type": "Market",
                        "assets_ids": self.token_ids
                    });

                    if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                        error!("Polymarket: Sub failed: {}", e);
                        continue;
                    }

                    // 2. Loop
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                // Poly sends an array of events usually
                                let events: Vec<PolyMessage> = match serde_json::from_str(&text) {
                                    Ok(e) => e,
                                    Err(_) => {
                                        // Sometimes it sends a single object, not array
                                        match serde_json::from_str::<PolyMessage>(&text) {
                                            Ok(e) => vec![e],
                                            Err(e) => {
                                                warn!("Poly parse error: {}", e);
                                                continue;
                                            }
                                        }
                                    }
                                };

                                for event in events {
                                    self.process_event(event).await;
                                }
                            }
                            Err(e) => {
                                error!("Poly WS Error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => error!("Poly Connect Error: {}", e),
            }

            warn!("Polymarket: Reconnecting in 5s...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn process_event(&mut self, event: PolyMessage) {
        let mut dirty_token = None;

        match event {
            PolyMessage::Book(b) => {
                let book = self
                    .books
                    .entry(b.asset_id.clone())
                    .or_insert_with(LocalOrderBook::new);
                book.set_snapshot(b.clone());
                dirty_token = Some((b.asset_id, b.timestamp));
            }
            PolyMessage::PriceChange(pc) => {
                let book = self
                    .books
                    .entry(pc.asset_id.clone())
                    .or_insert_with(LocalOrderBook::new);
                
                for change in pc.price_changes {
                    book.apply_delta(change);
                }
                dirty_token = Some((pc.asset_id, pc.timestamp));
            }
            _ => {}
        }

        // If book changed, emit MarketEvent
        if let Some((token_id, ts_str)) = dirty_token {
            if let Some(book) = self.books.get(&token_id) {
                let event = MarketEvent {
                    timestamp: ts_str.parse().unwrap_or(0),
                    instrument: token_id,
                    best_bid: book.get_best_bid(),
                    best_ask: book.get_best_ask(),
                    delta: None, // Poly doesn't give greeks directly
                };
                
                let _ = self.tx.send(event).await;
            }
        }
    }
}

// --- Execution Stub ---
// Note: Real Poly trading requires EIP-712 signing (complex).
// This is a placeholder structure matching your interface.
pub struct PolymarketExec {
    api_key: String,
}

impl PolymarketExec {
    pub async fn new(api_key: String) -> Self {
        Self { api_key }
    }
}

#[async_trait]
impl ExecutionClient for PolymarketExec {
    async fn place_order(&mut self, _order: Order) -> Result<OrderId, String> {
        info!("POLY EXEC: (Stub) Placing order with key {}", self.api_key);
        Ok("poly_fake_id".to_string())
    }
}