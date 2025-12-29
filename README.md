# Rust Async Trading Engine 🦀 📈

A high-performance, event-driven trading framework built in Rust. This engine is designed to run **multiple strategies concurrently** while sharing a single WebSocket connection per exchange, seamlessly switching between **Live Trading** and **Backtesting**.

It solves the "Borrow Checker" challenges common in Rust trading systems by strictly segregating the **Read Path** (Market Data) from the **Write Path** (Execution).

## Building & Running

```bash
# Build
cargo build

# Backtest (multiple strategies on mock data)
cargo run -- --mode backtest

# Historical backtest from JSONL file
cargo run -- --mode historical-backtest --file data/market_data.jsonl

# Historical backtest with realtime simulation (2x speed)
cargo run -- --mode historical-backtest --file data/market_data.jsonl --realtime --speed 2.0

# Live trading on Deribit
DERIBIT_KEY=your_api_key cargo run -- --mode live-deribit

# Live trading on Polymarket
cargo run -- --mode live-poly

# Live trading on Derive
cargo run -- --mode live-derive

# Run with web dashboard on port 8080
cargo run -- --mode backtest --dashboard 8080
```

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run connector tests only
cargo test --lib connectors::

# Run with output
cargo test -- --nocapture
```

Unit tests cover:
- **Polymarket**: Order book logic (`LocalOrderBook`), JSON deserialization, snapshot/delta flows
- **Deribit**: IV normalization, ticker parsing, Greeks handling
- **Derive**: Instrument filtering

## 🚀 Features

* **Multi-Strategy Engine:** Run multiple strategies concurrently, each declaring which instruments it needs.
* **Subscription Aggregation:** One WebSocket connection per exchange with automatic subscription merging.
* **Unified Interface:** Strategies are agnostic to the environment—same code runs in production and backtests.
* **Async-First:** Built on `Tokio` and `async-trait` for non-blocking I/O.
* **Exchange Support:** Native integration for Deribit, Derive (options/futures), and Polymarket (prediction markets).
* **Historical Data:** Record live streams to JSONL files and replay them for backtesting with optional realtime simulation.
* **Market Discovery:** Search markets by slug, description, or regex patterns via `MarketCatalog` trait.
* **Shared Execution:** Thread-safe execution clients (`SharedExecutionClient`) allow strategies to share connections.
* **Type Safety:** Strong typing for Greeks (`delta`, `gamma`) and Order types prevents logic errors.
* **Web Dashboard:** Real-time web UI with one tab per strategy, WebSocket updates, and embedded frontend.

## 🏗 Architecture

The system uses a **MarketRouter** to fan out market events to multiple strategies:

```mermaid
graph TD
    subgraph Connectors
        DS[DeribitStream]
        DRS[DeriveStream]
        PS[PolymarketStream]
        BS[BacktestStream]
        HS[HistoricalStream<br/>JSONL file reader]
    end

    subgraph Recording
        RS[RecordingStream&lt;S&gt;<br/>Wraps any stream]
    end

    subgraph Engine
        MR[MarketRouter<br/>Aggregates subscriptions<br/>Routes events by instrument]
    end

    subgraph Strategies
        S1[GammaScalp<br/>BTC options]
        S2[MomentumStrategy<br/>ETH options]
    end

    subgraph Execution
        SEC[SharedExecutionClient<br/>Arc&lt;dyn ExecutionClient&gt;]
        DE[DeribitExec]
        DRE[DeriveExec]
        PE[PolymarketExec]
        ME[MockExec]
    end

    DS -->|MarketEvent| MR
    DRS -->|MarketEvent| MR
    PS -->|MarketEvent| MR
    BS -->|MarketEvent| MR
    HS -->|MarketEvent| MR
    
    DS -.->|wrap| RS
    RS -.->|writes to| JSONL[(market_data.jsonl)]

    MR -->|on_event| S1
    MR -->|on_event| S2

    S1 --> SEC
    S2 --> SEC
    SEC --> DE
    SEC --> DRE
    SEC --> PE
    SEC --> ME
```

**Flow:**
1. Each strategy declares `required_subscriptions()` (e.g., `["BTC-29MAR24-60000-C"]`)
2. `MarketRouter` aggregates all subscriptions and creates ONE stream per exchange
3. Incoming `MarketEvent`s are routed only to strategies that subscribed to that instrument
4. Strategies share a `SharedExecutionClient` (`Arc<dyn ExecutionClient>`) for thread-safe order placement

## 📼 Historical Data & Recording

The framework supports file-based backtesting using JSONL (JSON Lines) format, where each line is a serialized `MarketEvent`.

### Data Format

```json
{"timestamp":1700000000,"instrument":"BTC-29MAR24-60000-C","best_bid":100.0,"best_ask":101.0,"delta":0.6}
{"timestamp":1700000001,"instrument":"ETH-29MAR24-4000-C","best_bid":50.0,"best_ask":51.0,"delta":0.3}
{"timestamp":1700000002,"instrument":"BTC-29MAR24-60000-C","best_bid":102.0,"best_ask":103.0,"delta":0.7}
```

### Recording Live Data

Use `RecordingStream` to capture live market data for future backtests:

```rust
use trading_bot::connectors::backtest::RecordingStream;
use trading_bot::connectors::deribit::DeribitStream;

// Wrap any live stream to record its events
let live_stream = DeribitStream::new(instruments).await;
let recording_stream = RecordingStream::new(live_stream, "recorded_data.jsonl")?;

// Events flow through to your strategies AND get saved to disk
let router = MarketRouter::new(recording_stream, strategies);
router.run().await;
```

### Playback Options

| Flag | Description |
|------|-------------|
| `--file <path>` | Path to JSONL data file (required) |
| `--realtime` | Sleep between events based on timestamp deltas |
| `--speed <float>` | Playback speed multiplier (default: 1.0) |

Realtime playback is useful for testing strategies that depend on wall-clock time or rate limiting.

## 📁 Project Structure

```
src/
├── main.rs              # Entry point, mode selection
├── lib.rs               # Module exports
├── models.rs            # MarketEvent, Order, exchange-specific types
├── traits.rs            # Strategy, MarketStream, ExecutionClient, Dashboard traits
├── engine/
│   └── mod.rs           # MarketRouter (pub/sub event distribution)
├── catalog/
│   ├── mod.rs           # MarketCatalog trait + shared types
│   └── polymarket.rs    # Polymarket market discovery
├── dashboard/
│   └── mod.rs           # DashboardServer, REST API, WebSocket, embedded frontend
├── strategy/
│   ├── gamma_scalp.rs   # Delta-based hedging strategy
│   └── momentum.rs      # Price momentum strategy
└── connectors/
    ├── deribit.rs       # Deribit WebSocket + REST
    ├── derive.rs        # Derive (Lyra) WebSocket + REST
    ├── polymarket.rs    # Polymarket CLOB WebSocket
    └── backtest.rs      # BacktestStream, HistoricalStream, RecordingStream, MockExec
```

## 🔌 Adding a New Strategy

1. Create a new file in `src/strategy/`:

```rust
use crate::models::MarketEvent;
use crate::traits::{Dashboard, DashboardSchema, SharedExecutionClient, Strategy, Widget};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MyStrategy {
    name: String,
    instruments: Vec<String>,
    exec: SharedExecutionClient,
    state: Mutex<MyState>,
}

struct MyState {
    trade_count: u64,
}

impl MyStrategy {
    pub fn new(name: impl Into<String>, instruments: Vec<String>, exec: SharedExecutionClient) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            instruments,
            exec,
            state: Mutex::new(MyState { trade_count: 0 }),
        })
    }
}

// Dashboard is required for all strategies
#[async_trait]
impl Dashboard for MyStrategy {
    fn dashboard_name(&self) -> &str { &self.name }
    
    async fn dashboard_state(&self) -> Value {
        let state = self.state.lock().await;
        json!({ "trade_count": state.trade_count })
    }
    
    fn dashboard_schema(&self) -> DashboardSchema {
        DashboardSchema {
            widgets: vec![
                Widget::KeyValue {
                    label: "Trades".to_string(),
                    key: "trade_count".to_string(),
                    format: None,
                },
            ],
        }
    }
}

#[async_trait]
impl Strategy for MyStrategy {
    fn name(&self) -> &str { &self.name }
    
    fn required_subscriptions(&self) -> Vec<String> { 
        self.instruments.clone() 
    }
    
    async fn on_event(&self, event: MarketEvent) {
        // Your logic here
    }
}
```

2. Export it in `src/strategy/mod.rs`
3. Add it to the strategies list in `main.rs`

## 🔌 Adding a New Exchange

Implement the `MarketStream` and `ExecutionClient` traits:

```rust
#[async_trait]
impl MarketStream for MyExchangeStream {
    async fn next(&mut self) -> Option<MarketEvent> { /* ... */ }
}

#[async_trait]
impl ExecutionClient for MyExchangeExec {
    async fn place_order(&self, order: Order) -> Result<OrderId, String> { /* ... */ }
}
```

The `MarketRouter` works with any `MarketStream` implementation.

## 🔍 Market Discovery with Catalogs

The `MarketCatalog` trait enables strategies to discover markets dynamically. This is useful for strategies that need to find new markets as they're created (e.g., daily BTC price prediction markets on Polymarket).

### Usage Example

```rust
use crate::catalog::{MarketCatalog, PolymarketCatalog, TokenInfo};

// Create catalog (loads from cache, auto-refreshes if stale)
let catalog = PolymarketCatalog::new(None).await;

// Search by text (weighted: slug 8x, question 4x, tags 2x, description 1x)
let results = catalog.search("bitcoin price december", 10);
for r in results {
    println!("{}: {}", r.market.slug.unwrap_or_default(), r.score);
}

// Find by exact slug
if let Some(market) = catalog.find_by_slug("will-bitcoin-be-above-100000") {
    // Get tokens by outcome name (not array index!)
    if let Some(yes) = market.yes_token() {
        println!("YES token: {}", yes.token_id);
    }
    if let Some(no) = market.no_token() {
        println!("NO token: {}", no.token_id);
    }
}

// Find by regex pattern (useful for recurring market patterns)
let btc_markets = catalog.find_by_slug_regex(r"^will-bitcoin-be-above-\d+")?;

// For multi-outcome markets (e.g., elections), use token_by_outcome
if let Some(market) = catalog.find_by_slug("who-will-win-2024-election") {
    if let Some(trump) = market.token_by_outcome("Trump") {
        println!("Trump token: {}", trump.token_id);
    }
}

// Manual refresh (runs in background by default if cache is >1 day old)
let count = catalog.refresh().await?;
```

### Available Methods

| Method | Description |
|--------|-------------|
| `search(query, limit)` | Weighted text search across slug, question, tags, description |
| `find_by_slug(slug)` | Exact slug match |
| `find_by_slug_regex(pattern)` | Regex match on slugs |
| `find_by_token_id(token_id)` | Find market containing a token ID |
| `get(id)` | Get market by condition_id |
| `all()` | Get all cached markets |
| `refresh()` | Fetch fresh data from exchange API |
| `last_updated()` | Unix timestamp of last refresh |

The catalog caches to `polymarket_markets.jsonl` and auto-refreshes in the background when the cache is older than 1 day.

### Key Types

- **`TokenInfo`** — A token with its `token_id` and `outcome` (e.g., "Yes", "No", "Trump")
- **`MarketInfo`** — Market metadata with `tokens: Vec<TokenInfo>` preserving outcome associations
- **`MarketCatalog`** — Trait for market discovery (search, regex, lookup)

The catalog caches to `polymarket_markets.jsonl` and auto-refreshes when the cache is older than 1 day.

## 📊 Web Dashboard

Every strategy implements the `Dashboard` trait, enabling a real-time web UI with one tab per strategy.

### Running with Dashboard

Add the `--dashboard <port>` flag to any mode:

```bash
# Backtest with dashboard
cargo run -- --mode backtest --dashboard 8080

# Live trading with dashboard
cargo run -- --mode live-deribit --dashboard 8080

# Historical backtest with dashboard
cargo run -- --mode historical-backtest --file data.jsonl --realtime --dashboard 8080
```

Then open http://localhost:8080 in your browser.

### Dashboard Features

- **Tab-per-strategy UI** — Switch between strategies with a single click
- **Real-time updates** — WebSocket connection pushes state updates every 500ms
- **Automatic metrics** — Numeric fields are displayed as key-value cards
- **Activity log** — Scrolling log of recent signals and events
- **Price/Delta charts** — Visual history of market data

### Dashboard Trait

All strategies must implement `Dashboard`:

```rust
#[async_trait]
pub trait Dashboard: Send + Sync {
    /// Display name for the dashboard tab
    fn dashboard_name(&self) -> &str;

    /// Returns the current state as JSON for the frontend
    async fn dashboard_state(&self) -> Value;

    /// Optional: Define custom layout with widgets
    fn dashboard_schema(&self) -> DashboardSchema {
        DashboardSchema::default()
    }
}
```

### Widget Types

| Widget | Description |
|--------|-------------|
| `KeyValue` | Simple label/value display |
| `Chart` | Time-series line/bar/area chart |
| `Table` | Data table with columns |
| `Log` | Scrolling activity log |
| `Divider` | Visual separator |

### REST API

The dashboard server exposes these endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /` | Embedded frontend HTML |
| `GET /api/strategies` | List all strategies |
| `GET /api/strategies/:name` | Get strategy state |
| `GET /api/strategies/:name/schema` | Get dashboard schema |
| `GET /ws` | WebSocket for real-time updates |

## LLM Context Cheatsheet:

```bash
cat README.md Cargo.toml src/**/*.rs > ignore.txt
```