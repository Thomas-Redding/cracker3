# Rust Async Trading Engine 🦀 📈

A high-performance, event-driven trading framework built in Rust. This engine is designed to run **multiple strategies concurrently** across **multiple exchanges**, with a **strategy-centric architecture** where strategies declare which exchanges they need and the engine auto-connects.

It solves the "Borrow Checker" challenges common in Rust trading systems by strictly segregating the **Read Path** (Market Data) from the **Write Path** (Execution).

## 🎯 What's New: Strategy-Centric Architecture

The engine now supports a **strategy-first** design:

- **Strategies declare exchanges** via `required_exchanges()` — not the other way around
- **Dynamic instrument discovery** via `discover_subscriptions()` — query catalogs at runtime
- **Typed instruments** with `Instrument::Deribit(...)`, `Instrument::Polymarket(...)`, etc.
- **Execution routing** via `ExecutionRouter` — place orders on any declared exchange
- **Historical catalogs** with time-travel via `as_of(timestamp)` — backtest with past state
- **TOML configuration** — define strategies in a config file, not code

## Building & Running

```bash
# Build
cargo build

# Generate a default config file
cargo run -- --generate-config > strategies.toml

# Live trading with config file
cargo run -- --mode live --config strategies.toml

# Demo mode with mock data
cargo run -- --mode demo

# Backtest from JSONL file
cargo run -- --mode backtest --file recordings/deribit_20251229.jsonl

# Backtest with realtime simulation (2x speed)
cargo run -- --mode backtest --file data.jsonl --realtime --speed 2.0

# Run with web dashboard on port 8080
cargo run -- --mode demo --dashboard 8080
```

### Environment Variables

| Variable | Exchange | Required |
|----------|----------|----------|
| `DERIBIT_KEY` | Deribit | Yes, for live trading |
| `DERIVE_KEY` | Derive | Optional |
| `POLYMARKET_KEY` | Polymarket | Optional |

## 📋 Configuration File

Strategies are defined in TOML format:

```toml
# strategies.toml

[global]
dashboard_port = 8080
subscription_refresh_secs = 60

[[strategies]]
type = "gamma_scalp"
name = "GammaScalp-BTC"
exchanges = ["deribit"]
instruments = ["BTC-29MAR24-60000-C", "BTC-29MAR24-70000-C"]
threshold = 0.5

[[strategies]]
type = "momentum"
name = "Momentum-ETH"
exchanges = ["deribit"]
instruments = ["ETH-29MAR24-4000-C"]
lookback_period = 10
momentum_threshold = 0.02

# Cross-exchange example
[[strategies]]
type = "gamma_scalp"
name = "GammaScalp-Derive"
exchanges = ["derive"]
instruments = ["BTC-20251226-100000-C"]
threshold = 0.3

# Polymarket example
[[strategies]]
type = "momentum"
name = "Momentum-Poly"
exchanges = ["polymarket"]
instruments = ["21742633143463906290569050155826241533067272736897614950488156847949938836455"]
lookback_period = 5
momentum_threshold = 0.01
```

The engine reads this config and:
1. Aggregates `required_exchanges()` from all strategies
2. Connects only to the exchanges that are needed
3. Builds an `ExecutionRouter` with clients for each exchange
4. Routes events to interested strategies

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
- **Catalogs**: Diff computation, time-travel reconstruction

## 🚀 Features

* **Strategy-Centric:** Strategies declare exchanges they need; engine auto-connects.
* **Multi-Exchange:** Run strategies across Deribit, Derive, and Polymarket simultaneously.
* **Typed Instruments:** `Instrument::Deribit("BTC-29MAR24-60000-C")` prevents exchange mix-ups.
* **Execution Router:** Place orders on any exchange via `ExecutionRouter::place_order()`.
* **Historical Catalogs:** Time-travel support with `as_of(timestamp)` for backtests.
* **TOML Config:** Define strategies in config files, not code.
* **Dynamic Subscriptions:** `discover_subscriptions()` called periodically to update subscriptions.
* **Subscription Aggregation:** One WebSocket connection per exchange with automatic subscription merging.
* **Unified Interface:** Same strategy code runs in live trading and backtests.
* **Async-First:** Built on `Tokio` and `async-trait` for non-blocking I/O.
* **Historical Data:** Live modes automatically record to JSONL files in `recordings/`.
* **Market Discovery:** Search markets by slug, description, or regex via `MarketCatalog` trait.
* **Type Safety:** Strong typing for instruments, exchanges, and order types.
* **Web Dashboard:** Real-time web UI with one tab per strategy.

## 🏗 Architecture

The system uses a unified **Engine** to manage multiple strategies across multiple exchanges:

```mermaid
graph TD
    subgraph Config
        TOML[strategies.toml]
    end

    subgraph Connectors
        DS[DeribitStream]
        DRS[DeriveStream]
        PS[PolymarketStream]
    end

    subgraph Catalogs
        DC[DeribitCatalog<br/>with time-travel]
        DRC[DeriveCatalog]
        PC[PolymarketCatalog]
    end

    subgraph Engine
        E[Engine<br/>Multi-exchange orchestrator]
    end

    subgraph Strategies
        S1[GammaScalp<br/>required_exchanges: [Deribit]]
        S2[Momentum<br/>required_exchanges: [Polymarket]]
    end

    subgraph Execution
        ER[ExecutionRouter<br/>Routes by instrument]
        DE[DeribitExec]
        DRE[DeriveExec]
        PE[PolymarketExec]
    end

    TOML --> E
    E --> DS
    E --> DRS
    E --> PS
    
    S1 -.->|discover_subscriptions| DC
    S2 -.->|discover_subscriptions| PC

    DS -->|MarketEvent| E
    DRS -->|MarketEvent| E
    PS -->|MarketEvent| E

    E -->|on_event| S1
    E -->|on_event| S2

    S1 --> ER
    S2 --> ER
    ER --> DE
    ER --> DRE
    ER --> PE
```

**Flow:**
1. Config file defines strategies with their exchanges and instruments
2. Engine aggregates `required_exchanges()` to determine which connections to open
3. Strategies call `discover_subscriptions()` to declare instruments (can query catalogs)
4. Engine subscribes to the superset of all instruments per exchange
5. Incoming `MarketEvent`s are routed to interested strategies
6. Strategies place orders via `ExecutionRouter`, which routes by `instrument.exchange()`

## 📦 Core Types

### Instrument & Exchange

```rust
// Typed instrument prevents exchange mix-ups
pub enum Instrument {
    Deribit(String),      // "BTC-29MAR24-60000-C"
    Polymarket(String),   // token ID
    Derive(String),       // "BTC-20251226-100000-C"
}

pub enum Exchange {
    Deribit,
    Polymarket,
    Derive,
}

// Get exchange from instrument
let exchange = instrument.exchange(); // Exchange::Deribit

// Get symbol
let symbol = instrument.symbol(); // "BTC-29MAR24-60000-C"
```

### Strategy Trait

```rust
#[async_trait]
pub trait Strategy: Dashboard + Send + Sync {
    fn name(&self) -> &str;
    
    /// Static: which exchanges this strategy needs
    fn required_exchanges(&self) -> HashSet<Exchange>;
    
    /// Dynamic: discover instruments via catalog queries
    async fn discover_subscriptions(&self) -> Vec<Instrument>;
    
    /// Called when a market event arrives
    async fn on_event(&self, event: MarketEvent);
}
```

### ExecutionRouter

```rust
pub struct ExecutionRouter {
    clients: HashMap<Exchange, SharedExecutionClient>,
}

impl ExecutionRouter {
    pub async fn place_order(&self, order: Order) -> Result<OrderId, String> {
        let exchange = order.instrument.exchange();
        self.clients.get(&exchange)?
            .place_order(order).await
    }
}
```

## 🕰 Historical Catalogs

Catalogs support **time-travel** for backtesting. You can query the catalog state as it existed at any past timestamp:

```rust
use crate::catalog::{Catalog, PolymarketCatalog};

let catalog = PolymarketCatalog::new(None).await;

// Current state
let current = catalog.current();

// State as of a specific timestamp
let historical = catalog.as_of(1704067200); // Jan 1, 2024

// Refresh and track changes
let diff = catalog.refresh().await?;
println!("Added: {}, Removed: {}, Modified: {}", 
    diff.added.len(), diff.removed.len(), diff.modified.len());
```

### Storage Format (JSONL)

```jsonl
{"type": "current", "timestamp": 1735400000, "items": [...]}
{"type": "diff", "timestamp": 1735313600, "added": [...], "removed": [...], "modified": [...]}
{"type": "diff", "timestamp": 1735227200, ...}
```

To reconstruct historical state: start with current, walk diffs backwards, invert each diff until reaching target timestamp.

## 📼 Historical Data & Recording

### Data Format

```json
{"timestamp":1700000000,"instrument":{"exchange":"deribit","symbol":"BTC-29MAR24-60000-C"},"best_bid":100.0,"best_ask":101.0,"delta":0.6}
```

### Recording Live Data

Live modes automatically record to `recordings/`:

```bash
cargo run -- --mode live --config strategies.toml
# Recording to: recordings/deribit_20251229_143052.jsonl
# Recording to: recordings/polymarket_20251229_143052.jsonl
```

### Playback Options

| Flag | Description |
|------|-------------|
| `--file <path>` | Path to JSONL data file (required) |
| `--realtime` | Sleep between events based on timestamps |
| `--speed <float>` | Playback speed multiplier (default: 1.0) |
| `--as-of <timestamp>` | Use catalog state from this timestamp |

## 📁 Project Structure

```
src/
├── main.rs              # CLI, mode selection
├── lib.rs               # Module exports
├── config.rs            # TOML config parsing, strategy registry
├── models.rs            # Instrument, Exchange, MarketEvent, Order
├── traits.rs            # Strategy, MarketStream, ExecutionClient, ExecutionRouter
├── engine/
│   └── mod.rs           # Unified Engine, MarketRouter (legacy)
├── catalog/
│   ├── mod.rs           # Catalog trait, CatalogDiff, time-travel helpers
│   ├── deribit.rs       # DeribitCatalog
│   ├── derive.rs        # DeriveCatalog
│   └── polymarket.rs    # PolymarketCatalog
├── dashboard/
│   └── mod.rs           # DashboardServer, REST API, WebSocket
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
use crate::models::{Exchange, Instrument, MarketEvent};
use crate::traits::{Dashboard, DashboardSchema, SharedExecutionRouter, Strategy, Widget};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MyStrategy {
    name: String,
    instruments: Vec<Instrument>,
    exchanges: HashSet<Exchange>,
    exec: SharedExecutionRouter,
    state: Mutex<MyState>,
}

struct MyState {
    trade_count: u64,
}

impl MyStrategy {
    pub fn new(
        name: impl Into<String>,
        instruments: Vec<Instrument>,
        exec: SharedExecutionRouter,
    ) -> Arc<Self> {
        let exchanges = instruments.iter().map(|i| i.exchange()).collect();
        Arc::new(Self {
            name: name.into(),
            instruments,
            exchanges,
            exec,
            state: Mutex::new(MyState { trade_count: 0 }),
        })
    }
}

#[async_trait]
impl Dashboard for MyStrategy {
    fn dashboard_name(&self) -> &str { &self.name }
    
    async fn dashboard_state(&self) -> Value {
        let state = self.state.lock().await;
        json!({ "trade_count": state.trade_count })
    }
}

#[async_trait]
impl Strategy for MyStrategy {
    fn name(&self) -> &str { &self.name }
    
    fn required_exchanges(&self) -> HashSet<Exchange> {
        self.exchanges.clone()
    }
    
    async fn discover_subscriptions(&self) -> Vec<Instrument> {
        // Can query catalogs here for dynamic discovery
        self.instruments.clone()
    }
    
    async fn on_event(&self, event: MarketEvent) {
        // Your trading logic here
        // Use self.exec.place_order(order).await to trade
    }
}
```

2. Export it in `src/strategy/mod.rs`
3. Add a config variant in `src/config.rs`

## 🔌 Adding a New Exchange

1. Implement `MarketStream` with typed instruments:

```rust
#[async_trait]
impl MarketStream for MyExchangeStream {
    async fn next(&mut self) -> Option<MarketEvent> {
        let raw = self.receiver.recv().await?;
        Some(MarketEvent {
            timestamp: raw.timestamp,
            instrument: Instrument::MyExchange(raw.symbol),
            best_bid: raw.best_bid,
            best_ask: raw.best_ask,
            delta: raw.delta,
        })
    }
    
    async fn subscribe(&mut self, instruments: &[Instrument]) -> Result<(), String> {
        // Dynamic subscription support
    }
    
    async fn unsubscribe(&mut self, instruments: &[Instrument]) -> Result<(), String> {
        // Dynamic unsubscription support
    }
}
```

2. Implement `ExecutionClient`
3. Add the exchange to `models.rs` (`Exchange` and `Instrument` enums)
4. Update `ExecutionRouter` creation in `main.rs`

## 🔍 Market Discovery with Catalogs

```rust
use crate::catalog::{Catalog, PolymarketCatalog};

let catalog = PolymarketCatalog::new(None).await;

// Search by text
let results = catalog.search("bitcoin price december", 10);

// Find by slug
if let Some(market) = catalog.find_by_slug("will-bitcoin-be-above-100000") {
    if let Some(yes) = market.yes_token() {
        println!("YES token: {}", yes.token_id);
    }
}

// Find by regex
let btc_markets = catalog.find_by_slug_regex(r"^will-bitcoin-be-above-\d+")?;

// Time-travel for backtests
let past_state = catalog.as_of(1704067200); // Jan 1, 2024
```

## 📊 Web Dashboard

```bash
cargo run -- --mode demo --dashboard 8080
# Open http://localhost:8080
```

### REST API

| Endpoint | Description |
|----------|-------------|
| `GET /` | Embedded frontend HTML |
| `GET /api/strategies` | List all strategies |
| `GET /api/strategies/:name` | Get strategy state |
| `GET /api/strategies/:name/schema` | Get dashboard schema |
| `GET /ws` | WebSocket for real-time updates |

## CLI Reference

```
trading-bot [OPTIONS]

Options:
  --mode <MODE>          Mode: live, backtest, demo [default: live]
  --config <FILE>        Path to TOML configuration file
  --strategies <LIST>    Comma-separated strategy names (quick testing)
  --file <FILE>          JSONL file for backtest mode
  --as-of <TIMESTAMP>    Historical catalog timestamp for backtest
  --realtime             Enable realtime playback simulation
  --speed <FLOAT>        Playback speed multiplier [default: 1.0]
  --dashboard <PORT>     Enable web dashboard on PORT
  --generate-config      Print default config template and exit
```

## TODOs

* P1: Implement BTC trading strategy.
* P2: Implement `refresh_subscriptions` in deribit.rs and derive.rs.
* P2: Allow each dashboard to affect each strategy. Especially enabling and disabling.
* P3: Implement real trading for the various exchanges.


## LLM Context Cheatsheet

```bash
cat README.md Cargo.toml src/**/*.rs > ignore.txt
```
