// src/connectors/backtest.rs

use crate::models::{Instrument, MarketEvent, Order, OrderId};
use crate::traits::{ExecutionClient, MarketStream, SharedExecutionClient};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

// --- 1. The Mock Stream ---
pub struct BacktestStream {
    // We use VecDeque to pop events off one by one, wrapped for interior mutability
    events: Mutex<VecDeque<MarketEvent>>,
}

impl BacktestStream {
    pub fn new(data: Vec<MarketEvent>) -> Self {
        Self {
            events: Mutex::new(VecDeque::from(data)),
        }
    }
}

#[async_trait]
impl MarketStream for BacktestStream {
    async fn next(&self) -> Option<MarketEvent> {
        // Instantly returns the next event from memory.
        // In a complex backtester, you might simulate "time" delays here.
        self.events.lock().await.pop_front()
    }
}

// --- 2. Historical Stream (File-based) ---

/// Configuration for time-aware playback in HistoricalStream.
#[derive(Debug, Clone)]
pub struct PlaybackConfig {
    /// If true, sleep between events based on timestamp differences.
    pub realtime: bool,
    /// Speed multiplier for realtime playback (1.0 = real speed, 2.0 = 2x speed, etc.)
    pub speed: f64,
}

impl Default for PlaybackConfig {
    fn default() -> Self {
        Self {
            realtime: false,
            speed: 1.0,
        }
    }
}

impl PlaybackConfig {
    /// Creates a config for instant playback (no delays).
    pub fn instant() -> Self {
        Self::default()
    }

    /// Creates a config for realtime playback at the given speed multiplier.
    pub fn realtime(speed: f64) -> Self {
        Self {
            realtime: true,
            speed: speed.max(0.01), // Prevent division by zero
        }
    }
}

/// Internal state for HistoricalStream.
struct HistoricalStreamState {
    reader: BufReader<File>,
    line_buffer: String,
    last_timestamp: Option<i64>,
}

/// A MarketStream that reads historical data from a JSONL file.
/// Each line should be a JSON-serialized MarketEvent.
pub struct HistoricalStream {
    state: Mutex<HistoricalStreamState>,
    playback_config: PlaybackConfig,
}

impl HistoricalStream {
    /// Creates a new HistoricalStream from a file path.
    /// Returns an error if the file cannot be opened.
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            state: Mutex::new(HistoricalStreamState {
                reader: BufReader::new(file),
                line_buffer: String::new(),
                last_timestamp: None,
            }),
            playback_config: PlaybackConfig::default(),
        })
    }

    /// Creates a new HistoricalStream with custom playback configuration.
    pub fn with_config<P: AsRef<Path>>(path: P, config: PlaybackConfig) -> std::io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            state: Mutex::new(HistoricalStreamState {
                reader: BufReader::new(file),
                line_buffer: String::new(),
                last_timestamp: None,
            }),
            playback_config: config,
        })
    }
}

#[async_trait]
impl MarketStream for HistoricalStream {
    async fn next(&self) -> Option<MarketEvent> {
        let mut state = self.state.lock().await;
        loop {
            state.line_buffer.clear();
            // Split borrows: get mutable references to reader and line_buffer separately
            let HistoricalStreamState { ref mut reader, ref mut line_buffer, ref mut last_timestamp } = *state;
            match reader.read_line(line_buffer) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    let line = line_buffer.trim();
                    if line.is_empty() {
                        continue; // Skip empty lines
                    }
                    match serde_json::from_str::<MarketEvent>(line) {
                        Ok(event) => {
                            // Handle time-aware playback
                            if self.playback_config.realtime {
                                if let Some(last_ts) = *last_timestamp {
                                    let delta_ms = event.timestamp - last_ts;
                                    if delta_ms > 0 {
                                        let sleep_ms =
                                            (delta_ms as f64 / self.playback_config.speed) as u64;
                                        // Drop the lock before sleeping to avoid holding it
                                        drop(state);
                                        tokio::time::sleep(
                                            tokio::time::Duration::from_millis(sleep_ms),
                                        )
                                        .await;
                                        // Reacquire to update timestamp
                                        state = self.state.lock().await;
                                    }
                                }
                            }
                            state.last_timestamp = Some(event.timestamp);
                            return Some(event);
                        }
                        Err(e) => {
                            log::warn!("Failed to parse MarketEvent from line: {}", e);
                            continue; // Skip malformed lines
                        }
                    }
                }
                Err(e) => {
                    log::error!("Error reading from historical data file: {}", e);
                    return None;
                }
            }
        }
    }
}

// --- 3. Recording Stream (Data Recorder Utility) ---

/// A wrapper that records all events from an inner MarketStream to a JSONL file.
/// Use this to capture live data for future backtesting.
pub struct RecordingStream<S: MarketStream> {
    inner: S,
    writer: Mutex<BufWriter<File>>,
}

impl<S: MarketStream> RecordingStream<S> {
    /// Creates a new RecordingStream that wraps an existing stream.
    /// Events will be written to the specified file path.
    pub fn new<P: AsRef<Path>>(inner: S, output_path: P) -> std::io::Result<Self> {
        let file = File::create(output_path)?;
        Ok(Self {
            inner,
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    /// Flushes any buffered data to disk.
    pub async fn flush(&self) -> std::io::Result<()> {
        self.writer.lock().await.flush()
    }
}

#[async_trait]
impl<S: MarketStream + Send + Sync> MarketStream for RecordingStream<S> {
    async fn next(&self) -> Option<MarketEvent> {
        let event = self.inner.next().await?;

        // Serialize and write to file
        match serde_json::to_string(&event) {
            Ok(json) => {
                let mut writer = self.writer.lock().await;
                if let Err(e) = writeln!(writer, "{}", json) {
                    log::error!("Failed to write event to recording file: {}", e);
                }
            }
            Err(e) => {
                log::error!("Failed to serialize MarketEvent: {}", e);
            }
        }

        Some(event)
    }

    async fn subscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        self.inner.subscribe(instruments).await
    }

    async fn unsubscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        self.inner.unsubscribe(instruments).await
    }
}

// --- 4. The Mock Execution ---

/// Mock execution client for backtesting.
/// 
/// Clone + thread-safe, tracks all filled orders for PnL calculation.
#[derive(Clone)]
pub struct MockExec {
    inner: Arc<MockExecInner>,
}

struct MockExecInner {
    fills: Mutex<Vec<Order>>,
}

impl MockExec {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockExecInner {
                fills: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Wraps this client in an Arc for use as SharedExecutionClient.
    pub fn shared(self) -> SharedExecutionClient {
        Arc::new(self)
    }

    /// Returns a copy of all filled orders (for PnL analysis after backtest).
    pub async fn get_fills(&self) -> Vec<Order> {
        self.inner.fills.lock().await.clone()
    }
}

impl Default for MockExec {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionClient for MockExec {
    async fn place_order(&self, order: Order) -> Result<OrderId, String> {
        println!("BACKTEST: Filled order for {:?}", order);
        self.inner.fills.lock().await.push(order);
        Ok("mock_ord_1".to_string())
    }
}
