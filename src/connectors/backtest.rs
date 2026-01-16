// src/connectors/backtest.rs

use crate::models::{Instrument, MarketEvent, Order, OrderId, Position};
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
    readers: Vec<BufReader<File>>,
    line_buffers: Vec<String>,
    next_events: Vec<Option<MarketEvent>>,
    last_timestamp: Option<i64>,
    total_size_bytes: u64,
    processed_bytes: u64,
    last_progress_log: std::time::Instant,
}

/// A MarketStream that reads historical data from a JSONL file.
/// Each line should be a JSON-serialized MarketEvent.
pub struct HistoricalStream {
    state: Mutex<HistoricalStreamState>,
    playback_config: PlaybackConfig,
}

impl HistoricalStream {
    /// Creates a new HistoricalStream from a list of file paths.
    pub fn new<P: AsRef<Path>>(paths: Vec<P>) -> std::io::Result<Self> {
        Self::with_config(paths, PlaybackConfig::default())
    }

    /// Creates a new HistoricalStream with custom playback configuration.
    pub fn with_config<P: AsRef<Path>>(
        paths: Vec<P>,
        config: PlaybackConfig,
    ) -> std::io::Result<Self> {
        let mut readers = Vec::with_capacity(paths.len());
        let mut line_buffers = Vec::with_capacity(paths.len());
        let mut next_events = Vec::with_capacity(paths.len());
        let mut total_size_bytes: u64 = 0;

        for path in paths {
            let metadata = std::fs::metadata(&path)?;
            total_size_bytes += metadata.len();
            
            let file = File::open(path)?;
            readers.push(BufReader::new(file));
            line_buffers.push(String::new());
            next_events.push(None); // Initially empty, will be filled on first next()
        }

        log::info!("Backtest: Total data size: {:.2} MB", total_size_bytes as f64 / 1_024.0 / 1_024.0);

        Ok(Self {
            state: Mutex::new(HistoricalStreamState {
                readers,
                line_buffers,
                next_events,
                last_timestamp: None,
                total_size_bytes,
                processed_bytes: 0,
                last_progress_log: std::time::Instant::now(),
            }),
            playback_config: config,
        })
    }
}

#[async_trait]
impl MarketStream for HistoricalStream {
    async fn next(&self) -> Option<MarketEvent> {
        let mut state_guard = self.state.lock().await;

        loop {
            let state = &mut *state_guard;
            
            // Check for progress logging (every 5 seconds)
            if state.last_progress_log.elapsed() >= std::time::Duration::from_secs(5) {
                let pct = if state.total_size_bytes > 0 {
                    (state.processed_bytes as f64 / state.total_size_bytes as f64) * 100.0
                } else {
                    0.0
                };
                
                let processed_mb = state.processed_bytes as f64 / 1024.0 / 1024.0;
                let total_mb = state.total_size_bytes as f64 / 1024.0 / 1024.0;
                
                let time_str = state.last_timestamp
                    .map(|ts| chrono::DateTime::from_timestamp(ts / 1000, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or("?".to_string()))
                    .unwrap_or("Start".to_string());

                // Use \r to overwrite line for cleaner progress
                eprint!("\r[progress] {:.1}% complete ({:.1} MB / {:.1} MB) @ {}", 
                    pct, processed_mb, total_mb, time_str);
                // Flush to ensure it prints
                use std::io::Write;
                let _ = std::io::stderr().flush();
                state.last_progress_log = std::time::Instant::now();
            }

            // Split borrows manually to avoid "cannot borrow `*state` as mutable more than once"
            let HistoricalStreamState { 
                ref mut readers, 
                ref mut line_buffers, 
                ref mut next_events, 
                ref mut last_timestamp,
                ref mut processed_bytes,
                ..
            } = state;

            // 1. Refill any empty slots
            let mut all_eof = true;

            // We iterate by index because we need to access parallel vectors
            for i in 0..readers.len() {
                if next_events[i].is_none() {
                    // Try to read next event for this reader
                    loop {
                        let reader = &mut readers[i];
                        let buffer = &mut line_buffers[i];
                        
                        buffer.clear();
                        match reader.read_line(buffer) {
                            Ok(0) => break, // EOF for this file
                            Ok(n) => {
                                *processed_bytes += n as u64;
                                let line = buffer.trim();
                                if line.is_empty() {
                                    continue;
                                }
                                match serde_json::from_str::<MarketEvent>(line) {
                                    Ok(event) => {
                                        next_events[i] = Some(event);
                                        break; // Found valid event
                                    }
                                    Err(e) => {
                                        log::warn!("Failed to parse MarketEvent: {}", e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Error reading historical file: {}", e);
                                break;
                            }
                        }
                    }
                }
                
                if next_events[i].is_some() {
                    all_eof = false;
                }
            }

            if all_eof {
                return None;
            }

            // 2. Find the event with minimum timestamp
            let mut min_ts = i64::MAX;
            let mut best_idx = None;

            for (i, event_opt) in next_events.iter().enumerate() {
                if let Some(event) = event_opt {
                    if event.timestamp < min_ts {
                        min_ts = event.timestamp;
                        best_idx = Some(i);
                    }
                }
            }

            // 3. Return and advance
            if let Some(idx) = best_idx {
                let event = next_events[idx].take().unwrap();

                // Handle delay
                // Note: We hold the lock while sleeping. This prevents other consumers from
                // racing to get the next event, which preserves strict ordering.
                if self.playback_config.realtime {
                    if let Some(last_ts) = *last_timestamp {
                        let delta_ms = event.timestamp - last_ts;
                        if delta_ms > 0 {
                            let sleep_ms = (delta_ms as f64 / self.playback_config.speed) as u64;
                            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                        }
                    }
                }

                *last_timestamp = Some(event.timestamp);
                return Some(event);
            } else {
                return None;
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
        log::info!("BACKTEST: Filled order for {:?}", order);
        self.inner.fills.lock().await.push(order);
        Ok("mock_ord_1".to_string())
    }

    async fn cancel_order(&self, _order_id: &OrderId, _instrument: &Instrument) -> Result<(), String> {
        Ok(())
    }

    async fn get_position(&self, _instrument: &Instrument) -> Result<Position, String> {
        // Return a zero position for testing if needed, or Err
        Err("Not implemented in MockExec".to_string())
    }

    async fn get_positions(&self) -> Result<Vec<Position>, String> {
        Ok(Vec::new())
    }

    async fn get_balance(&self) -> Result<f64, String> {
        Ok(1_000_000.0) // Return a default large balance for tests
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_event(ts: i64) -> MarketEvent {
        MarketEvent {
            timestamp: ts,
            instrument: Instrument::Deribit("BTC-PERPETUAL".to_string()),
            best_bid: Some(100.0),
            best_ask: Some(101.0),
            delta: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            underlying_price: None,
        }
    }

    #[tokio::test]
    async fn test_backtest_stream_memory() {
        let events = vec![
            create_test_event(100),
            create_test_event(200),
        ];
        let stream = BacktestStream::new(events);

        let evt1 = stream.next().await;
        assert!(evt1.is_some());
        assert_eq!(evt1.unwrap().timestamp, 100);

        let evt2 = stream.next().await;
        assert!(evt2.is_some());
        assert_eq!(evt2.unwrap().timestamp, 200);

        let evt3 = stream.next().await;
        assert!(evt3.is_none());
    }

    #[tokio::test]
    async fn test_historical_stream_sorting() {
        // file 1: events at 100, 300
        let mut file1 = NamedTempFile::new().unwrap();
        writeln!(file1, "{}", serde_json::to_string(&create_test_event(100)).unwrap()).unwrap();
        writeln!(file1, "{}", serde_json::to_string(&create_test_event(300)).unwrap()).unwrap();

        // file 2: events at 200, 400
        let mut file2 = NamedTempFile::new().unwrap();
        writeln!(file2, "{}", serde_json::to_string(&create_test_event(200)).unwrap()).unwrap();
        writeln!(file2, "{}", serde_json::to_string(&create_test_event(400)).unwrap()).unwrap();

        let stream = HistoricalStream::new(vec![file1.path(), file2.path()]).unwrap();

        // Expected order: 100, 200, 300, 400
        
        let e1 = stream.next().await.unwrap();
        assert_eq!(e1.timestamp, 100);

        let e2 = stream.next().await.unwrap();
        assert_eq!(e2.timestamp, 200);

        let e3 = stream.next().await.unwrap();
        assert_eq!(e3.timestamp, 300);

        let e4 = stream.next().await.unwrap();
        assert_eq!(e4.timestamp, 400);

        assert!(stream.next().await.is_none());
    }
}
