use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use trading_bot::connectors::backtest::HistoricalStream;
use trading_bot::models::{Instrument, MarketEvent};
use trading_bot::traits::MarketStream;

fn create_event(ts: i64, symbol: &str) -> MarketEvent {
    MarketEvent {
        timestamp: ts,
        instrument: Instrument::Deribit(symbol.to_string()),
        best_bid: None,
        best_ask: None,
        delta: None,
        mark_iv: None,
        bid_iv: None,
        ask_iv: None,
        underlying_price: None,
    }
}

#[tokio::test]
async fn test_multi_file_merge() {
    let dir = std::env::temp_dir().join(format!("test_backtest_{}", chrono::Utc::now().timestamp_nanos()));
    fs::create_dir_all(&dir).unwrap();
    let file1_path = dir.join("file1.jsonl");
    let file2_path = dir.join("file2.jsonl");

    // File 1 events: t=100, t=300, t=500
    {
        let mut f = File::create(&file1_path).unwrap();
        writeln!(f, "{}", serde_json::to_string(&create_event(100, "F1-1")).unwrap()).unwrap();
        writeln!(f, "{}", serde_json::to_string(&create_event(300, "F1-2")).unwrap()).unwrap();
        writeln!(f, "{}", serde_json::to_string(&create_event(500, "F1-3")).unwrap()).unwrap();
    }

    // File 2 events: t=200, t=400
    {
        let mut f = File::create(&file2_path).unwrap();
        writeln!(f, "{}", serde_json::to_string(&create_event(200, "F2-1")).unwrap()).unwrap();
        writeln!(f, "{}", serde_json::to_string(&create_event(400, "F2-2")).unwrap()).unwrap();
    }

    // Initialize stream with both files
    let stream = HistoricalStream::new(vec![file1_path.clone(), file2_path.clone()]).unwrap();

    // Verify order
    let e1 = stream.next().await.unwrap();
    assert_eq!(e1.timestamp, 100);
    assert_eq!(e1.instrument.symbol(), "F1-1");

    let e2 = stream.next().await.unwrap();
    assert_eq!(e2.timestamp, 200);
    assert_eq!(e2.instrument.symbol(), "F2-1");

    let e3 = stream.next().await.unwrap();
    assert_eq!(e3.timestamp, 300);
    assert_eq!(e3.instrument.symbol(), "F1-2");

    let e4 = stream.next().await.unwrap();
    assert_eq!(e4.timestamp, 400);
    assert_eq!(e4.instrument.symbol(), "F2-2");

    let e5 = stream.next().await.unwrap();
    assert_eq!(e5.timestamp, 500);
    assert_eq!(e5.instrument.symbol(), "F1-3");

    // EOF
    assert!(stream.next().await.is_none());
}
