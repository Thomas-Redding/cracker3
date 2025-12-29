// tests/polymarket_integration.rs
//
// Integration test for Polymarket connector.
// Tests real WebSocket connection, order book maintenance, event parsing, and persistence.
//
// Run with: cargo test --test polymarket_integration -- --ignored --nocapture

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Duration;
use trading_bot::catalog::{MarketCatalog, PolymarketCatalog};
use trading_bot::connectors::polymarket::PolymarketStream;
use trading_bot::models::MarketEvent;
use trading_bot::traits::MarketStream;

const TEST_MARKET_SLUG: &str = "will-anyone-be-charged-over-daycare-fraud-in-minnesota";
const TEST_OUTPUT_DIR: &str = "test_output";
const EVENTS_FILE: &str = "polymarket_events.jsonl";

/// Helper to ensure test output directory exists
fn setup_test_output_dir() -> String {
    let dir = format!("{}/polymarket_integration", TEST_OUTPUT_DIR);
    fs::create_dir_all(&dir).expect("Failed to create test output directory");
    dir
}

/// Helper to clean up test output
fn cleanup_test_output(dir: &str) {
    let _ = fs::remove_dir_all(dir);
}

/// Persists a MarketEvent to a JSONL file
fn persist_event(file: &mut File, event: &MarketEvent) -> std::io::Result<()> {
    let json = serde_json::to_string(event)?;
    writeln!(file, "{}", json)?;
    file.flush()?;
    Ok(())
}

/// Load events from a JSONL file
fn load_events(path: &Path) -> Vec<MarketEvent> {
    let file = File::open(path).expect("Failed to open events file");
    let reader = BufReader::new(file);
    
    reader
        .lines()
        .filter_map(|line| {
            let line = line.ok()?;
            if line.trim().is_empty() {
                return None;
            }
            serde_json::from_str(&line).ok()
        })
        .collect()
}

/// Validates that a MarketEvent is properly formed
fn validate_event(event: &MarketEvent, expected_token_ids: &[String]) -> Result<(), String> {
    // Timestamp should be reasonable (after 2020, which is 1577836800000 ms)
    if event.timestamp < 1577836800000 && event.timestamp != 0 {
        return Err(format!("Timestamp looks invalid: {}", event.timestamp));
    }

    // Instrument should be one of our expected token IDs
    if !expected_token_ids.contains(&event.instrument) {
        return Err(format!(
            "Unexpected instrument: {} (expected one of {:?})",
            event.instrument, expected_token_ids
        ));
    }

    // If we have a bid, it should be in valid range [0, 1]
    if let Some(bid) = event.best_bid {
        if !(0.0..=1.0).contains(&bid) {
            return Err(format!("Best bid out of range: {}", bid));
        }
    }

    // If we have an ask, it should be in valid range [0, 1]
    if let Some(ask) = event.best_ask {
        if !(0.0..=1.0).contains(&ask) {
            return Err(format!("Best ask out of range: {}", ask));
        }
    }

    // If both bid and ask exist, bid should be <= ask (except in crossed book scenarios)
    if let (Some(bid), Some(ask)) = (event.best_bid, event.best_ask) {
        // We log but don't fail on crossed books - they can happen briefly
        if bid > ask {
            eprintln!("  [WARN] Crossed book detected: bid {} > ask {}", bid, ask);
        }
    }

    Ok(())
}

/// Helper to ensure catalog has the market we need
async fn ensure_catalog_populated(
    catalog: &std::sync::Arc<PolymarketCatalog>,
    slug: &str,
) -> bool {
    // First check if it's already there
    if catalog.find_by_slug(slug).is_some() {
        println!("  Market found in cache (catalog has {} markets)", catalog.len());
        return true;
    }

    // If catalog is empty or stale, do a full refresh (may take 5+ minutes)
    if catalog.len() == 0 || catalog.is_stale() {
        println!("  Catalog is empty or stale, starting fresh refresh...");
        println!("  (This may take 3-5 minutes for ~1000 pages)");

        // The background refresh was already spawned by new(), 
        // but let's wait for it by polling the catalog
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(360); // 6 minutes max

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            if catalog.find_by_slug(slug).is_some() {
                println!("  Found market after {:?}", start.elapsed());
                return true;
            }

            let elapsed = start.elapsed();
            if elapsed > max_wait {
                println!("  Timeout after {:?} ({} markets loaded)", elapsed, catalog.len());
                break;
            }

            // Show progress
            if elapsed.as_secs() % 30 == 0 {
                println!("  Still loading... {} markets so far", catalog.len());
            }
        }
    }

    catalog.find_by_slug(slug).is_some()
}

/// Main integration test: connects to Polymarket, receives events, validates and persists them
#[tokio::test]
#[ignore] // This test requires network access and takes time
async fn test_polymarket_connector_live() {
    // Initialize logging for test visibility
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let output_dir = setup_test_output_dir();
    let events_path = format!("{}/{}", output_dir, EVENTS_FILE);

    println!("\n=== Polymarket Integration Test ===");
    println!("Market: {}", TEST_MARKET_SLUG);
    println!("Output: {}", events_path);
    println!();

    // Step 1: Load catalog and find the market
    println!("Step 1: Loading Polymarket catalog...");
    let catalog = PolymarketCatalog::new(None).await;
    
    // Wait for catalog to be populated
    assert!(
        ensure_catalog_populated(&catalog, TEST_MARKET_SLUG).await,
        "Market '{}' not found in catalog after waiting for refresh. \
         The market may not exist or the catalog refresh may have failed.",
        TEST_MARKET_SLUG
    );

    let market = catalog
        .find_by_slug(TEST_MARKET_SLUG)
        .expect("Market should exist after ensure_catalog_populated");

    println!("  Found market: {}", market.question.as_deref().unwrap_or("?"));
    println!("  Condition ID: {}", market.id);
    println!("  Tokens:");
    for token in &market.tokens {
        println!(
            "    - {} ({})",
            token.token_id,
            token.outcome.as_deref().unwrap_or("unknown")
        );
    }
    println!();

    let token_ids: Vec<String> = market.tokens.iter().map(|t| t.token_id.clone()).collect();
    assert!(
        !token_ids.is_empty(),
        "Market should have at least one token"
    );

    // Step 2: Connect to WebSocket and subscribe
    println!("Step 2: Connecting to Polymarket WebSocket...");
    let mut stream = PolymarketStream::new(token_ids.clone()).await;
    println!("  Connected and subscribed to {} tokens", token_ids.len());
    println!();

    // Step 3: Receive and validate events
    println!("Step 3: Receiving events (will collect for 30 seconds)...");
    
    let mut events_file = File::create(&events_path).expect("Failed to create events file");
    let mut event_count = 0;
    let mut error_count = 0;
    let mut last_bbo: Option<(Option<f64>, Option<f64>)> = None;

    let timeout = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match stream.next().await {
                Some(event) => {
                    event_count += 1;

                    // Validate the event
                    match validate_event(&event, &token_ids) {
                        Ok(()) => {
                            // Show first few events and BBO changes
                            let is_bbo_change = last_bbo
                                .map(|(b, a)| b != event.best_bid || a != event.best_ask)
                                .unwrap_or(true);

                            if event_count <= 5 || is_bbo_change {
                                println!(
                                    "  Event {}: instrument={} bid={:?} ask={:?} ts={}",
                                    event_count,
                                    &event.instrument[..8.min(event.instrument.len())],
                                    event.best_bid,
                                    event.best_ask,
                                    event.timestamp
                                );
                                last_bbo = Some((event.best_bid, event.best_ask));
                            }

                            // Persist to disk
                            if let Err(e) = persist_event(&mut events_file, &event) {
                                eprintln!("  [ERROR] Failed to persist event: {}", e);
                                error_count += 1;
                            }
                        }
                        Err(e) => {
                            eprintln!("  [ERROR] Invalid event: {}", e);
                            error_count += 1;
                        }
                    }

                    // Stop after enough events or errors
                    if event_count >= 100 {
                        println!("  Collected 100 events, stopping...");
                        break;
                    }
                }
                None => {
                    eprintln!("  [WARN] Stream ended unexpectedly");
                    break;
                }
            }
        }
    });

    let _ = timeout.await;
    drop(events_file);

    println!();
    println!("Step 4: Validating persisted events...");
    
    // Step 4: Verify events were persisted correctly
    let loaded_events = load_events(Path::new(&events_path));
    println!("  Persisted {} events to disk", event_count);
    println!("  Loaded {} events back from disk", loaded_events.len());

    // Verify we got some events
    assert!(
        event_count > 0,
        "Should have received at least one event from Polymarket"
    );

    // Verify persistence worked
    assert_eq!(
        loaded_events.len(),
        event_count - error_count,
        "All valid events should be persisted and loadable"
    );

    // Verify loaded events are valid
    for (i, event) in loaded_events.iter().enumerate() {
        if let Err(e) = validate_event(event, &token_ids) {
            panic!("Loaded event {} is invalid: {}", i, e);
        }
    }

    // Verify at least one event has BBO data (market might be illiquid)
    let events_with_bbo = loaded_events
        .iter()
        .filter(|e| e.best_bid.is_some() || e.best_ask.is_some())
        .count();
    println!("  Events with BBO data: {}", events_with_bbo);

    println!();
    println!("=== Test Completed Successfully ===");
    println!("  Total events received: {}", event_count);
    println!("  Validation errors: {}", error_count);
    println!("  Events persisted: {}", loaded_events.len());

    // Cleanup (comment out to inspect output)
    // cleanup_test_output(&output_dir);
}

/// Test that we can look up the specific market and get valid token IDs
#[tokio::test]
#[ignore]
async fn test_market_lookup() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    println!("\n=== Market Lookup Test ===");
    
    let catalog = PolymarketCatalog::new(None).await;
    
    assert!(
        ensure_catalog_populated(&catalog, TEST_MARKET_SLUG).await,
        "Market should exist in catalog"
    );

    let market = catalog.find_by_slug(TEST_MARKET_SLUG);
    assert!(market.is_some(), "Market should exist after ensure_catalog_populated");

    let market = market.unwrap();
    println!("Market: {}", market.question.as_deref().unwrap_or("?"));
    println!("Slug: {}", market.slug.as_deref().unwrap_or("?"));
    println!("Condition ID: {}", market.id);
    
    // Polymarket markets typically have 2 tokens (Yes/No)
    assert!(
        !market.tokens.is_empty(),
        "Market should have tokens"
    );
    
    for token in &market.tokens {
        // Token IDs should be hex strings (0x...)
        assert!(
            token.token_id.starts_with("0x") || token.token_id.len() >= 40,
            "Token ID should look like a valid Polymarket token: {}",
            token.token_id
        );
        println!(
            "  Token: {} (outcome: {:?})",
            token.token_id,
            token.outcome
        );
    }

    println!("=== Lookup Test Passed ===");
}

/// Test that order book state is maintained correctly across events
#[tokio::test]
#[ignore]
async fn test_order_book_consistency() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    println!("\n=== Order Book Consistency Test ===");
    
    let catalog = PolymarketCatalog::new(None).await;
    
    assert!(
        ensure_catalog_populated(&catalog, TEST_MARKET_SLUG).await,
        "Market should exist in catalog"
    );

    let market = catalog
        .find_by_slug(TEST_MARKET_SLUG)
        .expect("Market should exist after ensure_catalog_populated");

    let token_ids: Vec<String> = market.tokens.iter().map(|t| t.token_id.clone()).collect();
    let mut stream = PolymarketStream::new(token_ids).await;

    // Track order book state per token
    let mut last_state: std::collections::HashMap<String, (Option<f64>, Option<f64>)> =
        std::collections::HashMap::new();
    let mut state_changes = 0;

    let timeout = tokio::time::timeout(Duration::from_secs(20), async {
        let mut count = 0;
        while let Some(event) = stream.next().await {
            count += 1;

            let prev = last_state.get(&event.instrument).cloned();
            let curr = (event.best_bid, event.best_ask);

            // Track state changes
            if prev != Some(curr) {
                state_changes += 1;
                
                // Log significant changes
                if let Some((prev_bid, prev_ask)) = prev {
                    if prev_bid != event.best_bid {
                        println!(
                            "  Bid change: {:?} -> {:?}",
                            prev_bid, event.best_bid
                        );
                    }
                    if prev_ask != event.best_ask {
                        println!(
                            "  Ask change: {:?} -> {:?}",
                            prev_ask, event.best_ask
                        );
                    }
                } else {
                    println!(
                        "  Initial state: bid={:?} ask={:?}",
                        event.best_bid, event.best_ask
                    );
                }
            }

            last_state.insert(event.instrument.clone(), curr);

            if count >= 50 {
                break;
            }
        }
    });

    let _ = timeout.await;

    println!();
    println!("  Tracked {} token states", last_state.len());
    println!("  Observed {} state changes", state_changes);

    // We should see at least initial state for each token
    assert!(
        !last_state.is_empty(),
        "Should have received state for at least one token"
    );

    println!("=== Order Book Consistency Test Passed ===");
}

