// src/catalog/polymarket.rs
//
// Polymarket market catalog implementation with historical time-travel support.
// Fetches and caches market metadata from Polymarket's CLOB API.
// Tracks all changes (added/removed/modified) for historical reconstruction.

use super::{
    apply_diff, compute_diff, format_timestamp, invert_diff, AutoRefreshGuard, Catalog,
    CatalogDiff, CatalogFileEntry, MarketCatalog, MarketInfo, Refreshable, SearchResult, 
    TokenInfo, DEFAULT_MARKET_STALE_THRESHOLD_SECS,
};
use async_trait::async_trait;
use log::{error, info, warn};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const POLYMARKET_CLOB_URL: &str = "https://clob.polymarket.com";
const DEFAULT_CACHE_PATH: &str = "cache/polymarket_markets.jsonl";

/// Static flag to prevent multiple concurrent auto-refreshes.
/// Only one background refresh can run at a time across all catalog instances.
static AUTO_REFRESH_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Raw market data from Polymarket API.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolymarketApiMarket {
    condition_id: String,
    market_slug: Option<String>,
    question: Option<String>,
    description: Option<String>,
    tags: Option<Vec<String>>,
    tokens: Vec<PolymarketToken>,
    neg_risk_market_id: Option<String>,
    end_date_iso: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    /// Minimum shares for limit orders (typically 5 or 15)
    minimum_order_size: Option<f64>,
    /// Minimum price increment (typically 0.01 or 0.001)
    minimum_tick_size: Option<f64>,
    // Capture everything else
    #[serde(flatten)]
    extra: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolymarketToken {
    token_id: String,
    outcome: Option<String>,
}

/// Response from Polymarket markets endpoint.
#[derive(Debug, Deserialize)]
struct MarketsResponse {
    data: Vec<PolymarketApiMarket>,
    next_cursor: String,
}

/// Internal state for the catalog.
struct CatalogState {
    markets: HashMap<String, MarketInfo>,
    diffs: Vec<CatalogDiff<MarketInfo>>,
    last_updated: u64,
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            markets: HashMap::new(),
            diffs: Vec::new(),
            last_updated: 0,
        }
    }
}

/// Polymarket market catalog with historical time-travel support.
/// 
/// Thread-safe via internal RwLock. Uses std::sync::RwLock (not tokio) because:
/// - Read operations are fast hashmap lookups (no I/O)
/// - Write lock is held only briefly to swap in new state
/// - Safe to call sync trait methods from async context without blocking runtime
/// 
/// # File Format
/// The catalog is stored as JSONL with the following structure:
/// - Line 1: `{"type": "current", "timestamp": ..., "items": [...]}`
/// - Line 2+: `{"type": "diff", "timestamp": ..., "added": [...], ...}`
/// 
/// Diffs are stored newest-first for efficient reconstruction.
pub struct PolymarketCatalog {
    inner: RwLock<CatalogState>,
    cache_path: String,
    http_client: reqwest::Client,
    /// Stale threshold in seconds (triggers auto-refresh when exceeded)
    stale_threshold_secs: u64,
}

impl PolymarketCatalog {
    /// Create a new catalog, loading from cache if available.
    /// 
    /// If the cache is empty, performs a blocking refresh to fetch markets.
    /// If the cache exists but is stale, spawns a background refresh task.
    pub async fn new(
        cache_path: Option<&str>,
        stale_threshold_secs: Option<u64>,
    ) -> Arc<Self> {
        let cache_path = cache_path.unwrap_or(DEFAULT_CACHE_PATH).to_string();
        let stale_threshold_secs = stale_threshold_secs.unwrap_or(DEFAULT_MARKET_STALE_THRESHOLD_SECS);
        let state = Self::load_from_disk(&cache_path).unwrap_or_default();
        
        let loaded_count = state.markets.len();
        let last_updated = state.last_updated;
        let diff_count = state.diffs.len();
        let cache_was_empty = loaded_count == 0;
        
        let catalog = Arc::new(Self {
            inner: RwLock::new(state),
            cache_path,
            http_client: reqwest::Client::new(),
            stale_threshold_secs,
        });

        if loaded_count > 0 {
            info!(
                "PolymarketCatalog: Loaded {} markets, {} diffs from cache (updated {})",
                loaded_count,
                diff_count,
                format_timestamp(last_updated)
            );
        }

        // If cache was empty, do a blocking refresh so discover_subscriptions has data
        // If cache exists but is stale, refresh in background to avoid blocking startup
        if catalog.is_stale_internal(last_updated) {
            // Try to acquire the refresh lock - only one refresh can run at a time
            if AUTO_REFRESH_IN_PROGRESS
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                if cache_was_empty {
                    // No cache - must refresh synchronously or strategies won't find any markets
                    info!("PolymarketCatalog: No cache found, fetching markets...");
                    let _guard = AutoRefreshGuard::new(&AUTO_REFRESH_IN_PROGRESS);
                    match Refreshable::refresh(catalog.as_ref()).await {
                        Ok(count) => info!(
                            "PolymarketCatalog: Initial fetch complete, {} markets",
                            count
                        ),
                        Err(e) => error!("PolymarketCatalog: Initial fetch failed: {}", e),
                    }
                } else {
                    // Cache exists but stale - refresh in background
                    info!("PolymarketCatalog: Cache is stale, spawning background refresh...");
                    let catalog_clone = catalog.clone();
                    tokio::spawn(async move {
                        // Guard ensures flag is reset even if this task panics
                        let _guard = AutoRefreshGuard::new(&AUTO_REFRESH_IN_PROGRESS);
                        match Refreshable::refresh(catalog_clone.as_ref()).await {
                            Ok(count) => info!(
                                "PolymarketCatalog: Background refresh complete, {} changes",
                                count
                            ),
                            Err(e) => error!("PolymarketCatalog: Background refresh failed: {}", e),
                        }
                    });
                }
            } else {
                info!("PolymarketCatalog: Cache is stale, but refresh already in progress");
            }
        }

        catalog
    }

    /// Create a catalog without loading from cache.
    pub async fn new_empty() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(CatalogState::default()),
            cache_path: DEFAULT_CACHE_PATH.to_string(),
            http_client: reqwest::Client::new(),
            stale_threshold_secs: DEFAULT_MARKET_STALE_THRESHOLD_SECS,
        })
    }

    fn is_stale_internal(&self, last_updated: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Use saturating_sub to prevent underflow if last_updated > now
        // (e.g., from clock skew or corrupted cache data)
        now.saturating_sub(last_updated) > self.stale_threshold_secs
    }

    /// Check if the catalog cache is stale.
    pub fn is_stale(&self) -> bool {
        let state = self.inner.read().unwrap();
        self.is_stale_internal(state.last_updated)
    }

    fn load_from_disk(path: &str) -> Option<CatalogState> {
        let file = File::open(path).ok()?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // First line should be current state
        let first_line = lines.next()?.ok()?;
        let first_entry: CatalogFileEntry<MarketInfo> = serde_json::from_str(&first_line).ok()?;

        let (markets, last_updated) = match first_entry {
            CatalogFileEntry::Current { timestamp, items } => {
                let map: HashMap<String, MarketInfo> =
                    items.into_iter().map(|m| (m.id.clone(), m)).collect();
                (map, timestamp)
            }
            _ => return None, // First line must be current state
        };

        // Remaining lines are diffs (newest first)
        let mut diffs = Vec::new();
        for line in lines {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            if line.trim().is_empty() {
                continue;
            }
            
            let entry: CatalogFileEntry<MarketInfo> = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    warn!("PolymarketCatalog: Failed to parse entry: {}", e);
                    continue;
                }
            };

            if let CatalogFileEntry::Diff(diff) = entry {
                diffs.push(diff);
            }
        }

        Some(CatalogState {
            markets,
            diffs,
            last_updated,
        })
    }

    fn save_to_disk(&self) -> Result<(), String> {
        let state = self.inner.read().unwrap();

        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(&self.cache_path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create cache directory: {}", e))?;
        }

        let mut file = File::create(&self.cache_path)
            .map_err(|e| format!("Failed to create cache file: {}", e))?;

        // Write current state first
        let current_entry = CatalogFileEntry::Current {
            timestamp: state.last_updated,
            items: state.markets.values().cloned().collect(),
        };
        writeln!(file, "{}", serde_json::to_string(&current_entry).unwrap())
            .map_err(|e| format!("Failed to write current state: {}", e))?;

        // Write diffs (newest first)
        for diff in &state.diffs {
            let diff_entry: CatalogFileEntry<MarketInfo> = CatalogFileEntry::Diff(diff.clone());
            writeln!(file, "{}", serde_json::to_string(&diff_entry).unwrap())
                .map_err(|e| format!("Failed to write diff: {}", e))?;
        }

        Ok(())
    }

    /// Convert API market to our generic MarketInfo.
    fn convert_market(api_market: PolymarketApiMarket) -> MarketInfo {
        let tokens = api_market
            .tokens
            .iter()
            .map(|t| TokenInfo {
                token_id: t.token_id.clone(),
                outcome: t.outcome.clone(),
            })
            .collect();

        MarketInfo {
            id: api_market.condition_id,
            slug: api_market.market_slug,
            question: api_market.question,
            description: api_market.description,
            tags: api_market.tags,
            tokens,
            extra: serde_json::json!({
                "neg_risk_market_id": api_market.neg_risk_market_id,
                "end_date_iso": api_market.end_date_iso,
                "active": api_market.active,
                "closed": api_market.closed,
                "minimum_order_size": api_market.minimum_order_size,
                "minimum_tick_size": api_market.minimum_tick_size,
            }),
        }
    }

    /// Fetches all markets from the Polymarket API.
    async fn fetch_all_markets(&self) -> Result<HashMap<String, MarketInfo>, String> {
        info!("PolymarketCatalog: Starting fetch...");
        
        let mut all_markets: HashMap<String, MarketInfo> = HashMap::new();
        let mut next_cursor: Option<String> = None;
        let mut page = 0;

        loop {
            page += 1;
            if page % 50 == 0 {
                info!("PolymarketCatalog: Fetching page {}...", page);
            }

            let url = format!("{}/markets", POLYMARKET_CLOB_URL);

            let mut request = self.http_client.get(&url);
            if let Some(cursor) = &next_cursor {
                request = request.query(&[("next_cursor", cursor)]);
            }

            let response = request
                .send()
                .await
                .map_err(|e| format!("HTTP request failed: {}", e))?;

            if !response.status().is_success() {
                return Err(format!("API returned status: {}", response.status()));
            }

            let body: MarketsResponse = response
                .json()
                .await
                .map_err(|e| format!("Failed to parse response: {}", e))?;

            for api_market in body.data {
                let market = Self::convert_market(api_market);
                all_markets.insert(market.id.clone(), market);
            }

            if body.next_cursor == "LTE=" || body.next_cursor.is_empty() {
                break;
            }

            next_cursor = Some(body.next_cursor);

            if page > 1000 {
                warn!("PolymarketCatalog: Hit page limit, stopping");
                break;
            }
        }

        Ok(all_markets)
    }

    /// Get tokens for a market by condition_id.
    pub fn get_tokens(&self, condition_id: &str) -> Option<Vec<TokenInfo>> {
        let state = self.inner.read().unwrap();
        state.markets.get(condition_id).map(|m| m.tokens.clone())
    }

    /// Get a specific token by condition_id and outcome name.
    pub fn get_token_by_outcome(&self, condition_id: &str, outcome: &str) -> Option<TokenInfo> {
        let state = self.inner.read().unwrap();
        state.markets.get(condition_id).and_then(|m| {
            m.token_by_outcome(outcome).cloned()
        })
    }

    /// Find markets by neg_risk_market_id.
    pub fn find_by_neg_risk_market_id(&self, neg_risk_market_id: &str) -> Vec<MarketInfo> {
        let state = self.inner.read().unwrap();
        state
            .markets
            .values()
            .filter(|m| {
                m.extra
                    .get("neg_risk_market_id")
                    .and_then(|v| v.as_str())
                    .map(|id| id == neg_risk_market_id)
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }
}

// =============================================================================
// =============================================================================
// Refreshable Trait Implementation (used by Engine)
// =============================================================================

#[async_trait]
impl Refreshable for PolymarketCatalog {
    async fn refresh(&self) -> Result<usize, String> {
        let diff = self.refresh_with_diff().await?;
        Ok(diff.change_count())
    }

    fn last_updated(&self) -> u64 {
        self.inner.read().unwrap().last_updated
    }
}

// =============================================================================
// Catalog Trait Implementation (with time-travel)
// =============================================================================

#[async_trait]
impl Catalog for PolymarketCatalog {
    type Item = MarketInfo;

    fn item_id(item: &Self::Item) -> String {
        item.id.clone()
    }

    fn current(&self) -> HashMap<String, MarketInfo> {
        self.inner.read().unwrap().markets.clone()
    }

    fn as_of(&self, timestamp: u64) -> HashMap<String, MarketInfo> {
        let state = self.inner.read().unwrap();
        
        // Start with current state
        let mut result = state.markets.clone();

        // Walk backwards through diffs, inverting and applying each one
        // Diffs are stored newest-first
        for diff in &state.diffs {
            if diff.timestamp <= timestamp {
                // We've gone far enough back
                break;
            }
            
            // Invert the diff and apply it
            let inverted = invert_diff(diff);
            apply_diff(&mut result, &inverted, Self::item_id);
        }

        result
    }

    async fn refresh_with_diff(&self) -> Result<CatalogDiff<MarketInfo>, String> {
        let new_markets = self.fetch_all_markets().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let diff = {
            let state = self.inner.read().unwrap();
            compute_diff(&state.markets, &new_markets, now, Self::item_id)
        };

        // Update state
        {
            let mut state = self.inner.write().unwrap();
            
            // Only add diff if there are actual changes
            if !diff.is_empty() {
                state.diffs.insert(0, diff.clone()); // Insert at front (newest first)
            }
            
            state.markets = new_markets;
            state.last_updated = now;
        }

        // Save to disk
        if let Err(e) = self.save_to_disk() {
            error!("PolymarketCatalog: Failed to save cache: {}", e);
        } else {
            let state = self.inner.read().unwrap();
            info!(
                "PolymarketCatalog: Saved {} markets, {} diffs to cache",
                state.markets.len(),
                state.diffs.len()
            );
        }

        Ok(diff)
    }

    fn diffs(&self) -> Vec<CatalogDiff<MarketInfo>> {
        self.inner.read().unwrap().diffs.clone()
    }

    fn len(&self) -> usize {
        self.inner.read().unwrap().markets.len()
    }
}

// =============================================================================
// Legacy MarketCatalog Trait Implementation
// =============================================================================

#[async_trait]
impl MarketCatalog for PolymarketCatalog {
    async fn refresh(&self) -> Result<usize, String> {
        // For the legacy trait, we can't mutate self, so we do an internal refresh
        let new_markets = self.fetch_all_markets().await?;
        let count = new_markets.len();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let diff = {
            let state = self.inner.read().unwrap();
            compute_diff(&state.markets, &new_markets, now, Self::item_id)
        };

        {
            let mut state = self.inner.write().unwrap();
            if !diff.is_empty() {
                state.diffs.insert(0, diff);
            }
            state.markets = new_markets;
            state.last_updated = now;
        }

        if let Err(e) = self.save_to_disk() {
            error!("PolymarketCatalog: Failed to save cache: {}", e);
        }

        Ok(count)
    }

    fn search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let state = self.inner.read().unwrap();
        
        let query_lower = query.to_lowercase();
        let query_terms: Vec<&str> = query_lower.split_whitespace().collect();
        if query_terms.is_empty() {
            return vec![];
        }

        let mut results: Vec<(u32, MarketInfo)> = state
            .markets
            .values()
            .map(|market| {
                let mut score: u32 = 0;
                let slug_lower = market.slug.as_deref().unwrap_or("").to_lowercase();
                let question_lower = market.question.as_deref().unwrap_or("").to_lowercase();
                let desc_lower = market.description.as_deref().unwrap_or("").to_lowercase();
                let tags_lower = market
                    .tags
                    .as_ref()
                    .map(|t| t.join(" ").to_lowercase())
                    .unwrap_or_default();

                for term in &query_terms {
                    score += 8 * slug_lower.matches(term).count() as u32;
                    score += 4 * question_lower.matches(term).count() as u32;
                    score += 2 * tags_lower.matches(term).count() as u32;
                    score += desc_lower.matches(term).count() as u32;
                }

                (score, market.clone())
            })
            .filter(|(score, _)| *score > 0)
            .collect();

        results.sort_by(|a, b| b.0.cmp(&a.0));

        results
            .into_iter()
            .take(limit)
            .map(|(score, market)| SearchResult { market, score })
            .collect()
    }

    fn find_by_slug(&self, slug: &str) -> Option<MarketInfo> {
        let state = self.inner.read().unwrap();
        state
            .markets
            .values()
            .find(|m| m.slug.as_deref() == Some(slug))
            .cloned()
    }

    fn find_by_slug_regex(&self, pattern: &str) -> Result<Vec<MarketInfo>, String> {
        let regex = Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))?;
        let state = self.inner.read().unwrap();
        
        Ok(state
            .markets
            .values()
            .filter(|m| {
                m.slug
                    .as_ref()
                    .map(|s| regex.is_match(s))
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    fn find_by_token_id(&self, token_id: &str) -> Option<MarketInfo> {
        let state = self.inner.read().unwrap();
        state
            .markets
            .values()
            .find(|m| m.tokens.iter().any(|t| t.token_id == token_id))
            .cloned()
    }

    fn get(&self, id: &str) -> Option<MarketInfo> {
        let state = self.inner.read().unwrap();
        state.markets.get(id).cloned()
    }

    fn all(&self) -> Vec<MarketInfo> {
        let state = self.inner.read().unwrap();
        state.markets.values().cloned().collect()
    }

    fn last_updated(&self) -> i64 {
        self.inner.read().unwrap().last_updated as i64
    }

    fn len(&self) -> usize {
        self.inner.read().unwrap().markets.len()
    }
}

// =============================================================================
// Order Validation
// =============================================================================

use crate::models::{Order, OrderType};

/// Errors that can occur when validating a Polymarket order.
#[derive(Debug, Clone, PartialEq)]
pub enum PolymarketOrderError {
    /// Order quantity is below the market's minimum_order_size
    QuantityTooSmall {
        quantity: f64,
        minimum: f64,
    },
    /// Order price is not aligned to the market's minimum_tick_size
    InvalidTickSize {
        price: f64,
        tick_size: f64,
        nearest_valid: f64,
    },
    /// Market order value is below the $1 minimum
    MarketOrderTooSmall {
        estimated_value: f64,
        minimum: f64,
    },
}

impl std::fmt::Display for PolymarketOrderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QuantityTooSmall { quantity, minimum } => {
                write!(f, "Order quantity {} is below minimum {} shares", quantity, minimum)
            }
            Self::InvalidTickSize { price, tick_size, nearest_valid } => {
                write!(f, "Price {} is not aligned to tick size {} (nearest: {})", 
                       price, tick_size, nearest_valid)
            }
            Self::MarketOrderTooSmall { estimated_value, minimum } => {
                write!(f, "Market order value ${:.2} is below minimum ${:.2}", 
                       estimated_value, minimum)
            }
        }
    }
}

impl std::error::Error for PolymarketOrderError {}

/// Minimum value for market orders on Polymarket (in USDC).
pub const POLYMARKET_MIN_MARKET_ORDER_VALUE: f64 = 1.0;

/// Validate a Polymarket order against market constraints.
/// 
/// # Arguments
/// * `order` - The order to validate
/// * `market` - The market info containing constraints (minimum_order_size, minimum_tick_size)
/// * `estimated_price` - For market orders, the estimated execution price (e.g., best bid/ask)
/// 
/// # Returns
/// * `Ok(())` if the order is valid
/// * `Err(PolymarketOrderError)` describing the validation failure
/// 
/// # Example
/// ```ignore
/// use trading_bot::catalog::{validate_polymarket_order, MarketInfo};
/// use trading_bot::models::{Order, Instrument, OrderSide};
/// 
/// let market = catalog.get("condition_id").unwrap();
/// let order = Order::limit(
///     Instrument::polymarket("token_id"),
///     OrderSide::Buy,
///     15.0,  // quantity
///     0.55,  // price
/// );
/// 
/// // For limit orders, estimated_price can be None or the limit price
/// validate_polymarket_order(&order, &market, Some(0.55))?;
/// ```
pub fn validate_polymarket_order(
    order: &Order,
    market: &MarketInfo,
    estimated_price: Option<f64>,
) -> Result<(), PolymarketOrderError> {
    // Check minimum order size (applies to limit orders only)
    // Market orders have a $1 minimum value instead, checked below
    if order.order_type == OrderType::Limit {
        if let Some(min_size) = market.minimum_order_size() {
            if order.quantity < min_size {
                return Err(PolymarketOrderError::QuantityTooSmall {
                    quantity: order.quantity,
                    minimum: min_size,
                });
            }
        }
    }

    // Check tick size alignment for limit orders
    if order.order_type == OrderType::Limit {
        if let (Some(price), Some(tick_size)) = (order.price, market.minimum_tick_size()) {
            if tick_size > 0.0 {
                let ticks = price / tick_size;
                let rounded = ticks.round();
                // Allow for floating point tolerance
                if (ticks - rounded).abs() > 1e-9 {
                    let nearest_valid = rounded * tick_size;
                    return Err(PolymarketOrderError::InvalidTickSize {
                        price,
                        tick_size,
                        nearest_valid,
                    });
                }
            }
        }
    }

    // Check market order minimum value ($1)
    // Note: This requires an estimated_price to be provided by the caller.
    // If estimated_price is None for market orders, this check is skipped
    // and the exchange will reject invalid orders directly.
    if order.order_type == OrderType::Market {
        if let Some(price) = estimated_price {
            let estimated_value = order.quantity * price;
            if estimated_value < POLYMARKET_MIN_MARKET_ORDER_VALUE {
                return Err(PolymarketOrderError::MarketOrderTooSmall {
                    estimated_value,
                    minimum: POLYMARKET_MIN_MARKET_ORDER_VALUE,
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_scoring() {
        // Basic scoring test - would need mock data
    }

    #[test]
    fn test_is_stale_with_empty_cache() {
        // When last_updated is 0 (empty cache), should always be stale
        let catalog = PolymarketCatalog {
            inner: RwLock::new(CatalogState::default()),
            cache_path: "test.jsonl".to_string(),
            http_client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            stale_threshold_secs: 3600, // 1 hour
        };

        // Default state has last_updated = 0, which is always stale
        assert!(catalog.is_stale());
    }

    #[test]
    fn test_is_stale_with_recent_update() {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = CatalogState {
            markets: HashMap::new(),
            diffs: vec![],
            last_updated: now - 100, // 100 seconds ago
        };

        let catalog = PolymarketCatalog {
            inner: RwLock::new(state),
            cache_path: "test.jsonl".to_string(),
            http_client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            stale_threshold_secs: 3600, // 1 hour threshold
        };

        // Updated 100s ago with 1 hour threshold = not stale
        assert!(!catalog.is_stale());
    }

    #[test]
    fn test_is_stale_with_old_update() {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = CatalogState {
            markets: HashMap::new(),
            diffs: vec![],
            last_updated: now - 7200, // 2 hours ago
        };

        let catalog = PolymarketCatalog {
            inner: RwLock::new(state),
            cache_path: "test.jsonl".to_string(),
            http_client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            stale_threshold_secs: 3600, // 1 hour threshold
        };

        // Updated 2 hours ago with 1 hour threshold = stale
        assert!(catalog.is_stale());
    }

    #[test]
    fn test_as_of_with_no_diffs() {
        // When there are no diffs, as_of should return current state
        let state = CatalogState {
            markets: {
                let mut m = HashMap::new();
                m.insert("a".to_string(), MarketInfo {
                    id: "a".to_string(),
                    slug: Some("test-market".to_string()),
                    question: Some("Test?".to_string()),
                    description: None,
                    tags: None,
                    tokens: vec![],
                    extra: serde_json::json!({}),
                });
                m
            },
            diffs: vec![],
            last_updated: 1000,
        };

        let catalog = PolymarketCatalog {
            inner: RwLock::new(state),
            cache_path: "test.jsonl".to_string(),
            http_client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            stale_threshold_secs: DEFAULT_MARKET_STALE_THRESHOLD_SECS,
        };

        let result = catalog.as_of(500);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("a"));
    }

    // =========================================================================
    // Order Validation Tests
    // =========================================================================

    use crate::models::{Instrument, OrderSide};

    fn make_test_market(min_order_size: Option<f64>, min_tick_size: Option<f64>) -> MarketInfo {
        MarketInfo {
            id: "test-condition".to_string(),
            slug: Some("test-market".to_string()),
            question: Some("Test?".to_string()),
            description: None,
            tags: None,
            tokens: vec![
                TokenInfo { token_id: "token-yes".to_string(), outcome: Some("Yes".to_string()) },
                TokenInfo { token_id: "token-no".to_string(), outcome: Some("No".to_string()) },
            ],
            extra: serde_json::json!({
                "minimum_order_size": min_order_size,
                "minimum_tick_size": min_tick_size,
            }),
        }
    }

    #[test]
    fn test_validate_order_quantity_valid() {
        let market = make_test_market(Some(15.0), Some(0.01));
        let order = Order::limit(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            20.0,  // Above minimum
            0.55,
        );
        
        assert!(validate_polymarket_order(&order, &market, None).is_ok());
    }

    #[test]
    fn test_validate_order_quantity_too_small() {
        let market = make_test_market(Some(15.0), Some(0.01));
        let order = Order::limit(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            10.0,  // Below minimum of 15
            0.55,
        );
        
        let result = validate_polymarket_order(&order, &market, None);
        assert!(matches!(
            result,
            Err(PolymarketOrderError::QuantityTooSmall { quantity: 10.0, minimum: 15.0 })
        ));
    }

    #[test]
    fn test_validate_order_tick_size_valid() {
        let market = make_test_market(Some(5.0), Some(0.01));
        let order = Order::limit(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            10.0,
            0.55,  // Valid: 55 ticks of 0.01
        );
        
        assert!(validate_polymarket_order(&order, &market, None).is_ok());
    }

    #[test]
    fn test_validate_order_tick_size_invalid() {
        let market = make_test_market(Some(5.0), Some(0.01));
        let order = Order::limit(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            10.0,
            0.555,  // Invalid: not aligned to 0.01
        );
        
        let result = validate_polymarket_order(&order, &market, None);
        assert!(matches!(
            result,
            Err(PolymarketOrderError::InvalidTickSize { .. })
        ));
    }

    #[test]
    fn test_validate_market_order_value_valid() {
        let market = make_test_market(None, None);
        let order = Order::market(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            10.0,  // 10 shares
        );
        
        // At 0.50 price: 10 * 0.50 = $5.00 (above $1 minimum)
        assert!(validate_polymarket_order(&order, &market, Some(0.50)).is_ok());
    }

    #[test]
    fn test_validate_market_order_value_too_small() {
        let market = make_test_market(None, None);
        let order = Order::market(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            1.0,  // 1 share
        );
        
        // At 0.50 price: 1 * 0.50 = $0.50 (below $1 minimum)
        let result = validate_polymarket_order(&order, &market, Some(0.50));
        assert!(matches!(
            result,
            Err(PolymarketOrderError::MarketOrderTooSmall { .. })
        ));
    }

    #[test]
    fn test_validate_order_no_constraints() {
        // When market has no constraints set, validation should pass
        let market = make_test_market(None, None);
        let order = Order::limit(
            Instrument::polymarket("token-yes"),
            OrderSide::Buy,
            1.0,
            0.555,
        );
        
        assert!(validate_polymarket_order(&order, &market, None).is_ok());
    }

    #[test]
    fn test_market_info_accessors() {
        let market = make_test_market(Some(15.0), Some(0.01));
        
        assert_eq!(market.minimum_order_size(), Some(15.0));
        assert_eq!(market.minimum_tick_size(), Some(0.01));
        assert!(market.is_valid_order_size(15.0));
        assert!(market.is_valid_order_size(20.0));
        assert!(!market.is_valid_order_size(10.0));
        
        // Test round_to_tick
        assert!((market.round_to_tick(0.555) - 0.56).abs() < 1e-9);
        assert!((market.round_to_tick(0.554) - 0.55).abs() < 1e-9);
        assert!((market.round_to_tick(0.50) - 0.50).abs() < 1e-9);
    }
}
