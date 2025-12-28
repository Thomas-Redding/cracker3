// src/catalog/polymarket.rs
//
// Polymarket market catalog implementation.
// Fetches and caches market metadata from Polymarket's CLOB API.

use super::{MarketCatalog, MarketInfo, SearchResult, TokenInfo};
use async_trait::async_trait;
use log::{error, info, warn};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const POLYMARKET_CLOB_URL: &str = "https://clob.polymarket.com";
const DEFAULT_CACHE_PATH: &str = "polymarket_markets.jsonl";
const STALE_THRESHOLD_SECS: i64 = 86400; // 1 day

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

/// Cache file header.
#[derive(Debug, Serialize, Deserialize)]
struct CacheHeader {
    last_updated: i64,
    version: u32,
}

/// Internal state for the catalog.
struct CatalogState {
    markets: HashMap<String, MarketInfo>,
    last_updated: i64,
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            markets: HashMap::new(),
            last_updated: 0,
        }
    }
}

/// Polymarket market catalog.
/// 
/// Thread-safe via internal RwLock. Uses std::sync::RwLock (not tokio) because:
/// - Read operations are fast hashmap lookups (no I/O)
/// - Write lock is held only briefly to swap in new state
/// - Safe to call sync trait methods from async context without blocking runtime
pub struct PolymarketCatalog {
    inner: RwLock<CatalogState>,
    cache_path: String,
    http_client: reqwest::Client,
}

impl PolymarketCatalog {
    /// Create a new catalog, loading from cache if available.
    /// 
    /// If the cache is stale (>1 day), spawns a background refresh task.
    pub async fn new(cache_path: Option<&str>) -> Arc<Self> {
        let cache_path = cache_path.unwrap_or(DEFAULT_CACHE_PATH).to_string();
        let state = Self::load_from_disk(&cache_path).unwrap_or_default();
        
        let loaded_count = state.markets.len();
        let last_updated = state.last_updated;
        
        let catalog = Arc::new(Self {
            inner: RwLock::new(state),
            cache_path,
            http_client: reqwest::Client::new(),
        });

        if loaded_count > 0 {
            info!(
                "PolymarketCatalog: Loaded {} markets from cache (updated {})",
                loaded_count,
                format_timestamp(last_updated)
            );
        }

        // Check staleness and auto-refresh in background
        if catalog.is_stale_internal(last_updated) {
            info!("PolymarketCatalog: Cache is stale, spawning background refresh...");
            let catalog_clone = catalog.clone();
            tokio::spawn(async move {
                match catalog_clone.refresh().await {
                    Ok(count) => info!("PolymarketCatalog: Background refresh complete, {} markets", count),
                    Err(e) => error!("PolymarketCatalog: Background refresh failed: {}", e),
                }
            });
        }

        catalog
    }

    /// Create a catalog without loading from cache.
    pub async fn new_empty() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(CatalogState::default()),
            cache_path: DEFAULT_CACHE_PATH.to_string(),
            http_client: reqwest::Client::new(),
        })
    }

    fn is_stale_internal(&self, last_updated: i64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        now - last_updated > STALE_THRESHOLD_SECS
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

        // First line is header
        let header_line = lines.next()?.ok()?;
        let header: CacheHeader = serde_json::from_str(&header_line).ok()?;

        let mut markets = HashMap::new();
        for line in lines {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            if line.trim().is_empty() {
                continue;
            }
            let market: MarketInfo = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(e) => {
                    warn!("PolymarketCatalog: Failed to parse market line: {}", e);
                    continue;
                }
            };
            markets.insert(market.id.clone(), market);
        }

        Some(CatalogState {
            markets,
            last_updated: header.last_updated,
        })
    }

    fn save_to_disk(&self) -> Result<(), String> {
        let state = self.inner.read().unwrap();
        
        let mut file = File::create(&self.cache_path)
            .map_err(|e| format!("Failed to create cache file: {}", e))?;

        // Write header
        let header = CacheHeader {
            last_updated: state.last_updated,
            version: 1,
        };
        writeln!(file, "{}", serde_json::to_string(&header).unwrap())
            .map_err(|e| format!("Failed to write header: {}", e))?;

        // Write markets
        for market in state.markets.values() {
            writeln!(file, "{}", serde_json::to_string(market).unwrap())
                .map_err(|e| format!("Failed to write market: {}", e))?;
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
            }),
        }
    }

    /// Get tokens for a market by condition_id.
    /// Returns the full token info including outcomes.
    pub fn get_tokens(&self, condition_id: &str) -> Option<Vec<TokenInfo>> {
        let state = self.inner.read().unwrap();
        state.markets.get(condition_id).map(|m| m.tokens.clone())
    }

    /// Get a specific token by condition_id and outcome name.
    /// 
    /// # Example
    /// ```ignore
    /// let yes_token = catalog.get_token_by_outcome("condition123", "Yes");
    /// let trump_token = catalog.get_token_by_outcome("condition456", "Trump");
    /// ```
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

#[async_trait]
impl MarketCatalog for PolymarketCatalog {
    async fn refresh(&self) -> Result<usize, String> {
        info!("PolymarketCatalog: Starting refresh...");
        
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
                // Use .query() for proper URL encoding of base64 cursors
                // (which may contain +, /, = characters)
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

            // "LTE=" indicates end of pagination
            if body.next_cursor == "LTE=" || body.next_cursor.is_empty() {
                break;
            }

            next_cursor = Some(body.next_cursor);

            // Safety: prevent infinite loops
            if page > 1000 {
                warn!("PolymarketCatalog: Hit page limit, stopping");
                break;
            }
        }

        let count = all_markets.len();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Update state - brief lock, just swapping in the new data
        {
            let mut state = self.inner.write().unwrap();
            state.markets = all_markets;
            state.last_updated = now;
        }

        // Save to disk (also uses brief read lock)
        if let Err(e) = self.save_to_disk() {
            error!("PolymarketCatalog: Failed to save cache: {}", e);
        } else {
            info!("PolymarketCatalog: Saved {} markets to cache", count);
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

        // Sort by score descending
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
        let state = self.inner.read().unwrap();
        state.last_updated
    }

    fn len(&self) -> usize {
        let state = self.inner.read().unwrap();
        state.markets.len()
    }
}

/// Format a Unix timestamp for logging.
fn format_timestamp(ts: i64) -> String {
    if ts == 0 {
        return "never".to_string();
    }
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("unix:{}", ts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_scoring() {
        // Basic scoring test - would need mock data
    }
}

