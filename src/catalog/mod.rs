// src/catalog/mod.rs
//
// Market discovery and catalog functionality.
// This is distinct from streaming (real-time prices) and execution (placing orders).
// Catalogs allow strategies to discover markets by slug, description, or pattern.

pub mod polymarket;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Re-export for convenience
pub use polymarket::PolymarketCatalog;

/// Metadata about a tradeable market.
/// Generic enough to work across exchanges, with exchange-specific data in `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketInfo {
    /// Primary identifier (condition_id for Polymarket, instrument_name for Deribit)
    pub id: String,
    /// Human-readable slug (e.g., "will-bitcoin-be-above-100000-on-december-31")
    pub slug: Option<String>,
    /// The market question or name
    pub question: Option<String>,
    /// Longer description of the market
    pub description: Option<String>,
    /// Categorization tags
    pub tags: Option<Vec<String>>,
    /// Token IDs for trading (YES/NO tokens for Polymarket)
    pub token_ids: Vec<String>,
    /// Exchange-specific fields (neg_risk_market_id, end_date, etc.)
    pub extra: serde_json::Value,
}

/// Search result with relevance scoring.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub market: MarketInfo,
    pub score: u32,
}

/// Trait for discovering markets on an exchange.
/// 
/// Implementations should be thread-safe and cache results locally.
/// The `refresh()` method fetches fresh data from the exchange (may take minutes).
#[async_trait]
pub trait MarketCatalog: Send + Sync {
    /// Refresh the catalog from the exchange.
    /// This may take several minutes as it paginates through all markets.
    async fn refresh(&self) -> Result<usize, String>;

    /// Search markets by text query.
    /// Scoring is weighted: slug (8x) > question (4x) > tags (2x) > description (1x).
    fn search(&self, query: &str, limit: usize) -> Vec<SearchResult>;

    /// Find market by exact slug.
    fn find_by_slug(&self, slug: &str) -> Option<MarketInfo>;

    /// Find markets matching a slug regex pattern.
    fn find_by_slug_regex(&self, pattern: &str) -> Result<Vec<MarketInfo>, String>;

    /// Find market containing a specific token ID.
    fn find_by_token_id(&self, token_id: &str) -> Option<MarketInfo>;

    /// Get market by its primary ID (condition_id for Polymarket).
    fn get(&self, id: &str) -> Option<MarketInfo>;

    /// Get all markets in the catalog.
    fn all(&self) -> Vec<MarketInfo>;

    /// Get the Unix timestamp (seconds) of the last refresh.
    fn last_updated(&self) -> i64;

    /// Get the number of markets in the catalog.
    fn len(&self) -> usize;

    /// Check if the catalog is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Shared catalog for use across strategies.
pub type SharedMarketCatalog = Arc<dyn MarketCatalog>;

