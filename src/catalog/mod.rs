// src/catalog/mod.rs
//
// Market discovery and catalog functionality with historical time-travel support.
// Catalogs allow strategies to discover markets by slug, description, or pattern,
// and can reconstruct the catalog state as of any historical timestamp.

pub mod deribit;
pub mod derive;
pub mod polymarket;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// Re-export for convenience
pub use deribit::{DeribitCatalog, DeribitInstrument};
pub use derive::{DeriveCatalog, DeriveInstrument};
pub use polymarket::PolymarketCatalog;

// =============================================================================
// Historical Diff Types
// =============================================================================

/// A diff representing changes to the catalog between two points in time.
/// 
/// Used for time-travel: start with current state, walk backwards through
/// diffs inverting each one until reaching the target timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogDiff<T> {
    /// Unix timestamp (seconds) when this diff was recorded.
    pub timestamp: u64,
    /// Items that were added in this update (to invert: remove these).
    pub added: Vec<T>,
    /// Items that were removed in this update (to invert: add these back).
    pub removed: Vec<T>,
    /// Items that were modified (before/after pairs).
    pub modified: Vec<Modification<T>>,
}

/// Represents a modification to an item, storing both before and after states.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Modification<T> {
    /// The state before this modification.
    pub before: T,
    /// The state after this modification.
    pub after: T,
}

impl<T: Clone> CatalogDiff<T> {
    /// Creates an empty diff with the given timestamp.
    pub fn empty(timestamp: u64) -> Self {
        Self {
            timestamp,
            added: Vec::new(),
            removed: Vec::new(),
            modified: Vec::new(),
        }
    }

    /// Returns true if this diff has no changes.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.modified.is_empty()
    }

    /// Returns the total number of changes in this diff.
    pub fn change_count(&self) -> usize {
        self.added.len() + self.removed.len() + self.modified.len()
    }
}

/// Entry types for the JSONL file format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogFileEntry<T> {
    /// The current/latest state snapshot.
    Current {
        timestamp: u64,
        items: Vec<T>,
    },
    /// A historical diff entry.
    Diff(CatalogDiff<T>),
}

// =============================================================================
// Catalog Trait
// =============================================================================

/// Trait for catalogs that support historical time-travel.
/// 
/// Implementations store the current state plus a series of diffs,
/// enabling reconstruction of the catalog state at any past timestamp.
/// 
/// # Type Parameters
/// * `Item` - The type of items stored in the catalog (e.g., `MarketInfo`)
#[async_trait]
pub trait Catalog: Send + Sync {
    /// The type of items stored in this catalog.
    type Item: Serialize + DeserializeOwned + Clone + Send + Sync;

    /// Returns the unique identifier for an item (used for diff tracking).
    fn item_id(item: &Self::Item) -> String;

    /// Returns the current state of the catalog.
    fn current(&self) -> HashMap<String, Self::Item>;

    /// Reconstructs the catalog state as it existed at the given timestamp.
    /// 
    /// Works by starting with the current state and walking backwards through
    /// diffs, inverting each one, until reaching a diff older than the target.
    /// 
    /// # Arguments
    /// * `timestamp` - Unix timestamp (seconds) to reconstruct state for
    fn as_of(&self, timestamp: u64) -> HashMap<String, Self::Item>;

    /// Refreshes the catalog from the exchange and records any changes as a diff.
    /// 
    /// # Returns
    /// The diff containing all changes, or an error if refresh failed.
    async fn refresh(&mut self) -> Result<CatalogDiff<Self::Item>, String>;

    /// Returns the Unix timestamp of the last refresh.
    fn last_updated(&self) -> u64;

    /// Returns all historical diffs, newest first.
    fn diffs(&self) -> &[CatalogDiff<Self::Item>];

    /// Returns the number of items currently in the catalog.
    fn len(&self) -> usize {
        self.current().len()
    }

    /// Returns true if the catalog is empty.
    fn is_empty(&self) -> bool {
        self.current().is_empty()
    }
}

/// Helper function to compute a diff between old and new states.
pub fn compute_diff<T, F>(
    old_state: &HashMap<String, T>,
    new_state: &HashMap<String, T>,
    timestamp: u64,
    _id_fn: F,
) -> CatalogDiff<T>
where
    T: Clone + PartialEq,
    F: Fn(&T) -> String,
{
    let mut diff = CatalogDiff::empty(timestamp);

    // Find added items (in new but not in old)
    for (id, item) in new_state {
        if !old_state.contains_key(id) {
            diff.added.push(item.clone());
        }
    }

    // Find removed items (in old but not in new)
    for (id, item) in old_state {
        if !new_state.contains_key(id) {
            diff.removed.push(item.clone());
        }
    }

    // Find modified items (in both, but different)
    for (id, new_item) in new_state {
        if let Some(old_item) = old_state.get(id) {
            if old_item != new_item {
                diff.modified.push(Modification {
                    before: old_item.clone(),
                    after: new_item.clone(),
                });
            }
        }
    }

    diff
}

/// Helper function to invert a diff (for walking backwards in time).
pub fn invert_diff<T: Clone>(diff: &CatalogDiff<T>) -> CatalogDiff<T> {
    CatalogDiff {
        timestamp: diff.timestamp,
        // Swap added/removed
        added: diff.removed.clone(),
        removed: diff.added.clone(),
        // Swap before/after in modifications
        modified: diff
            .modified
            .iter()
            .map(|m| Modification {
                before: m.after.clone(),
                after: m.before.clone(),
            })
            .collect(),
    }
}

/// Apply a diff to a state (for reconstruction).
pub fn apply_diff<T, F>(state: &mut HashMap<String, T>, diff: &CatalogDiff<T>, id_fn: F)
where
    T: Clone,
    F: Fn(&T) -> String,
{
    // Remove items that were added (we're going backwards)
    for item in &diff.removed {
        state.remove(&id_fn(item));
    }

    // Add items that were removed (we're going backwards)
    for item in &diff.added {
        let id = id_fn(item);
        state.insert(id, item.clone());
    }

    // Apply modifications (use 'after' since we're going backwards and diff is already inverted)
    for modification in &diff.modified {
        let id = id_fn(&modification.after);
        state.insert(id, modification.after.clone());
    }
}

// =============================================================================
// Legacy Types (for backwards compatibility with existing MarketCatalog trait)
// =============================================================================

/// A tradeable token within a market.
/// 
/// For Polymarket, each condition_id has exactly two tokens representing
/// the two sides of the bet. The `outcome` field identifies what each token
/// represents (e.g., "Yes"/"No" or "Trump"/"Harris").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenInfo {
    /// The token ID used for trading
    pub token_id: String,
    /// The outcome this token represents (e.g., "Yes", "No", "Trump", "Harris")
    pub outcome: Option<String>,
}

/// Metadata about a tradeable market.
/// Generic enough to work across exchanges, with exchange-specific data in `extra`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Tokens for trading, with their associated outcomes
    pub tokens: Vec<TokenInfo>,
    /// Exchange-specific fields (neg_risk_market_id, end_date, etc.)
    pub extra: serde_json::Value,
}

impl MarketInfo {
    /// Get all token IDs (convenience method, loses outcome association).
    pub fn token_ids(&self) -> Vec<&str> {
        self.tokens.iter().map(|t| t.token_id.as_str()).collect()
    }

    /// Find a token by its outcome name (case-insensitive).
    /// 
    /// # Example
    /// ```ignore
    /// let yes_token = market.token_by_outcome("Yes");
    /// let no_token = market.token_by_outcome("No");
    /// ```
    pub fn token_by_outcome(&self, outcome: &str) -> Option<&TokenInfo> {
        let outcome_lower = outcome.to_lowercase();
        self.tokens.iter().find(|t| {
            t.outcome
                .as_ref()
                .map(|o| o.to_lowercase() == outcome_lower)
                .unwrap_or(false)
        })
    }

    /// Get the "Yes" token if it exists.
    pub fn yes_token(&self) -> Option<&TokenInfo> {
        self.token_by_outcome("Yes")
    }

    /// Get the "No" token if it exists.
    pub fn no_token(&self) -> Option<&TokenInfo> {
        self.token_by_outcome("No")
    }
}

/// Search result with relevance scoring.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub market: MarketInfo,
    pub score: u32,
}

/// Legacy trait for discovering markets on an exchange.
/// 
/// This trait is kept for backwards compatibility. New catalogs should
/// implement the `Catalog` trait instead for time-travel support.
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestItem {
        id: String,
        name: String,
        value: i32,
    }

    fn item_id(item: &TestItem) -> String {
        item.id.clone()
    }

    #[test]
    fn test_compute_diff_added() {
        let old: HashMap<String, TestItem> = HashMap::new();
        let mut new = HashMap::new();
        new.insert(
            "a".to_string(),
            TestItem {
                id: "a".to_string(),
                name: "Item A".to_string(),
                value: 1,
            },
        );

        let diff = compute_diff(&old, &new, 1000, item_id);

        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.removed.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.added[0].id, "a");
    }

    #[test]
    fn test_compute_diff_removed() {
        let mut old = HashMap::new();
        old.insert(
            "a".to_string(),
            TestItem {
                id: "a".to_string(),
                name: "Item A".to_string(),
                value: 1,
            },
        );
        let new: HashMap<String, TestItem> = HashMap::new();

        let diff = compute_diff(&old, &new, 1000, item_id);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.removed[0].id, "a");
    }

    #[test]
    fn test_compute_diff_modified() {
        let mut old = HashMap::new();
        old.insert(
            "a".to_string(),
            TestItem {
                id: "a".to_string(),
                name: "Item A".to_string(),
                value: 1,
            },
        );
        let mut new = HashMap::new();
        new.insert(
            "a".to_string(),
            TestItem {
                id: "a".to_string(),
                name: "Item A Modified".to_string(),
                value: 2,
            },
        );

        let diff = compute_diff(&old, &new, 1000, item_id);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.removed.len(), 0);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].before.value, 1);
        assert_eq!(diff.modified[0].after.value, 2);
    }

    #[test]
    fn test_invert_diff() {
        let diff = CatalogDiff {
            timestamp: 1000,
            added: vec![TestItem {
                id: "a".to_string(),
                name: "A".to_string(),
                value: 1,
            }],
            removed: vec![TestItem {
                id: "b".to_string(),
                name: "B".to_string(),
                value: 2,
            }],
            modified: vec![Modification {
                before: TestItem {
                    id: "c".to_string(),
                    name: "C".to_string(),
                    value: 3,
                },
                after: TestItem {
                    id: "c".to_string(),
                    name: "C".to_string(),
                    value: 4,
                },
            }],
        };

        let inverted = invert_diff(&diff);

        // Added becomes removed and vice versa
        assert_eq!(inverted.added.len(), 1);
        assert_eq!(inverted.added[0].id, "b");
        assert_eq!(inverted.removed.len(), 1);
        assert_eq!(inverted.removed[0].id, "a");

        // Before/after swapped
        assert_eq!(inverted.modified[0].before.value, 4);
        assert_eq!(inverted.modified[0].after.value, 3);
    }
}
