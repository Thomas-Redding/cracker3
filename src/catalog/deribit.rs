// src/catalog/deribit.rs
//
// Deribit options catalog with historical time-travel support.
// Fetches and caches instrument metadata from Deribit's public API.
// Options only add/remove (they don't change after listing, they just expire).

use super::{
    apply_diff, compute_diff, format_timestamp, invert_diff, AutoRefreshGuard, Catalog,
    CatalogDiff, CatalogFileEntry, Refreshable, DEFAULT_OPTIONS_STALE_THRESHOLD_SECS,
};
use async_trait::async_trait;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const DERIBIT_API_URL: &str = "https://www.deribit.com/api/v2";
const DEFAULT_CACHE_PATH: &str = "deribit_instruments.jsonl";

/// Static flag to prevent multiple concurrent auto-refreshes.
static AUTO_REFRESH_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Deribit instrument info.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeribitInstrument {
    /// Instrument name (e.g., "BTC-29MAR24-60000-C")
    pub instrument_name: String,
    /// Underlying asset (e.g., "BTC", "ETH")
    pub base_currency: String,
    /// Quote currency (usually "USD")
    pub quote_currency: String,
    /// Instrument type: "option", "future", "spot"
    pub kind: String,
    /// Option type: "call" or "put" (None for non-options)
    pub option_type: Option<String>,
    /// Strike price (None for non-options)
    pub strike: Option<f64>,
    /// Expiration timestamp in milliseconds
    pub expiration_timestamp: i64,
    /// Whether the instrument is active
    pub is_active: bool,
    /// Contract size
    pub contract_size: f64,
    /// Minimum trade amount
    pub min_trade_amount: f64,
    /// Tick size
    pub tick_size: f64,
}

/// Response from Deribit get_instruments endpoint.
#[derive(Debug, Deserialize)]
struct GetInstrumentsResponse {
    result: Vec<DeribitApiInstrument>,
}

#[derive(Debug, Deserialize)]
struct DeribitApiInstrument {
    instrument_name: String,
    base_currency: String,
    quote_currency: String,
    kind: String,
    option_type: Option<String>,
    strike: Option<f64>,
    expiration_timestamp: i64,
    is_active: bool,
    contract_size: f64,
    min_trade_amount: f64,
    tick_size: f64,
}

/// Internal state for the catalog.
struct CatalogState {
    instruments: HashMap<String, DeribitInstrument>,
    diffs: Vec<CatalogDiff<DeribitInstrument>>,
    last_updated: u64,
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            instruments: HashMap::new(),
            diffs: Vec::new(),
            last_updated: 0,
        }
    }
}

/// Deribit instrument catalog with historical time-travel support.
pub struct DeribitCatalog {
    inner: RwLock<CatalogState>,
    cache_path: String,
    http_client: reqwest::Client,
    /// Currencies to fetch (e.g., ["BTC", "ETH"])
    currencies: Vec<String>,
    /// Stale threshold in seconds (triggers auto-refresh when exceeded)
    stale_threshold_secs: u64,
}

impl DeribitCatalog {
    /// Create a new catalog for the specified currencies.
    /// 
    /// If the cache is stale, spawns a background refresh task.
    pub async fn new(
        currencies: Vec<String>,
        cache_path: Option<&str>,
        stale_threshold_secs: Option<u64>,
    ) -> Arc<Self> {
        let cache_path = cache_path.unwrap_or(DEFAULT_CACHE_PATH).to_string();
        let stale_threshold_secs = stale_threshold_secs.unwrap_or(DEFAULT_OPTIONS_STALE_THRESHOLD_SECS);
        let state = Self::load_from_disk(&cache_path).unwrap_or_default();

        let loaded_count = state.instruments.len();
        let last_updated = state.last_updated;
        let diff_count = state.diffs.len();

        let catalog = Arc::new(Self {
            inner: RwLock::new(state),
            cache_path,
            http_client: reqwest::Client::new(),
            currencies,
            stale_threshold_secs,
        });

        if loaded_count > 0 {
            info!(
                "DeribitCatalog: Loaded {} instruments, {} diffs from cache (updated {})",
                loaded_count,
                diff_count,
                format_timestamp(last_updated)
            );
        }

        // Check staleness and auto-refresh in background
        if catalog.is_stale_internal(last_updated) {
            if AUTO_REFRESH_IN_PROGRESS
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                info!("DeribitCatalog: Cache is stale, spawning background refresh...");
                let catalog_clone = catalog.clone();
                tokio::spawn(async move {
                    // Guard ensures flag is reset even if this task panics
                    let _guard = AutoRefreshGuard::new(&AUTO_REFRESH_IN_PROGRESS);
                    match catalog_clone.refresh_internal().await {
                        Ok(diff) => info!(
                            "DeribitCatalog: Background refresh complete, {} changes",
                            diff.change_count()
                        ),
                        Err(e) => error!("DeribitCatalog: Background refresh failed: {}", e),
                    }
                });
            } else {
                info!("DeribitCatalog: Cache is stale, but refresh already in progress");
            }
        }

        catalog
    }

    /// Create an empty catalog.
    pub fn new_empty(currencies: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(CatalogState::default()),
            cache_path: DEFAULT_CACHE_PATH.to_string(),
            http_client: reqwest::Client::new(),
            currencies,
            stale_threshold_secs: DEFAULT_OPTIONS_STALE_THRESHOLD_SECS,
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

        let first_line = lines.next()?.ok()?;
        let first_entry: CatalogFileEntry<DeribitInstrument> =
            serde_json::from_str(&first_line).ok()?;

        let (instruments, last_updated) = match first_entry {
            CatalogFileEntry::Current { timestamp, items } => {
                let map: HashMap<String, DeribitInstrument> = items
                    .into_iter()
                    .map(|i| (i.instrument_name.clone(), i))
                    .collect();
                (map, timestamp)
            }
            _ => return None,
        };

        let mut diffs = Vec::new();
        for line in lines {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            if line.trim().is_empty() {
                continue;
            }

            let entry: CatalogFileEntry<DeribitInstrument> = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    warn!("DeribitCatalog: Failed to parse entry: {}", e);
                    continue;
                }
            };

            if let CatalogFileEntry::Diff(diff) = entry {
                diffs.push(diff);
            }
        }

        Some(CatalogState {
            instruments,
            diffs,
            last_updated,
        })
    }

    fn save_to_disk(&self) -> Result<(), String> {
        let state = self.inner.read().unwrap();

        let mut file = File::create(&self.cache_path)
            .map_err(|e| format!("Failed to create cache file: {}", e))?;

        let current_entry = CatalogFileEntry::Current {
            timestamp: state.last_updated,
            items: state.instruments.values().cloned().collect(),
        };
        writeln!(file, "{}", serde_json::to_string(&current_entry).unwrap())
            .map_err(|e| format!("Failed to write current state: {}", e))?;

        for diff in &state.diffs {
            let diff_entry: CatalogFileEntry<DeribitInstrument> =
                CatalogFileEntry::Diff(diff.clone());
            writeln!(file, "{}", serde_json::to_string(&diff_entry).unwrap())
                .map_err(|e| format!("Failed to write diff: {}", e))?;
        }

        Ok(())
    }

    async fn fetch_all_instruments(&self) -> Result<HashMap<String, DeribitInstrument>, String> {
        let mut all_instruments = HashMap::new();

        for currency in &self.currencies {
            for kind in &["option", "future"] {
                let url = format!(
                    "{}/public/get_instruments?currency={}&kind={}",
                    DERIBIT_API_URL, currency, kind
                );

                let response = self
                    .http_client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| format!("HTTP request failed: {}", e))?;

                if !response.status().is_success() {
                    warn!(
                        "DeribitCatalog: Failed to fetch {} {}: {}",
                        currency,
                        kind,
                        response.status()
                    );
                    continue;
                }

                let body: GetInstrumentsResponse = response
                    .json()
                    .await
                    .map_err(|e| format!("Failed to parse response: {}", e))?;

                for api_inst in body.result {
                    let inst = DeribitInstrument {
                        instrument_name: api_inst.instrument_name.clone(),
                        base_currency: api_inst.base_currency,
                        quote_currency: api_inst.quote_currency,
                        kind: api_inst.kind,
                        option_type: api_inst.option_type,
                        strike: api_inst.strike,
                        expiration_timestamp: api_inst.expiration_timestamp,
                        is_active: api_inst.is_active,
                        contract_size: api_inst.contract_size,
                        min_trade_amount: api_inst.min_trade_amount,
                        tick_size: api_inst.tick_size,
                    };
                    all_instruments.insert(inst.instrument_name.clone(), inst);
                }
            }
        }

        info!(
            "DeribitCatalog: Fetched {} instruments for {:?}",
            all_instruments.len(),
            self.currencies
        );

        Ok(all_instruments)
    }

    /// Get all active options for a currency.
    pub fn get_options(&self, currency: &str) -> Vec<DeribitInstrument> {
        let state = self.inner.read().unwrap();
        state
            .instruments
            .values()
            .filter(|i| i.kind == "option" && i.base_currency == currency && i.is_active)
            .cloned()
            .collect()
    }

    /// Get all futures for a currency.
    pub fn get_futures(&self, currency: &str) -> Vec<DeribitInstrument> {
        let state = self.inner.read().unwrap();
        state
            .instruments
            .values()
            .filter(|i| i.kind == "future" && i.base_currency == currency && i.is_active)
            .cloned()
            .collect()
    }

    /// Get instrument by name.
    pub fn get(&self, instrument_name: &str) -> Option<DeribitInstrument> {
        let state = self.inner.read().unwrap();
        state.instruments.get(instrument_name).cloned()
    }

    /// Filter options by strike range and type.
    pub fn filter_options(
        &self,
        currency: &str,
        min_strike: f64,
        max_strike: f64,
        option_type: Option<&str>,
    ) -> Vec<DeribitInstrument> {
        let state = self.inner.read().unwrap();
        state
            .instruments
            .values()
            .filter(|i| {
                if i.kind != "option" || i.base_currency != currency || !i.is_active {
                    return false;
                }
                if let Some(strike) = i.strike {
                    if strike < min_strike || strike > max_strike {
                        return false;
                    }
                } else {
                    return false;
                }
                if let Some(otype) = option_type {
                    if i.option_type.as_deref() != Some(otype) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    /// Internal refresh that works with &self (for background tasks).
    async fn refresh_internal(&self) -> Result<CatalogDiff<DeribitInstrument>, String> {
        let new_instruments = self.fetch_all_instruments().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let diff = {
            let state = self.inner.read().unwrap();
            compute_diff(&state.instruments, &new_instruments, now, Self::item_id)
        };

        {
            let mut state = self.inner.write().unwrap();
            if !diff.is_empty() {
                state.diffs.insert(0, diff.clone());
            }
            state.instruments = new_instruments;
            state.last_updated = now;
        }

        if let Err(e) = self.save_to_disk() {
            error!("DeribitCatalog: Failed to save cache: {}", e);
        } else {
            let state = self.inner.read().unwrap();
            info!(
                "DeribitCatalog: Saved {} instruments, {} diffs to cache",
                state.instruments.len(),
                state.diffs.len()
            );
        }

        Ok(diff)
    }
}

impl DeribitInstrument {
    /// Get expiration as a chrono DateTime (for cross-exchange comparison).
    pub fn expiration_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.expiration_timestamp)
    }

    /// Get expiration as Unix timestamp in seconds.
    pub fn expiration_unix_secs(&self) -> i64 {
        self.expiration_timestamp / 1000
    }
}

// Implement Refreshable (used by Engine)
#[async_trait]
impl Refreshable for DeribitCatalog {
    async fn refresh(&self) -> Result<usize, String> {
        let diff = self.refresh_internal().await?;
        Ok(diff.change_count())
    }

    fn last_updated(&self) -> u64 {
        self.inner.read().unwrap().last_updated
    }
}

// Implement Catalog (extends Refreshable with time-travel)
#[async_trait]
impl Catalog for DeribitCatalog {
    type Item = DeribitInstrument;

    fn item_id(item: &Self::Item) -> String {
        item.instrument_name.clone()
    }

    fn current(&self) -> HashMap<String, DeribitInstrument> {
        self.inner.read().unwrap().instruments.clone()
    }

    fn as_of(&self, timestamp: u64) -> HashMap<String, DeribitInstrument> {
        let state = self.inner.read().unwrap();
        let mut result = state.instruments.clone();

        for diff in &state.diffs {
            if diff.timestamp <= timestamp {
                break;
            }
            let inverted = invert_diff(diff);
            apply_diff(&mut result, &inverted, Self::item_id);
        }

        result
    }

    async fn refresh_with_diff(&self) -> Result<CatalogDiff<DeribitInstrument>, String> {
        self.refresh_internal().await
    }

    fn diffs(&self) -> Vec<CatalogDiff<DeribitInstrument>> {
        self.inner.read().unwrap().diffs.clone()
    }

    fn len(&self) -> usize {
        self.inner.read().unwrap().instruments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_instrument(name: &str, currency: &str, strike: f64) -> DeribitInstrument {
        DeribitInstrument {
            instrument_name: name.to_string(),
            base_currency: currency.to_string(),
            quote_currency: "USD".to_string(),
            kind: "option".to_string(),
            option_type: Some("call".to_string()),
            strike: Some(strike),
            expiration_timestamp: 1735689600000, // 2025-01-01 00:00:00 UTC
            is_active: true,
            contract_size: 1.0,
            min_trade_amount: 0.1,
            tick_size: 0.0001,
        }
    }

    fn make_test_catalog(instruments: HashMap<String, DeribitInstrument>) -> DeribitCatalog {
        DeribitCatalog {
            inner: RwLock::new(CatalogState {
                instruments,
                diffs: vec![],
                last_updated: 1000,
            }),
            cache_path: "test.jsonl".to_string(),
            http_client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            currencies: vec!["BTC".to_string()],
            stale_threshold_secs: DEFAULT_OPTIONS_STALE_THRESHOLD_SECS,
        }
    }

    #[test]
    fn test_as_of_with_no_diffs() {
        let mut instruments = HashMap::new();
        instruments.insert(
            "BTC-TEST".to_string(),
            make_test_instrument("BTC-TEST", "BTC", 50000.0),
        );

        let catalog = make_test_catalog(instruments);

        let result = catalog.as_of(500);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("BTC-TEST"));
    }

    #[test]
    fn test_expiration_datetime() {
        let inst = make_test_instrument("BTC-TEST", "BTC", 50000.0);
        let dt = inst.expiration_datetime().unwrap();
        assert_eq!(dt.timestamp(), 1735689600);
    }

    #[test]
    fn test_expiration_unix_secs() {
        let inst = make_test_instrument("BTC-TEST", "BTC", 50000.0);
        assert_eq!(inst.expiration_unix_secs(), 1735689600);
    }

    #[test]
    fn test_get_options() {
        let mut instruments = HashMap::new();
        instruments.insert(
            "BTC-OPT".to_string(),
            make_test_instrument("BTC-OPT", "BTC", 50000.0),
        );
        
        let mut future = make_test_instrument("BTC-FUT", "BTC", 50000.0);
        future.kind = "future".to_string();
        instruments.insert("BTC-FUT".to_string(), future);

        let catalog = make_test_catalog(instruments);

        let options = catalog.get_options("BTC");
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].instrument_name, "BTC-OPT");
    }

    #[test]
    fn test_filter_options() {
        let mut instruments = HashMap::new();
        instruments.insert(
            "BTC-50K".to_string(),
            make_test_instrument("BTC-50K", "BTC", 50000.0),
        );
        instruments.insert(
            "BTC-60K".to_string(),
            make_test_instrument("BTC-60K", "BTC", 60000.0),
        );
        instruments.insert(
            "BTC-70K".to_string(),
            make_test_instrument("BTC-70K", "BTC", 70000.0),
        );

        let catalog = make_test_catalog(instruments);

        let filtered = catalog.filter_options("BTC", 55000.0, 65000.0, None);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].instrument_name, "BTC-60K");
    }
}

