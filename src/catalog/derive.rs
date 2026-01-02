// src/catalog/derive.rs
//
// Derive (formerly Lyra) options catalog with historical time-travel support.
// Fetches and caches instrument metadata from Derive's public API.
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

const DERIVE_API_URL: &str = "https://api.lyra.finance/public";
const DEFAULT_CACHE_PATH: &str = "derive_instruments.jsonl";

/// Static flag to prevent multiple concurrent auto-refreshes.
static AUTO_REFRESH_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Derive instrument info.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeriveInstrument {
    /// Instrument name (e.g., "BTC-20251226-100000-C")
    pub instrument_name: String,
    /// Underlying asset (e.g., "BTC", "ETH")
    pub base_currency: String,
    /// Instrument type: "option", "perp"
    pub instrument_type: String,
    /// Option type: "C" (call) or "P" (put)
    pub option_type: Option<String>,
    /// Strike price
    pub strike: Option<f64>,
    /// Expiration date as YYYYMMDD
    pub expiry: Option<String>,
    /// Whether the instrument is active
    pub is_active: bool,
}

/// Response from Derive get_instruments endpoint.
#[derive(Debug, Deserialize)]
struct GetInstrumentsResponse {
    result: Option<Vec<DeriveApiInstrument>>,
}

#[derive(Debug, Deserialize)]
struct DeriveApiInstrument {
    instrument_name: String,
    #[serde(default)]
    base_currency: Option<String>,
    #[serde(default)]
    instrument_type: Option<String>,
    #[serde(default)]
    option_type: Option<String>,
    #[serde(default)]
    is_active: Option<bool>,
}

/// Internal state for the catalog.
struct CatalogState {
    instruments: HashMap<String, DeriveInstrument>,
    diffs: Vec<CatalogDiff<DeriveInstrument>>,
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

/// Derive instrument catalog with historical time-travel support.
pub struct DeriveCatalog {
    inner: RwLock<CatalogState>,
    cache_path: String,
    http_client: reqwest::Client,
    /// Currencies to fetch (e.g., ["BTC", "ETH"])
    currencies: Vec<String>,
    /// Stale threshold in seconds (triggers auto-refresh when exceeded)
    stale_threshold_secs: u64,
}

impl DeriveCatalog {
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
                "DeriveCatalog: Loaded {} instruments, {} diffs from cache (updated {})",
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
                info!("DeriveCatalog: Cache is stale, spawning background refresh...");
                let catalog_clone = catalog.clone();
                tokio::spawn(async move {
                    // Guard ensures flag is reset even if this task panics
                    let _guard = AutoRefreshGuard::new(&AUTO_REFRESH_IN_PROGRESS);
                    match catalog_clone.refresh_internal().await {
                        Ok(diff) => info!(
                            "DeriveCatalog: Background refresh complete, {} changes",
                            diff.change_count()
                        ),
                        Err(e) => error!("DeriveCatalog: Background refresh failed: {}", e),
                    }
                });
            } else {
                info!("DeriveCatalog: Cache is stale, but refresh already in progress");
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
        let first_entry: CatalogFileEntry<DeriveInstrument> =
            serde_json::from_str(&first_line).ok()?;

        let (instruments, last_updated) = match first_entry {
            CatalogFileEntry::Current { timestamp, items } => {
                let map: HashMap<String, DeriveInstrument> = items
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

            let entry: CatalogFileEntry<DeriveInstrument> = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    warn!("DeriveCatalog: Failed to parse entry: {}", e);
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
            let diff_entry: CatalogFileEntry<DeriveInstrument> =
                CatalogFileEntry::Diff(diff.clone());
            writeln!(file, "{}", serde_json::to_string(&diff_entry).unwrap())
                .map_err(|e| format!("Failed to write diff: {}", e))?;
        }

        Ok(())
    }

    /// Parse instrument name to extract components.
    fn parse_instrument_name(name: &str) -> (Option<String>, Option<f64>, Option<String>) {
        // Format: BTC-YYYYMMDD-STRIKE-TYPE
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() >= 4 {
            let expiry = Some(parts[1].to_string());
            let strike = parts[2].parse().ok();
            let option_type = Some(parts[3].to_string());
            (expiry, strike, option_type)
        } else {
            (None, None, None)
        }
    }

    async fn fetch_all_instruments(&self) -> Result<HashMap<String, DeriveInstrument>, String> {
        let mut all_instruments = HashMap::new();

        for currency in &self.currencies {
            let url = format!(
                "{}/get_instruments?currency={}&instrument_type=option&expired=false",
                DERIVE_API_URL, currency
            );

            let response = self
                .http_client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("HTTP request failed: {}", e))?;

            if !response.status().is_success() {
                warn!(
                    "DeriveCatalog: Failed to fetch {}: {}",
                    currency,
                    response.status()
                );
                continue;
            }

            let body: GetInstrumentsResponse = response
                .json()
                .await
                .map_err(|e| format!("Failed to parse response: {}", e))?;

            for api_inst in body.result.unwrap_or_default() {
                let (expiry, strike, option_type) =
                    Self::parse_instrument_name(&api_inst.instrument_name);

                let inst = DeriveInstrument {
                    instrument_name: api_inst.instrument_name.clone(),
                    base_currency: api_inst.base_currency.unwrap_or_else(|| currency.clone()),
                    instrument_type: api_inst.instrument_type.unwrap_or_else(|| "option".to_string()),
                    option_type: api_inst.option_type.or(option_type),
                    strike,
                    expiry,
                    is_active: api_inst.is_active.unwrap_or(true),
                };
                all_instruments.insert(inst.instrument_name.clone(), inst);
            }
        }

        info!(
            "DeriveCatalog: Fetched {} instruments for {:?}",
            all_instruments.len(),
            self.currencies
        );

        Ok(all_instruments)
    }

    /// Get all active options for a currency.
    pub fn get_options(&self, currency: &str) -> Vec<DeriveInstrument> {
        let state = self.inner.read().unwrap();
        state
            .instruments
            .values()
            .filter(|i| {
                i.instrument_type == "option" && i.base_currency == currency && i.is_active
            })
            .cloned()
            .collect()
    }

    /// Get instrument by name.
    pub fn get(&self, instrument_name: &str) -> Option<DeriveInstrument> {
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
    ) -> Vec<DeriveInstrument> {
        let state = self.inner.read().unwrap();
        state
            .instruments
            .values()
            .filter(|i| {
                if i.instrument_type != "option" || i.base_currency != currency || !i.is_active {
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
    async fn refresh_internal(&self) -> Result<CatalogDiff<DeriveInstrument>, String> {
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
            error!("DeriveCatalog: Failed to save cache: {}", e);
        } else {
            let state = self.inner.read().unwrap();
            info!(
                "DeriveCatalog: Saved {} instruments, {} diffs to cache",
                state.instruments.len(),
                state.diffs.len()
            );
        }

        Ok(diff)
    }
}

impl DeriveInstrument {
    /// Get expiration as a chrono DateTime (for cross-exchange comparison).
    /// 
    /// Parses the YYYYMMDD format expiry string.
    pub fn expiration_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        use chrono::{NaiveDate, TimeZone, Utc};
        
        self.expiry.as_ref().and_then(|expiry_str| {
            // Parse YYYYMMDD format
            NaiveDate::parse_from_str(expiry_str, "%Y%m%d")
                .ok()
                .and_then(|date| {
                    // Expiry is typically at 08:00 UTC for options
                    date.and_hms_opt(8, 0, 0)
                        .map(|dt| Utc.from_utc_datetime(&dt))
                })
        })
    }

    /// Get expiration as Unix timestamp in seconds.
    pub fn expiration_unix_secs(&self) -> Option<i64> {
        self.expiration_datetime().map(|dt| dt.timestamp())
    }
}

// Implement Refreshable (used by Engine)
#[async_trait]
impl Refreshable for DeriveCatalog {
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
impl Catalog for DeriveCatalog {
    type Item = DeriveInstrument;

    fn item_id(item: &Self::Item) -> String {
        item.instrument_name.clone()
    }

    fn current(&self) -> HashMap<String, DeriveInstrument> {
        self.inner.read().unwrap().instruments.clone()
    }

    fn as_of(&self, timestamp: u64) -> HashMap<String, DeriveInstrument> {
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

    async fn refresh_with_diff(&self) -> Result<CatalogDiff<DeriveInstrument>, String> {
        self.refresh_internal().await
    }

    fn diffs(&self) -> Vec<CatalogDiff<DeriveInstrument>> {
        self.inner.read().unwrap().diffs.clone()
    }

    fn len(&self) -> usize {
        self.inner.read().unwrap().instruments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_instrument(name: &str, currency: &str, strike: f64, expiry: &str) -> DeriveInstrument {
        DeriveInstrument {
            instrument_name: name.to_string(),
            base_currency: currency.to_string(),
            instrument_type: "option".to_string(),
            option_type: Some("C".to_string()),
            strike: Some(strike),
            expiry: Some(expiry.to_string()),
            is_active: true,
        }
    }

    fn make_test_catalog(instruments: HashMap<String, DeriveInstrument>) -> DeriveCatalog {
        DeriveCatalog {
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
            make_test_instrument("BTC-TEST", "BTC", 50000.0, "20251226"),
        );

        let catalog = make_test_catalog(instruments);

        let result = catalog.as_of(500);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("BTC-TEST"));
    }

    #[test]
    fn test_expiration_datetime() {
        let inst = make_test_instrument("BTC-TEST", "BTC", 50000.0, "20251226");
        let dt = inst.expiration_datetime().unwrap();
        // 2025-12-26 08:00:00 UTC
        assert_eq!(dt.format("%Y-%m-%d %H:%M:%S").to_string(), "2025-12-26 08:00:00");
    }

    #[test]
    fn test_expiration_unix_secs() {
        let inst = make_test_instrument("BTC-TEST", "BTC", 50000.0, "20251226");
        let unix_secs = inst.expiration_unix_secs().unwrap();
        // Should be approximately 2025-12-26 08:00:00 UTC
        assert!(unix_secs > 1735200000);
    }

    #[test]
    fn test_parse_instrument_name() {
        let (expiry, strike, option_type) = DeriveCatalog::parse_instrument_name("BTC-20251226-100000-C");
        assert_eq!(expiry, Some("20251226".to_string()));
        assert_eq!(strike, Some(100000.0));
        assert_eq!(option_type, Some("C".to_string()));
    }

    #[test]
    fn test_parse_instrument_name_invalid() {
        let (expiry, strike, option_type) = DeriveCatalog::parse_instrument_name("BTC-PERP");
        assert_eq!(expiry, None);
        assert_eq!(strike, None);
        assert_eq!(option_type, None);
    }

    #[test]
    fn test_get_options() {
        let mut instruments = HashMap::new();
        instruments.insert(
            "BTC-OPT".to_string(),
            make_test_instrument("BTC-OPT", "BTC", 50000.0, "20251226"),
        );
        
        let mut perp = make_test_instrument("BTC-PERP", "BTC", 50000.0, "20251226");
        perp.instrument_type = "perp".to_string();
        instruments.insert("BTC-PERP".to_string(), perp);

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
            make_test_instrument("BTC-50K", "BTC", 50000.0, "20251226"),
        );
        instruments.insert(
            "BTC-60K".to_string(),
            make_test_instrument("BTC-60K", "BTC", 60000.0, "20251226"),
        );
        instruments.insert(
            "BTC-70K".to_string(),
            make_test_instrument("BTC-70K", "BTC", 70000.0, "20251226"),
        );

        let catalog = make_test_catalog(instruments);

        let filtered = catalog.filter_options("BTC", 55000.0, 65000.0, None);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].instrument_name, "BTC-60K");
    }
}

