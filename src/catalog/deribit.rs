// src/catalog/deribit.rs
//
// Deribit options catalog with historical time-travel support.
// Fetches and caches instrument metadata from Deribit's public API.
// Options only add/remove (they don't change after listing, they just expire).

use super::{apply_diff, compute_diff, invert_diff, Catalog, CatalogDiff, CatalogFileEntry};
use async_trait::async_trait;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const DERIBIT_API_URL: &str = "https://www.deribit.com/api/v2";
const DEFAULT_CACHE_PATH: &str = "deribit_instruments.jsonl";
#[allow(dead_code)]
const STALE_THRESHOLD_SECS: u64 = 3600; // 1 hour (instruments change more frequently)

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
}

impl DeribitCatalog {
    /// Create a new catalog for the specified currencies.
    pub async fn new(currencies: Vec<String>, cache_path: Option<&str>) -> Self {
        let cache_path = cache_path.unwrap_or(DEFAULT_CACHE_PATH).to_string();
        let state = Self::load_from_disk(&cache_path).unwrap_or_default();

        let loaded_count = state.instruments.len();
        let last_updated = state.last_updated;

        if loaded_count > 0 {
            info!(
                "DeribitCatalog: Loaded {} instruments from cache (updated {})",
                loaded_count,
                format_timestamp(last_updated)
            );
        }

        Self {
            inner: RwLock::new(state),
            cache_path,
            http_client: reqwest::Client::new(),
            currencies,
        }
    }

    /// Create an empty catalog.
    pub fn new_empty(currencies: Vec<String>) -> Self {
        Self {
            inner: RwLock::new(CatalogState::default()),
            cache_path: DEFAULT_CACHE_PATH.to_string(),
            http_client: reqwest::Client::new(),
            currencies,
        }
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
}

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

    async fn refresh(&mut self) -> Result<CatalogDiff<DeribitInstrument>, String> {
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
        }

        Ok(diff)
    }

    fn last_updated(&self) -> u64 {
        self.inner.read().unwrap().last_updated
    }

    fn diffs(&self) -> &[CatalogDiff<DeribitInstrument>] {
        &[]
    }

    fn len(&self) -> usize {
        self.inner.read().unwrap().instruments.len()
    }
}

fn format_timestamp(ts: u64) -> String {
    if ts == 0 {
        return "never".to_string();
    }
    chrono::DateTime::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("unix:{}", ts))
}

