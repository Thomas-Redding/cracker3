// src/catalog/derive.rs
//
// Derive (formerly Lyra) options catalog with historical time-travel support.
// Fetches and caches instrument metadata from Derive's public API.
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

const DERIVE_API_URL: &str = "https://api.lyra.finance/public";
const DEFAULT_CACHE_PATH: &str = "derive_instruments.jsonl";
#[allow(dead_code)]
const STALE_THRESHOLD_SECS: u64 = 3600; // 1 hour

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
}

impl DeriveCatalog {
    /// Create a new catalog for the specified currencies.
    pub async fn new(currencies: Vec<String>, cache_path: Option<&str>) -> Self {
        let cache_path = cache_path.unwrap_or(DEFAULT_CACHE_PATH).to_string();
        let state = Self::load_from_disk(&cache_path).unwrap_or_default();

        let loaded_count = state.instruments.len();
        let last_updated = state.last_updated;

        if loaded_count > 0 {
            info!(
                "DeriveCatalog: Loaded {} instruments from cache (updated {})",
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
}

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

    async fn refresh(&mut self) -> Result<CatalogDiff<DeriveInstrument>, String> {
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
        }

        Ok(diff)
    }

    fn last_updated(&self) -> u64 {
        self.inner.read().unwrap().last_updated
    }

    fn diffs(&self) -> &[CatalogDiff<DeriveInstrument>] {
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

