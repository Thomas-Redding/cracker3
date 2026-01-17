// src/strategy/cross_market.rs
//
// Cross-market mispricing strategy using Deribit as volatility source of truth.
// Identifies opportunities on Polymarket (binary options) and Derive (vanilla options).

use crate::catalog::{DeribitCatalog, DeriveCatalog, MarketCatalog, PolymarketCatalog};
use crate::models::{Exchange, Instrument, MarketEvent};
use crate::optimizer::{
    KellyOptimizer, KellyConfig, Opportunity, OpportunityScanner,
    OptimizedPortfolio, ScannerConfig,
};
use crate::pricing::{
    OptionType, PriceDistribution, VolatilitySurface, VolTimeStrategy,
    CalendarVolTimeStrategy, WeightedVolTimeStrategy,
    vol_surface::DeribitTickerInput,
};
use crate::traits::{Dashboard, DashboardSchema, SharedExecutionRouter, Strategy, Widget, TableColumn};
use async_trait::async_trait;
use chrono::TimeZone;
use chrono_tz::America::New_York;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Maximum log entries to keep.
const MAX_LOG_ENTRIES: usize = 200;

/// Discovered Polymarket market info.
#[derive(Debug, Clone)]
struct PolymarketDiscovery {
    condition_id: String,
    question: String,
    strike: f64,
    expiry_ms: i64,
    yes_token: String,
    no_token: String,
    minimum_order_size: Option<f64>,
    minimum_tick_size: Option<f64>,
}

/// Stats for debugging opportunity scanning.
#[derive(Default)]
struct ScanStats {
    polymarket_scanned: usize,
    derive_scanned: usize,
    invalid_prices: usize,
    no_model_prob: usize,
    below_edge_threshold: usize,
    opportunities_found: usize,
}

/// Configuration for the cross-market strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossMarketConfig {
    /// Underlying currency (e.g., "BTC")
    pub currency: String,
    /// Minimum edge threshold for opportunities
    pub min_edge: f64,
    /// Maximum time to expiry in years
    pub max_time_to_expiry: f64,
    /// Maximum expiry days for instrument discovery
    pub max_expiry_days: u32,
    /// Risk-free rate
    pub rate: f64,
    /// Kelly optimizer configuration
    pub kelly_config: KellyConfig,
    /// Scanner configuration
    pub scanner_config: ScannerConfig,
    /// Regex pattern for matching Polymarket BTC price markets
    pub polymarket_pattern: String,
    /// How often to recalculate (seconds)
    pub recalc_interval_secs: u64,
    /// Vol time strategy type: "calendar" or "weighted"
    #[serde(default = "default_vol_time_strategy")]
    pub vol_time_strategy: String,
    /// Historical hourly volatilities for weighted strategy (168 values = 7 days × 24 hours)
    /// Only used if vol_time_strategy is "weighted"
    #[serde(default)]
    pub hourly_vols: Vec<f64>,
    /// Regime scaler: recent volatility / long-term average (GARCH-lite)
    /// Only used if vol_time_strategy is "weighted"
    #[serde(default = "default_regime_scaler")]
    pub regime_scaler: f64,
}

fn default_regime_scaler() -> f64 {
    1.0
}

fn default_vol_time_strategy() -> String {
    "calendar".to_string()
}

impl Default for CrossMarketConfig {
    fn default() -> Self {
        Self {
            currency: "BTC".to_string(),
            min_edge: 0.02,
            max_time_to_expiry: 0.5, // 6 months
            max_expiry_days: 90,     // 90 days for discovery
            rate: 0.0,
            kelly_config: KellyConfig::default(),
            scanner_config: ScannerConfig::default(),
            // Pattern matches Polymarket slugs like "bitcoin-above-100000-on-december-31"
            polymarket_pattern: r"bitcoin-above-\d+".to_string(),
            recalc_interval_secs: 60,
            vol_time_strategy: "calendar".to_string(),
            hourly_vols: Vec::new(),
            regime_scaler: 1.0,
        }
    }
}

/// Internal state for the strategy.
struct CrossMarketState {
    /// Volatility surface (calibrated from Deribit)
    vol_surface: VolatilitySurface,
    /// Price distribution
    distribution: Option<PriceDistribution>,
    /// Volatility-weighted time strategy (required)
    vol_time_strategy: Box<dyn VolTimeStrategy>,
    /// Current opportunities
    opportunities: Vec<Opportunity>,
    /// Optimized portfolio
    portfolio: Option<OptimizedPortfolio>,
    /// Last Deribit ticker updates (instrument -> ticker data)
    deribit_tickers: HashMap<String, DeribitTickerSnapshot>,
    /// Last Derive ticker updates
    derive_tickers: HashMap<String, DeriveTicker>,
    /// Polymarket markets we're tracking
    polymarket_markets: HashMap<String, PolymarketMarket>,
    /// Reverse lookup: Token ID -> Condition ID (Market Key)
    token_to_market_key: HashMap<String, String>,
    /// Last recalculation timestamp
    last_recalc: i64,
    /// Activity log
    log: VecDeque<LogEntry>,
    /// Deribit instruments we're subscribed to
    deribit_subscriptions: HashSet<String>,
    /// Derive instruments we're subscribed to
    derive_subscriptions: HashSet<String>,
    /// Event counter for throttling logs
    event_counter: usize,
}



impl Default for CrossMarketState {
    fn default() -> Self {
        Self {
            vol_surface: VolatilitySurface::new(0.0),
            distribution: None,
            vol_time_strategy: Box::new(CalendarVolTimeStrategy),
            opportunities: Vec::new(),
            portfolio: None,
            deribit_tickers: HashMap::new(),
            derive_tickers: HashMap::new(),
            polymarket_markets: HashMap::new(),
            token_to_market_key: HashMap::new(),
            last_recalc: 0,
            log: VecDeque::with_capacity(MAX_LOG_ENTRIES),
            deribit_subscriptions: HashSet::new(),
            derive_subscriptions: HashSet::new(),
            event_counter: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeribitTickerSnapshot {
    instrument_name: String,
    timestamp: i64,
    underlying_price: Option<f64>,
    mark_iv: Option<f64>,
    bid_iv: Option<f64>,
    ask_iv: Option<f64>,
    strike: Option<f64>,
    expiry_timestamp: i64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeriveTicker {
    instrument_name: String,
    timestamp: i64,
    underlying_price: Option<f64>,
    mark_iv: Option<f64>,
    bid_iv: Option<f64>,
    ask_iv: Option<f64>,
    strike: f64,
    expiry_timestamp: i64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    best_bid_amount: Option<f64>,
    best_ask_amount: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolymarketMarket {
    condition_id: String,
    question: String,
    strike: f64,
    expiry_timestamp: i64,
    yes_token_id: String,
    no_token_id: String,
    yes_price: f64,
    no_price: f64,
    yes_liquidity: f64,
    no_liquidity: f64,
    last_updated: i64,
    /// Minimum shares for limit orders (Polymarket constraint)
    minimum_order_size: Option<f64>,
    /// Minimum price increment (Polymarket constraint)
    minimum_tick_size: Option<f64>,
}

#[derive(Clone, Serialize)]
struct LogEntry {
    time: String,
    level: String,
    message: String,
}

/// Cross-market mispricing strategy.
/// 
/// Uses Deribit IV data to calibrate a volatility surface, then scans
/// Polymarket and Derive for mispriced options.
pub struct CrossMarketStrategy {
    name: String,
    config: CrossMarketConfig,
    state: RwLock<CrossMarketState>,
    #[allow(dead_code)]
    exec: SharedExecutionRouter,
    scanner: OpportunityScanner,

    /// Polymarket catalog for market discovery (refreshed by Engine)
    polymarket_catalog: Option<Arc<PolymarketCatalog>>,
    /// Deribit catalog for instrument discovery (refreshed by Engine)
    deribit_catalog: Option<Arc<DeribitCatalog>>,
    /// Derive catalog for instrument discovery (refreshed by Engine)
    derive_catalog: Option<Arc<DeriveCatalog>>,
}

impl CrossMarketStrategy {
    /// Creates a new cross-market strategy.
    pub fn new(
        name: impl Into<String>,
        config: CrossMarketConfig,
        exec: SharedExecutionRouter,
    ) -> Arc<Self> {
        let scanner = OpportunityScanner::new(config.scanner_config.clone());

        Arc::new(Self {
            name: name.into(),
            config: config.clone(), // Clone config to use it
            state: RwLock::new(CrossMarketState::default()),
            exec,
            scanner,
            polymarket_catalog: None,
            deribit_catalog: None,
            derive_catalog: None,
        })
    }

    /// Creates a new cross-market strategy with catalog references.
    /// 
    /// Catalogs are used for live market discovery in `discover_subscriptions()`.
    /// The Engine refreshes catalogs before calling discover_subscriptions,
    /// ensuring strategies see newly listed markets.
    pub fn with_catalogs(
        name: impl Into<String>,
        config: CrossMarketConfig,
        exec: SharedExecutionRouter,
        polymarket_catalog: Option<Arc<PolymarketCatalog>>,
        deribit_catalog: Option<Arc<DeribitCatalog>>,
        derive_catalog: Option<Arc<DeriveCatalog>>,
    ) -> Arc<Self> {
        let scanner = OpportunityScanner::new(config.scanner_config.clone());

        Arc::new(Self {
            name: name.into(),
            config: config.clone(),
            state: RwLock::new(CrossMarketState::default()),
            exec,
            scanner,
            polymarket_catalog,
            deribit_catalog,
            derive_catalog,
        })
    }

    /// Creates with default configuration.
    pub fn with_defaults(
        name: impl Into<String>,
        exec: SharedExecutionRouter,
    ) -> Arc<Self> {
        Self::new(name, CrossMarketConfig::default(), exec)
    }

    /// Initializes subscriptions by fetching instruments from exchange APIs.
    /// Call this after creating the strategy to populate the subscription list.
    pub async fn initialize_subscriptions(&self, max_expiry_days: u32) {
        let currency = &self.config.currency;
        let polymarket_pattern = &self.config.polymarket_pattern;
        
        // Fetch Deribit options
        match Self::fetch_deribit_options(currency, max_expiry_days).await {
            Ok(instruments) => {
                let mut state = self.state.write().await;
                for inst in instruments {
                    state.deribit_subscriptions.insert(inst);
                }
                let count = state.deribit_subscriptions.len();
                drop(state);
                self.add_log("info", format!("Initialized {} Deribit subscriptions", count)).await;
            }
            Err(e) => {
                self.add_log("error", format!("Failed to fetch Deribit instruments: {}", e)).await;
            }
        }

        // Fetch Derive options
        match Self::fetch_derive_options(currency, max_expiry_days).await {
            Ok(instruments) => {
                let mut state = self.state.write().await;
                for inst in instruments {
                    state.derive_subscriptions.insert(inst);
                }
                let count = state.derive_subscriptions.len();
                drop(state);
                self.add_log("info", format!("Initialized {} Derive subscriptions", count)).await;
            }
            Err(e) => {
                self.add_log("error", format!("Failed to fetch Derive instruments: {}", e)).await;
            }
        }

        // Fetch Polymarket BTC price markets
        match Self::fetch_polymarket_markets(polymarket_pattern).await {
            Ok(markets) => {
                let count = markets.len();
                for market in markets {
                    self.register_polymarket_market(
                        &market.condition_id,
                        &market.question,
                        market.strike,
                        market.expiry_ms,
                        &market.yes_token,
                        &market.no_token,
                        market.minimum_order_size,
                        market.minimum_tick_size,
                    ).await;
                }
                self.add_log("info", format!("Initialized {} Polymarket markets", count)).await;
            }
            Err(e) => {
                self.add_log("error", format!("Failed to fetch Polymarket markets: {}", e)).await;
            }
        }
    }

    /// Fetches Deribit BTC options from the public API.
    async fn fetch_deribit_options(currency: &str, max_expiry_days: u32) -> Result<Vec<String>, String> {
        let url = format!(
            "https://www.deribit.com/api/v2/public/get_instruments?currency={}&kind=option&expired=false",
            currency
        );

        let client = reqwest::Client::new();
        let response = client.get(&url)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("API returned status: {}", response.status()));
        }

        #[derive(serde::Deserialize)]
        struct DeribitResponse {
            result: Vec<DeribitInstrument>,
        }

        #[derive(serde::Deserialize)]
        struct DeribitInstrument {
            instrument_name: String,
            expiration_timestamp: i64,
        }

        let data: DeribitResponse = response.json().await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        let now = chrono::Utc::now();
        let max_expiry_ms = now.timestamp_millis() + (max_expiry_days as i64 * 24 * 3600 * 1000);

        let instruments: Vec<String> = data.result
            .into_iter()
            .filter(|i| i.expiration_timestamp <= max_expiry_ms)
            .map(|i| i.instrument_name)
            .collect();

        info!("Deribit: Found {} {} options within {} days", instruments.len(), currency, max_expiry_days);
        Ok(instruments)
    }

    /// Fetches Derive options from the public API.
    async fn fetch_derive_options(currency: &str, max_expiry_days: u32) -> Result<Vec<String>, String> {
        let url = format!(
            "https://api.lyra.finance/public/get_instruments?currency={}&instrument_type=option&expired=false",
            currency
        );

        let client = reqwest::Client::new();
        let response = client.get(&url)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("API returned status: {}", response.status()));
        }

        #[derive(serde::Deserialize)]
        struct DeriveResponse {
            result: Option<Vec<DeriveInstrument>>,
        }

        #[derive(serde::Deserialize)]
        struct DeriveInstrument {
            instrument_name: String,
        }

        let data: DeriveResponse = response.json().await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        let instruments: Vec<String> = data.result
            .unwrap_or_default()
            .into_iter()
            .filter_map(|i| {
                // Filter by expiry (parse from instrument name: BTC-YYYYMMDD-STRIKE-TYPE)
                let parts: Vec<&str> = i.instrument_name.split('-').collect();
                if parts.len() < 4 {
                    return None;
                }
                if let Ok(expiry) = chrono::NaiveDate::parse_from_str(parts[1], "%Y%m%d") {
                    let now = chrono::Utc::now().date_naive();
                    let days_until = (expiry - now).num_days();
                    if days_until >= 0 && days_until <= max_expiry_days as i64 {
                        return Some(i.instrument_name);
                    }
                }
                None
            })
            .collect();

        info!("Derive: Found {} {} options within {} days", instruments.len(), currency, max_expiry_days);
        Ok(instruments)
    }

    /// Fetches Polymarket markets matching a pattern (e.g., "bitcoin-above").
    async fn fetch_polymarket_markets(pattern: &str) -> Result<Vec<PolymarketDiscovery>, String> {
        // Create catalog (loads from cache or fetches)
        let catalog = PolymarketCatalog::new(None, None).await;
        
        // Search for markets matching the pattern
        let markets = catalog.find_by_slug_regex(pattern)
            .map_err(|e| format!("Regex search failed: {}", e))?;
        
        info!("Polymarket: Regex '{}' matched {} markets", pattern, markets.len());
        
        let now_ms = chrono::Utc::now().timestamp_millis();
        
        // Collect markets into a vector so we can reference them later
        let markets_vec: Vec<_> = markets;
        
        // Build map for end_date_iso lookup (for debugging)
        let mut end_date_map: HashMap<String, String> = HashMap::new();
        for m in &markets_vec {
            if let Some(end_date) = m.extra.get("end_date_iso").and_then(|v| v.as_str()) {
                end_date_map.insert(m.id.clone(), end_date.to_string());
            }
        }
        
        let discoveries: Vec<PolymarketDiscovery> = markets_vec
            .into_iter()
            .filter_map(|m| {
                // Must have "bitcoin" in slug (case insensitive check)
                let slug = m.slug.as_ref()?;
                if !slug.to_lowercase().contains("bitcoin") {
                    return None;
                }
                
                // Parse strike price - must be a reasonable BTC price ($10k - $1M)
                let strike = Self::parse_strike_from_market(&m)?;
                if strike < 10_000.0 || strike > 1_000_000.0 {
                    return None;
                }
                
                // Parse expiry - must be in the future
                let expiry_ms = Self::parse_expiry_from_market(&m)?;
                if expiry_ms <= now_ms {
                    return None; // Already expired
                }
                
                // Check if market is active (not closed)
                if let Some(closed) = m.extra.get("closed").and_then(|v| v.as_bool()) {
                    if closed {
                        return None;
                    }
                }
                
                // Get YES/NO tokens
                let (yes_token, no_token) = Self::parse_tokens_from_market(&m)?;
                
                // Extract constraints before consuming the market
                let minimum_order_size = m.minimum_order_size();
                let minimum_tick_size = m.minimum_tick_size();
                
                Some(PolymarketDiscovery {
                    condition_id: m.id.clone(),
                    question: m.question.unwrap_or_default(),
                    strike,
                    expiry_ms,
                    yes_token,
                    no_token,
                    minimum_order_size,
                    minimum_tick_size,
                })
            })
            .collect();
        
        info!("Polymarket: Found {} active BTC price markets:", discoveries.len());
        for d in &discoveries {
            let expiry_str = chrono::DateTime::from_timestamp_millis(d.expiry_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "?".to_string());
            
            debug!("  - {} @ ${:.0} (expires {})", 
                d.question.chars().take(60).collect::<String>(),
                d.strike,
                expiry_str
            );
            
            if let Some(end_date) = end_date_map.get(&d.condition_id) {
                debug!("    end_date_iso: {}", end_date);
            }
        }
        Ok(discoveries)
    }

    /// Parses strike price from market slug/question.
    fn parse_strike_from_market(market: &crate::catalog::MarketInfo) -> Option<f64> {
        use regex::Regex;
        
        // Try to extract price from slug (e.g., "bitcoin-above-100000")
        let slug = market.slug.as_ref()?;
        
        // Pattern: number that looks like a price (5+ digits or with k suffix)
        let re = Regex::new(r"(\d{5,})|(\d+)k").ok()?;
        
        if let Some(caps) = re.captures(slug) {
            if let Some(m) = caps.get(1) {
                // Direct number like 100000
                return m.as_str().parse().ok();
            } else if let Some(m) = caps.get(2) {
                // Number with k suffix like 100k
                let base: f64 = m.as_str().parse().ok()?;
                return Some(base * 1000.0);
            }
        }
        
        // Try question text if slug didn't work
        if let Some(question) = &market.question {
            let re = Regex::new(r"\$?([\d,]+)k?").ok()?;
            for caps in re.captures_iter(question) {
                if let Some(m) = caps.get(1) {
                    let num_str = m.as_str().replace(',', "");
                    if let Ok(val) = num_str.parse::<f64>() {
                        if val >= 10000.0 {
                            return Some(val);
                        }
                    }
                }
            }
        }
        
        None
    }

    /// Parses expiry timestamp from market extra fields.
    /// 
    /// For Polymarket BTC markets, checks the description for "12:00 in the ET timezone"
    /// and uses that time instead of the midnight UTC in end_date_iso.
    /// ET (Eastern Time) is UTC-5 (EST) or UTC-4 (EDT) depending on daylight saving time.
    fn parse_expiry_from_market(market: &crate::catalog::MarketInfo) -> Option<i64> {
        // Look for end_date_iso in extra
        let end_date = market.extra.get("end_date_iso")?.as_str()?;
        
        // Parse ISO 8601 date to get the date
        let dt_utc = chrono::DateTime::parse_from_rfc3339(end_date).ok()?;
        let date = dt_utc.date_naive();
        
        // Check description for "12:00 in the ET timezone" or similar patterns
        // TODO: This is very hacky. Fix it/
        let use_et_noon = market.description.as_ref()
            .map(|desc| {
                desc.contains("12:00") && 
                (desc.contains("ET timezone") || desc.contains("ET") || desc.contains("Eastern"))
            })
            .unwrap_or(false);
        
        if use_et_noon {
            // Markets resolve at 12:00 ET (noon Eastern Time)
            // Use chrono-tz for accurate DST handling (America/New_York timezone)
            // This automatically handles EST (UTC-5) vs EDT (UTC-4) transitions
            let et_noon = date.and_hms_opt(12, 0, 0)?;
            
            // Convert 12:00 ET to UTC using proper timezone with DST support
            let et_datetime = New_York.from_local_datetime(&et_noon).single()?;
            let utc_datetime = et_datetime.with_timezone(&chrono::Utc);
            
            Some(utc_datetime.timestamp_millis())
        } else {
            // Fall back to the time specified in end_date_iso
            Some(dt_utc.timestamp_millis())
        }
    }

    /// Parses YES/NO tokens from market.
    fn parse_tokens_from_market(market: &crate::catalog::MarketInfo) -> Option<(String, String)> {
        if market.tokens.len() < 2 {
            return None;
        }
        
        let mut yes_token = None;
        let mut no_token = None;
        
        for token in &market.tokens {
            match token.outcome.as_deref() {
                Some("Yes") | Some("YES") | Some("yes") => {
                    yes_token = Some(token.token_id.clone());
                }
                Some("No") | Some("NO") | Some("no") => {
                    no_token = Some(token.token_id.clone());
                }
                _ => {}
            }
        }
        
        // If no explicit outcomes, assume first is YES, second is NO
        let yes = yes_token.or_else(|| market.tokens.get(0).map(|t| t.token_id.clone()))?;
        let no = no_token.or_else(|| market.tokens.get(1).map(|t| t.token_id.clone()))?;
        
        Some((yes, no))
    }

    /// Adds a log entry.
    async fn add_log(&self, level: &str, message: String) {
        let mut state = self.state.write().await;
        let entry = LogEntry {
            time: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            level: level.to_string(),
            message,
        };
        state.log.push_back(entry);
        if state.log.len() > MAX_LOG_ENTRIES {
            state.log.pop_front();
        }
    }

    /// Updates the volatility surface from Deribit ticker data.
    async fn update_vol_surface(&self, now_ms: i64) {
        let mut state = self.state.write().await;

        // Collect ticker inputs
        let inputs: Vec<DeribitTickerInput> = state.deribit_tickers.values()
            .filter_map(|t| {
                Some(DeribitTickerInput {
                    instrument_name: t.instrument_name.clone(),
                    strike: t.strike,
                    expiry_timestamp: t.expiry_timestamp,
                    underlying_price: t.underlying_price,
                    mark_iv: t.mark_iv,
                    bid_iv: t.bid_iv,
                    ask_iv: t.ask_iv,
                })
            })
            .collect();

        if inputs.is_empty() {
            return;
        }

        // Build surface
        state.vol_surface = VolatilitySurface::from_deribit_tickers(&inputs, now_ms);

        // Build distribution
        state.distribution = Some(PriceDistribution::from_vol_surface(
            &state.vol_surface,
            now_ms,
            self.config.rate,
        ));

        // Initialize vol time strategy based on config
        state.vol_time_strategy = match self.config.vol_time_strategy.as_str() {
            "weighted" => {
                if self.config.hourly_vols.is_empty() {
                    warn!("VOL TIME: 'weighted' strategy specified but hourly_vols is empty, falling back to calendar");
                    Box::new(CalendarVolTimeStrategy)
                } else {
                    debug!("VOL TIME: Initialized weighted strategy with {} hourly weights, regime_scaler={:.2}",
                        self.config.hourly_vols.len(),
                        self.config.regime_scaler
                    );
                    Box::new(WeightedVolTimeStrategy::new(
                        self.config.hourly_vols.clone(),
                        self.config.regime_scaler,
                        vec![], // Event overrides can be added later if needed
                    ))
                }
            }
            "calendar" | _ => {
                debug!("VOL TIME: Using calendar strategy");
                Box::new(CalendarVolTimeStrategy)
            }
        };

        // Log vol surface update with ATM IV for debugging
        let atm_ivs: Vec<(i64, f64)> = state.vol_surface.expiries()
            .iter()
            .filter_map(|&exp| {
                state.vol_surface.get_smile(exp)
                    .and_then(|s| s.atm_iv().map(|iv| (exp, iv)))
            })
            .collect();
        
        if !atm_ivs.is_empty() {
            let msg = format!("Vol surface updated: {} expiries, spot=${:.0}", atm_ivs.len(), state.vol_surface.spot());
            // We can't use self.add_log here because we have a mutable borrow of state
            // But we can add it to the state.log directly
            let time_str = chrono::DateTime::from_timestamp_millis(now_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "??:??:??".to_string());
                
            let entry = LogEntry {
                time: time_str,
                level: "info".to_string(),
                message: msg,
            };
            state.log.push_back(entry);
            if state.log.len() > MAX_LOG_ENTRIES {
                state.log.pop_front();
            }
        }
        
        debug!(
            "VOL SURFACE: spot=${:.0}, {} expiries, {} iv_points",
            state.vol_surface.spot(),
            state.vol_surface.num_expiries(),
            state.vol_surface.total_points()
        );
        
        // Print ATM IVs for each expiry to verify they're reasonable (should be ~0.5-0.8)
        for (exp, iv) in &atm_ivs {
            let date = chrono::DateTime::from_timestamp_millis(*exp)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "?".to_string());
            debug!("  Expiry {}: ATM IV = {:.4} ({:.1}%)", date, iv, iv * 100.0);
        }
        
        // Sample a few strikes to verify the distribution
        if let Some(ref dist) = state.distribution {
            let spot = state.vol_surface.spot();
            let expiries = dist.expiries();
            
            debug!("DISTRIBUTION: {} expiries available", expiries.len());
            
            if spot > 0.0 && !expiries.is_empty() {
                // Use the first actual expiry
                let sample_expiry = expiries[0];
                let expiry_date = chrono::DateTime::from_timestamp_millis(sample_expiry)
                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "?".to_string());
                
                // Sample probabilities at various strikes relative to spot
                let strikes = vec![
                    spot * 0.9,  // 10% below
                    spot * 0.95, // 5% below
                    spot,        // ATM
                    spot * 1.05, // 5% above
                    spot * 1.1,  // 10% above
                ];
                
                // Show price range of the distribution grid
                if let Some(exp_dist) = dist.get(sample_expiry) {
                    let (min_price, max_price) = exp_dist.price_range();
                    debug!("DISTRIBUTION (expiry {}): grid ${:.0} - ${:.0}, ATM IV={:.4}", 
                        expiry_date, min_price, max_price, exp_dist.atm_iv);
                } else {
                    debug!("DISTRIBUTION SAMPLE (expiry {}):", expiry_date);
                }
                
                for strike in strikes {
                    let prob = dist.probability_above(strike, sample_expiry);
                    debug!("  P(S > ${:.0}) = {:?}", strike, prob);
                }
            }
        }
    }

    /// Scans for opportunities across all markets.
    async fn scan_opportunities(&self, now_ms: i64) {
        let start = std::time::Instant::now();
        let mut state = self.state.write().await;

        // Diagnostic: Log vol surface state
        let spot = state.vol_surface.spot();
        let num_expiries = state.vol_surface.num_expiries();
        let total_points = state.vol_surface.total_points();
        
        if total_points == 0 {
            debug!("SCAN: No IV data yet, skipping opportunity scan");
            return;
        }
        
        debug!("SCAN: Vol surface - spot=${:.0}, expiries={}, iv_points={}", 
            spot, num_expiries, total_points);

        let distribution = match &state.distribution {
            Some(d) => d,
            None => {
                debug!("SCAN: No price distribution built yet");
                return;
            }
        };

        let mut opportunities = Vec::new();
        let mut scan_stats = ScanStats::default();

        // Get the vol-time strategy reference for passing to scanner functions
        let vol_time_strategy: Option<&dyn VolTimeStrategy> = Some(state.vol_time_strategy.as_ref());

        // Scan Polymarket binary options
        for market in state.polymarket_markets.values() {
            scan_stats.polymarket_scanned += 1;
            
            // Get model probability for diagnostics (uses vol-weighted interpolation)
            let model_prob = distribution.probability_above_with_strategy(
                market.strike,
                market.expiry_timestamp,
                vol_time_strategy,
            );
            
            // Calculate calendar time for logging
            let time_to_expiry = (market.expiry_timestamp - now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
            
            // Log every market for debugging
            debug!("SCAN PM: {} strike=${:.0} expiry={:.3}y | YES={:.2} NO={:.2} | model_prob={:?}",
                market.question.chars().take(40).collect::<String>(),
                market.strike,
                time_to_expiry,
                market.yes_price,
                market.no_price,
                model_prob
            );
            
            // Check for potential issues
            if market.yes_price <= 0.0 || market.yes_price >= 1.0 {
                scan_stats.invalid_prices += 1;
                if scan_stats.invalid_prices <= 5 {
                    debug!("  -> SKIP: Invalid YES price {} (must be 0.0 < p < 1.0)", market.yes_price);
                }
                continue;
            }
            if model_prob.is_none() {
                scan_stats.no_model_prob += 1;
                if scan_stats.no_model_prob <= 5 {
                    debug!("  -> SKIP: No model probability for strike ${:.0}", market.strike);
                }
                continue;
            }
            
            let prob = model_prob.unwrap();
            let yes_edge = (prob - market.yes_price) / market.yes_price;
            let no_edge = ((1.0 - prob) - market.no_price) / market.no_price;
            
            debug!("  -> model_prob={:.2} | yes_edge={:.1}% no_edge={:.1}%",
                prob, yes_edge * 100.0, no_edge * 100.0);
            
            let opps = self.scanner.scan_binary_option(
                &market.condition_id,
                &market.question,
                market.strike,
                market.expiry_timestamp,
                market.yes_price,
                market.no_price,
                market.yes_liquidity,
                market.no_liquidity,
                distribution,
                now_ms,
                Some(&market.yes_token_id),
                Some(&market.no_token_id),
                vol_time_strategy, // Pass vol-time strategy for interpolation
                market.minimum_order_size,
                market.minimum_tick_size,
            );
            
            if !opps.is_empty() {
                scan_stats.opportunities_found += opps.len();
                debug!("  -> FOUND {} opportunities!", opps.len());
            }
            
            opportunities.extend(opps);
        }

        // Scan Derive vanilla options
        for ticker in state.derive_tickers.values() {
            scan_stats.derive_scanned += 1;
            
            let option_type = if ticker.instrument_name.ends_with("-C") {
                OptionType::Call
            } else if ticker.instrument_name.ends_with("-P") {
                OptionType::Put
            } else {
                continue;
            };

            let bid = ticker.best_bid.unwrap_or(0.0);
            let ask = ticker.best_ask.unwrap_or(0.0);
            let liquidity = ticker.best_bid_amount.unwrap_or(0.0)
                .min(ticker.best_ask_amount.unwrap_or(0.0));

            let opps = self.scanner.scan_vanilla_option(
                &ticker.instrument_name,
                option_type,
                ticker.strike,
                ticker.expiry_timestamp,
                bid,
                ask,
                liquidity,
                &state.vol_surface,
                now_ms,
                vol_time_strategy, // Pass vol-time strategy for interpolation
            );
            
            if !opps.is_empty() {
                scan_stats.opportunities_found += opps.len();
            }
            
            opportunities.extend(opps);
        }

        // Log scan summary with timing
        let duration = start.elapsed();
        info!("SCAN SUMMARY (took {:?}): PM={} Derive={} | invalid_price={} no_model={} | opportunities={}",
            duration,
            scan_stats.polymarket_scanned,
            scan_stats.derive_scanned,
            scan_stats.invalid_prices,
            scan_stats.no_model_prob,
            scan_stats.opportunities_found
        );
        
        // Log the actual opportunities found
        for opp in &opportunities {
            info!("  OPPORTUNITY: {} | edge={:.1}% | fair={:.3} vs market={:.3} | {}",
                opp.exchange,
                opp.edge * 100.0,
                opp.fair_value,
                opp.market_price,
                opp.description
            );
        }
        
        state.opportunities = opportunities;
    }

    // Helper to get a fallback price for an opportunity ID when the opportunity 
    // is missing from the current scan. Returns the positive market price.
    // Handles expired positions by calculating intrinsic value (settlement).
    fn get_fallback_price(&self, opp_id: &str, state: &CrossMarketState, now_ms: i64) -> f64 {
        // Helper to calculate settlement value for Derive options
        let get_derive_value = |inst_id: &str, is_buy_back: bool| -> f64 {
            if let Some(ticker) = state.derive_tickers.get(inst_id) {
                // Check if expired (and has valid expiry)
                if ticker.expiry_timestamp > 0 && now_ms >= ticker.expiry_timestamp {
                    // Settlement Logic: Intrinsic Value based on Spot
                    let spot = state.vol_surface.spot();
                    if spot <= 0.0 { return 0.0; } // No spot ref
                    
                    let intrinsic = if ticker.instrument_name.ends_with("-C") {
                        (spot - ticker.strike).max(0.0)
                    } else if ticker.instrument_name.ends_with("-P") {
                        (ticker.strike - spot).max(0.0)
                    } else {
                        0.0
                    };
                    debug!("FALLBACK: {} expired (exp={}, now={}). Spot={:.2}. Settlement={:.4}", 
                        inst_id, ticker.expiry_timestamp, now_ms, spot, intrinsic);
                    return intrinsic;
                }
                
                // Active: Use market price
                if is_buy_back {
                   return ticker.best_ask.unwrap_or(0.0);
                } else {
                   return ticker.best_bid.unwrap_or(0.0);
                }
            }
            0.0
        };

        if opp_id.contains("_yes") { 
            if let Some(condition_id) = opp_id.strip_suffix("_yes") {
                if let Some(market) = state.polymarket_markets.get(condition_id) {
                    return market.yes_price;
                }
            }
        } else if opp_id.contains("_no") {
             if let Some(condition_id) = opp_id.strip_suffix("_no") {
                if let Some(market) = state.polymarket_markets.get(condition_id) {
                    return market.no_price;
                }
            }
        } else if opp_id.ends_with("_buy") {
            if let Some(inst_id) = opp_id.strip_suffix("_buy") {
                // Liquidation of Long -> Bid
                return get_derive_value(inst_id, false);
            }
        } else if opp_id.ends_with("_sell") {
            if let Some(inst_id) = opp_id.strip_suffix("_sell") {
                // Closing of Short -> Ask
                return get_derive_value(inst_id, true);
            }
        } else if opp_id.ends_with("_spread") {
            if let Some(legs_str) = opp_id.strip_suffix("_spread") {
                // expecting "ShortInstrument_LongInstrument"
                let parts: Vec<&str> = legs_str.split('_').collect();
                if parts.len() == 2 {
                    let short_inst = parts[0];
                    let long_inst = parts[1];
                    
                    let short_price = get_derive_value(short_inst, true); // Buy back short
                    let long_price = get_derive_value(long_inst, false); // Sell long
                        
                    // Cost to close = Buy Short - Sell Long
                    return short_price - long_price;
                }
            }
        }
        0.0
    }

    /// Calculates the total value of current holdings, correctly accounting for position direction.
    /// 
    /// Assets (Long positions) add to value.
    /// Liabilities (Short positions) subtract from value.
    fn calculate_holdings_value(
        &self, 
        current_positions: &HashMap<String, f64>, 
        opportunities: &[Opportunity], 
        now_ms: i64, 
        state: &CrossMarketState // Still need state for fallback pricing fallback (tickers)
    ) -> f64 {
        let opp_map: HashMap<&String, &Opportunity> = opportunities.iter()
            .map(|o| (&o.id, o))
            .collect();

        let mut holdings_value = 0.0;
        for (opp_id, qty) in current_positions {
            if *qty == 0.0 { continue; }
            
            let val = if let Some(opp) = opp_map.get(opp_id) {
                // Mark-to-Entry using scanner prices
                match opp.direction {
                    crate::optimizer::opportunity::TradeDirection::Buy => *qty * opp.market_price,
                    crate::optimizer::opportunity::TradeDirection::Sell => -*qty * opp.market_price,
                }
            } else {
                // Fallback pricing
                let price = self.get_fallback_price(opp_id, state, now_ms);
                
                // Direction inference
                let is_short_generated_id = opp_id.ends_with("_sell") || opp_id.ends_with("_spread");
                
                if is_short_generated_id {
                    -*qty * price
                } else {
                    *qty * price
                }
            };
            
            holdings_value += val;
        }
        holdings_value
    }

    /// Helper to convert ExecutionClient positions to Opportunity ID -> Quantity map.
    async fn get_opportunity_positions(&self) -> HashMap<String, f64> {
        let positions = match self.exec.get_positions().await {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to get positions: {}", e);
                return HashMap::new();
            }
        };

        let state = self.state.read().await;
        let mut opp_positions = HashMap::new();

        for pos in positions {
            if pos.quantity == 0.0 { continue; }

            match pos.instrument {
                Instrument::Deribit(name) | Instrument::Derive(name) => {
                    if pos.quantity > 0.0 {
                        opp_positions.insert(format!("{}_buy", name), pos.quantity);
                    } else if pos.quantity < 0.0 {
                        opp_positions.insert(format!("{}_sell", name), -pos.quantity);
                    }
                },
                Instrument::Polymarket(token) => {
                    if let Some(key) = state.token_to_market_key.get(&token) {
                        if let Some(m) = state.polymarket_markets.get(key) {
                            if m.yes_token_id == token {
                                opp_positions.insert(format!("{}_yes", key), pos.quantity);
                            } else if m.no_token_id == token {
                                opp_positions.insert(format!("{}_no", key), pos.quantity);
                            }
                        }
                    }
                }
            }
        }
        opp_positions
    }

    /// Optimizes the portfolio using Kelly criterion.


    /// Performs a full recalculation cycle.
    async fn recalculate(&self, now_ms: i64) {
        let start = std::time::Instant::now();
        // Log current Polymarket prices before recalc
        {
            let state = self.state.read().await;
            debug!("Polymarket markets with prices:");
            for market in state.polymarket_markets.values() {
                debug!("  - {} @ ${:.0}: YES={:.3} NO={:.3} (sum={:.3})",
                    market.question.chars().take(50).collect::<String>(),
                    market.strike,
                    market.yes_price,
                    market.no_price,
                    market.yes_price + market.no_price
                );
            }
        }
        
        self.update_vol_surface(now_ms).await;
        
        info!("Recalc: Scanning opportunities...");
        self.scan_opportunities(now_ms).await;
        
        // PnL Tracking & Portfolio Optimization
        
        // 0. Fetch current state (Positions & Balance)
        // We do this outside the lock to avoid holding it during async calls
        let current_positions_map = self.get_opportunity_positions().await;
        
        let mut balance = 0.0;
        for exchange in self.exec.exchanges() {
            if let Ok(b) = self.exec.get_balance(exchange).await {
                balance += b;
            } else {
                warn!("Failed to get balance for {:?}", exchange);
            }
        }
        
        // 1. Calculate current equity and create compounding optimizer
        let (_current_equity, optimizer, opportunities, distribution) = {
            let state = self.state.read().await;
            
            let holdings_value = self.calculate_holdings_value(&current_positions_map, &state.opportunities, now_ms, &state);
            
            let equity = balance + holdings_value;
            
            // Create new optimizer with updated wealth for compounding
            let mut kelly_config = self.config.kelly_config.clone();
            kelly_config.initial_wealth = equity; // Compound returns
            
            (
                equity, 
                KellyOptimizer::new(kelly_config),
                state.opportunities.clone(),
                state.distribution.clone()
            )
        };
        
        info!("Recalc: Scan complete ({} opps). Optimizing...", opportunities.len());
        
        // 2. Run optimization (without lock)
        let portfolio = if let Some(dist) = distribution {
            optimizer.optimize(&opportunities, &dist, now_ms)
        } else {
            let mut state = self.state.write().await;
            state.last_recalc = now_ms;
            return;
        };
    
        // 3. Rebalance Portfolio
        let _expected_utility = portfolio.expected_utility;
        let _expected_return = portfolio.expected_return;
        let _prob_loss = portfolio.prob_loss;
        let n_opps = opportunities.len();
        
        // Rebalance
        self.rebalance_portfolio(&current_positions_map, &portfolio.positions, &opportunities).await;
        
        // 4. Update Strategy State with Portfolio & Last Recalc
        {
            let mut state = self.state.write().await;
            state.portfolio = Some(portfolio);
            state.last_recalc = now_ms;
            
            // Note: We don't update history here anymore as the strategy doesn't own PnL history.
            // The backtest runner or a separate metrics tracker should handle that.
            
            let duration = start.elapsed();
            let msg = format!(
                "Recalc ({:?}): {} opps, Equity=${:.2}", 
                duration, n_opps, _current_equity
            );
            
            state.log.push_back(LogEntry {
                time: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                level: "info".to_string(),
                message: msg,
            });
            if state.log.len() > MAX_LOG_ENTRIES {
                state.log.pop_front();
            }
        }
    }

    /// Processes a Deribit market event.
    async fn handle_deribit_event(&self, event: &MarketEvent, instrument_name: &str) {
        // Ignore updates with no price data
        if event.best_bid.is_none() && event.best_ask.is_none() {
            return;
        }

        // Parse instrument for strike and expiry
        let parts: Vec<&str> = instrument_name.split('-').collect();
        if parts.len() < 4 {
            return; // Not an option
        }

        let expiry_str = parts[1];
        let strike: f64 = match parts[2].parse() {
            Ok(s) => s,
            Err(_) => return,
        };

        let expiry_timestamp = match crate::pricing::vol_surface::parse_deribit_expiry(expiry_str) {
            Some(ts) => ts,
            None => return,
        };

        // Create snapshot with IV data from the event
        let snapshot = DeribitTickerSnapshot {
            instrument_name: instrument_name.to_string(),
            timestamp: event.timestamp,
            underlying_price: event.underlying_price,
            mark_iv: event.mark_iv,
            bid_iv: event.bid_iv,
            ask_iv: event.ask_iv,
            strike: Some(strike),
            expiry_timestamp,
            best_bid: event.best_bid,
            best_ask: event.best_ask,
        };

        let mut state = self.state.write().await;
        state.deribit_tickers.insert(instrument_name.to_string(), snapshot);
    }

    /// Processes a Derive market event.
    async fn handle_derive_event(&self, event: &MarketEvent, instrument_name: &str) {
        // Parse instrument
        let parts: Vec<&str> = instrument_name.split('-').collect();
        if parts.len() < 4 {
            return;
        }

        let expiry_str = parts[1];
        let strike: f64 = match parts[2].parse() {
            Ok(s) => s,
            Err(_) => return,
        };

        // Parse YYYYMMDD expiry
        let expiry_timestamp = if expiry_str.len() == 8 {
            chrono::NaiveDate::parse_from_str(expiry_str, "%Y%m%d")
                .ok()
                .and_then(|d| d.and_hms_opt(8, 0, 0))
                .map(|dt| {
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc)
                        .timestamp_millis()
                })
                .unwrap_or(0)
        } else {
            0
        };

        let mut state = self.state.write().await;
        
        // If we already have this ticker and the new event has NO price data,
        // we should PRESERVE the old price data to avoid zeroing out liquidity during data gaps.
        // If it's a new ticker, we insert it anyway to register it.
        let preserve_prices = if let Some(old) = state.derive_tickers.get(instrument_name) {
             event.best_bid.is_none() && event.best_ask.is_none() && (old.best_bid.is_some() || old.best_ask.is_some())
        } else {
             false
        };

        if preserve_prices {
            return;
        }

        let ticker = DeriveTicker {
            instrument_name: instrument_name.to_string(),
            timestamp: event.timestamp,
            underlying_price: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            strike,
            expiry_timestamp,
            best_bid: event.best_bid,
            best_ask: event.best_ask,
            best_bid_amount: None,
            best_ask_amount: None,
        };

        state.derive_tickers.insert(instrument_name.to_string(), ticker);
    }

    /// Registers a Polymarket market to track.
    pub async fn register_polymarket_market(
        &self,
        condition_id: &str,
        question: &str,
        strike: f64,
        expiry_timestamp: i64,
        yes_token_id: &str,
        no_token_id: &str,
        minimum_order_size: Option<f64>,
        minimum_tick_size: Option<f64>,
    ) {
        let mut state = self.state.write().await;
        state.polymarket_markets.insert(
            condition_id.to_string(),
            PolymarketMarket {
                condition_id: condition_id.to_string(),
                question: question.to_string(),
                strike,
                expiry_timestamp,
                yes_token_id: yes_token_id.to_string(),
                no_token_id: no_token_id.to_string(),
                yes_price: 0.0,
                no_price: 0.0,
                yes_liquidity: 0.0,
                no_liquidity: 0.0,
                last_updated: 0,
                minimum_order_size,
                minimum_tick_size,
            },
        );
        state.token_to_market_key.insert(yes_token_id.to_string(), condition_id.to_string());
        state.token_to_market_key.insert(no_token_id.to_string(), condition_id.to_string());
    }

    /// Rebalances the portfolio based on optimizer target positions.
    async fn rebalance_portfolio(
        &self,
        current_positions: &HashMap<String, f64>,
        target_positions: &[crate::optimizer::kelly::PositionAllocation],
        opportunities: &[Opportunity],
    ) {
        // Build map of ID -> Opportunity
        let opp_map: HashMap<&String, &Opportunity> = opportunities.iter()
            .map(|o| (&o.id, o))
            .collect();

        // 1. Execute target positions
        for pos in target_positions {
            let old_qty = current_positions.get(&pos.opportunity_id).copied().unwrap_or(0.0);
            let needed = pos.size - old_qty;
            
            if needed.abs() > 1e-6 {
                if let Some(mut instrument) = self.get_instrument_for_opp(&pos.opportunity_id, &opp_map).await {
                     // Determine order side
                     // If opp is BUY/LONG and needed > 0 -> BUY
                     // If opp is BUY/LONG and needed < 0 -> SELL
                     // If opp is SELL/SHORT and needed > 0 -> SELL (Increase Short)
                     // If opp is SELL/SHORT and needed < 0 -> BUY (Decrease Short)
                     
                     let opp = opp_map.get(&pos.opportunity_id);
                     let is_sell_opp = match opp {
                         Some(o) => o.direction == crate::optimizer::opportunity::TradeDirection::Sell,
                         None => pos.opportunity_id.ends_with("_sell") || pos.opportunity_id.ends_with("_spread"),
                     };

                     let (side, qty_abs) = if is_sell_opp {
                         // Being Short.
                         if needed > 0.0 {
                             (crate::models::OrderSide::Sell, needed.abs())
                         } else {
                             (crate::models::OrderSide::Buy, needed.abs())
                         }
                     } else {
                         // Being Long.
                         if needed > 0.0 {
                             (crate::models::OrderSide::Buy, needed.abs())
                         } else {
                             (crate::models::OrderSide::Sell, needed.abs())
                         }
                     };
                     
                     let order = crate::models::Order::market(instrument, side, qty_abs);
                     if let Err(e) = self.exec.place_order(order).await {
                         error!("Failed to place order for {}: {}", pos.opportunity_id, e);
                     }
                } else {
                    warn!("Could not determine instrument for opportunity: {}", pos.opportunity_id);
                }
            }
        }
        
        // 2. Close positions not in new portfolio (if they exist in current)
        let new_pos_ids: HashSet<&String> = target_positions.iter().map(|p| &p.opportunity_id).collect();
        // To safely iterate and modify, we collect keys first
        let old_pos_ids: Vec<&String> = current_positions.keys().collect();
        
        for id in old_pos_ids {
            if !new_pos_ids.contains(id) {
                let old_qty = current_positions.get(id).copied().unwrap_or(0.0);
                if old_qty.abs() > 1e-6 {
                    // We need to close this position. needed = -old_qty (target 0)
                    // Logic similar to above
                    // We need Opportunity info to know instrument.
                    // But if opp is not in current scan, we might fail to map ID -> Instrument.
                    // We need fallback mapping logic based on ID string.
                    
                    if let Some(instrument) = self.resolve_instrument_from_id(id).await {
                        let is_sell_opp = id.ends_with("_sell") || id.ends_with("_spread"); // Fallback inference
                        
                        let needed = -old_qty;
                        let (side, qty_abs) = if is_sell_opp {
                             if needed > 0.0 { (crate::models::OrderSide::Sell, needed.abs()) }
                             else { (crate::models::OrderSide::Buy, needed.abs()) }
                        } else {
                             if needed > 0.0 { (crate::models::OrderSide::Buy, needed.abs()) }
                             else { (crate::models::OrderSide::Sell, needed.abs()) }
                        };
                        
                        let order = crate::models::Order::market(instrument, side, qty_abs);
                        if let Err(e) = self.exec.place_order(order).await {
                            error!("Failed to close position {}: {}", id, e);
                        }
                    } else {
                        warn!("Could not resolve instrument to close position: {}", id);
                    }
                }
            }
        }
    }
    
    // Helper to resolve instrument
    async fn get_instrument_for_opp(&self, opp_id: &str, opp_map: &HashMap<&String, &Opportunity>) -> Option<Instrument> {
        if let Some(opp) = opp_map.get(&opp_id.to_string()) {
            // Reconstruct instrument from opp info
            if let Some(token) = &opp.token_id {
                return Some(Instrument::Polymarket(token.clone()));
            } else if !opp.instrument_id.is_empty() {
                if opp.exchange == "deribit" {
                    return Some(Instrument::Deribit(opp.instrument_id.clone()));
                } else {
                    return Some(Instrument::Derive(opp.instrument_id.clone()));
                }
            }
        }
        // Fallback
        self.resolve_instrument_from_id(opp_id).await
    }
    
    async fn resolve_instrument_from_id(&self, opp_id: &str) -> Option<Instrument> {
        if opp_id.contains("_yes") { 
            if let Some(condition_id) = opp_id.strip_suffix("_yes") {
                let state = self.state.read().await;
                if let Some(market) = state.polymarket_markets.get(condition_id) {
                    return Some(Instrument::Polymarket(market.yes_token_id.clone()));
                }
            }
        } else if opp_id.contains("_no") {
             if let Some(condition_id) = opp_id.strip_suffix("_no") {
                 let state = self.state.read().await;
                if let Some(market) = state.polymarket_markets.get(condition_id) {
                    return Some(Instrument::Polymarket(market.no_token_id.clone()));
                }
            }
        } else {
            // Try stripping suffix
            let base = if let Some(stripped) = opp_id.strip_suffix("_buy") { stripped }
                       else if let Some(stripped) = opp_id.strip_suffix("_sell") { stripped }
                       else { return None };
                       
            // Assume Derive/Deribit based on name format? 
            // Deribit usually has -C/P? Derive too.
            // But we can check subscriptions or simple heuristics.
            // Or try both/either. Instrument::Deribit vs Derive.
            // Scanner puts instrument_name directly in suffix.
            // If it was scanned as Derive, it is Derive.
            // But here we don't know exchange effectively without opp.
            // Assume Derive for now as it's the primary target for vanilla options in this strat?
            // Actually config defaults to both.
            // We can check `state.deribit_tickers` or `derive_tickers` to see where it exists.
            let state = self.state.read().await;
            if state.deribit_tickers.contains_key(base) {
                return Some(Instrument::Deribit(base.to_string()));
            }
            if state.derive_tickers.contains_key(base) {
                return Some(Instrument::Derive(base.to_string()));
            }
            // Fallback: if not found, maybe default to Derive?
             return Some(Instrument::Derive(base.to_string()));
        }
        None
    }

    /// Updates Polymarket prices (called from external data source).
    pub async fn update_polymarket_prices(
        &self,
        condition_id: &str,
        yes_price: f64,
        no_price: f64,
        yes_liquidity: f64,
        no_liquidity: f64,
    ) {
        let mut state = self.state.write().await;
        if let Some(market) = state.polymarket_markets.get_mut(condition_id) {
            market.yes_price = yes_price;
            market.no_price = no_price;
            market.yes_liquidity = yes_liquidity;
            market.no_liquidity = no_liquidity;
            market.last_updated = chrono::Utc::now().timestamp_millis();
        }
    }
}

// =============================================================================
// Dashboard Implementation
// =============================================================================

#[async_trait]
impl Dashboard for CrossMarketStrategy {
    fn dashboard_name(&self) -> &str {
        &self.name
    }

    async fn dashboard_state(&self) -> Value {
        let state = self.state.read().await;

        let opportunities: Vec<Value> = state.opportunities.iter()
            .map(|o| {
                let expiry_str = chrono::DateTime::from_timestamp_millis(o.expiry_timestamp)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_else(|| "?".to_string());
                
                json!({
                    "id": o.id,
                    "type": format!("{:?}", o.opportunity_type),
                    "exchange": o.exchange,
                    "description": o.description,
                    "strike": format!("${:.0}", o.strike),
                    "expiry": expiry_str,
                    "edge": format!("{:.1}%", o.edge * 100.0),
                    "market_price": format!("{:.4}", o.market_price),
                    "fair_value": format!("{:.4}", o.fair_value),
                    "liquidity": o.liquidity,
                })
            })
            .collect();

        // Build a map of opportunity_id -> (strike, expiry) for position lookups
        let opp_map: HashMap<String, (f64, i64)> = state.opportunities.iter()
            .map(|o| (o.id.clone(), (o.strike, o.expiry_timestamp)))
            .collect();

        let positions: Vec<Value> = state.portfolio.as_ref()
            .map(|p| p.positions.iter()
                .filter_map(|pos| {
                    match opp_map.get(&pos.opportunity_id) {
                        Some((strike, expiry_ts)) => {
                            let expiry_str = chrono::DateTime::from_timestamp_millis(*expiry_ts)
                                .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                                .unwrap_or_else(|| "?".to_string());
                            
                            Some(json!({
                                "opportunity_id": pos.opportunity_id,
                                "strike": format!("${:.0}", strike),
                                "expiry": expiry_str,
                                "size": format!("{:.2}", pos.size),
                                "dollar_value": format!("${:.2}", pos.dollar_value),
                                "expected_profit": format!("${:.2}", pos.expected_profit),
                            }))
                        }
                        None => {
                            warn!("Position references missing opportunity_id: {}", pos.opportunity_id);
                            None
                        }
                    }
                })
                .collect())
            .unwrap_or_default();

        let portfolio_stats = state.portfolio.as_ref().map(|p| json!({
            "expected_return": format!("{:.2}%", p.expected_return * 100.0),
            "expected_sharpe": format!("{:.2}", p.expected_sharpe),
            "prob_loss": format!("{:.1}%", p.prob_loss * 100.0),
            "max_drawdown": format!("{:.1}%", p.max_drawdown * 100.0),
        }));

        // Build IV chart data from the volatility surface
        // Each point includes strike, IV, and expiry label for multi-series display
        let mut iv_chart_data: Vec<Value> = Vec::new();
        let expiries = state.vol_surface.expiries();
        
        for expiry_ts in &expiries {
            if let Some(smile) = state.vol_surface.get_smile(*expiry_ts) {
                let expiry_label = chrono::DateTime::from_timestamp_millis(*expiry_ts)
                    .map(|dt| dt.format("%b %d").to_string())
                    .unwrap_or_else(|| "?".to_string());
                
                for strike in smile.strikes() {
                    if let Some(iv) = smile.get_iv(strike) {
                        iv_chart_data.push(json!({
                            "strike": strike,
                            "iv": iv * 100.0, // Convert to percentage
                            "expiry": expiry_label,
                        }));
                    }
                }
            }
        }

        json!({
            "spot_price": state.vol_surface.spot(),
            "num_expiries": state.vol_surface.num_expiries(),
            "total_iv_points": state.vol_surface.total_points(),
            "deribit_tickers": state.deribit_tickers.len(),
            "derive_tickers": state.derive_tickers.len(),
            "polymarket_markets": state.polymarket_markets.len(),
            "opportunities": opportunities,
            "positions": positions,
            "portfolio_stats": portfolio_stats,
            "last_recalc": state.last_recalc,
            "log": state.log.iter().collect::<Vec<_>>(),
            "iv_chart": iv_chart_data,
        })
    }

    fn dashboard_schema(&self) -> DashboardSchema {
        DashboardSchema {
            widgets: vec![
                Widget::KeyValue {
                    label: "BTC Spot Price".to_string(),
                    key: "spot_price".to_string(),
                    format: Some("${:,.0}".to_string()),
                },
                Widget::KeyValue {
                    label: "Vol Surface Expiries".to_string(),
                    key: "num_expiries".to_string(),
                    format: None,
                },
                Widget::KeyValue {
                    label: "IV Data Points".to_string(),
                    key: "total_iv_points".to_string(),
                    format: None,
                },
                Widget::Divider,
                Widget::Chart {
                    title: "Implied Volatility Surface".to_string(),
                    data_key: "iv_chart".to_string(),
                    chart_type: "line".to_string(),
                },
                Widget::Divider,
                Widget::Table {
                    title: "Opportunities".to_string(),
                    columns: vec![
                        TableColumn { header: "ID".to_string(), key: "id".to_string(), format: None },
                        TableColumn { header: "Type".to_string(), key: "type".to_string(), format: None },
                        TableColumn { header: "Exchange".to_string(), key: "exchange".to_string(), format: None },
                        TableColumn { header: "Strike".to_string(), key: "strike".to_string(), format: None },
                        TableColumn { header: "Expiry".to_string(), key: "expiry".to_string(), format: None },
                        TableColumn { header: "Edge".to_string(), key: "edge".to_string(), format: None },
                        TableColumn { header: "Market".to_string(), key: "market_price".to_string(), format: None },
                        TableColumn { header: "Fair".to_string(), key: "fair_value".to_string(), format: None },
                    ],
                    data_key: "opportunities".to_string(),
                },
                Widget::Divider,
                Widget::Table {
                    title: "Optimized Positions".to_string(),
                    columns: vec![
                        TableColumn { header: "Opportunity".to_string(), key: "opportunity_id".to_string(), format: None },
                        TableColumn { header: "Strike".to_string(), key: "strike".to_string(), format: None },
                        TableColumn { header: "Expiry".to_string(), key: "expiry".to_string(), format: None },
                        TableColumn { header: "Size".to_string(), key: "size".to_string(), format: None },
                        TableColumn { header: "Value".to_string(), key: "dollar_value".to_string(), format: None },
                        TableColumn { header: "E[Profit]".to_string(), key: "expected_profit".to_string(), format: None },
                    ],
                    data_key: "positions".to_string(),
                },
                Widget::Divider,
                Widget::Log {
                    title: "Activity Log".to_string(),
                    data_key: "log".to_string(),
                    max_lines: 50,
                },
            ],
        }
    }
}

// =============================================================================
// Strategy Implementation
// =============================================================================

#[async_trait]
impl Strategy for CrossMarketStrategy {
    fn name(&self) -> &str {
        &self.name
    }

    fn required_exchanges(&self) -> HashSet<Exchange> {
        let mut exchanges = HashSet::new();
        exchanges.insert(Exchange::Deribit);
        exchanges.insert(Exchange::Derive);
        exchanges.insert(Exchange::Polymarket);
        exchanges
    }

    async fn discover_subscriptions(&self) -> Vec<Instrument> {
        use crate::catalog::Catalog;
        
        let mut instruments = Vec::new();
        let currency = &self.config.currency;
        let max_expiry_days = self.config.max_expiry_days;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let max_expiry_ms = now_ms + (max_expiry_days as i64 * 24 * 3600 * 1000);

        // Query Deribit catalog if available, otherwise fall back to cached state
        if let Some(catalog) = &self.deribit_catalog {
            for instrument in catalog.current().values() {
                // Filter by currency and expiry
                if instrument.base_currency == *currency 
                    && instrument.expiration_timestamp <= max_expiry_ms 
                {
                    instruments.push(Instrument::Deribit(instrument.instrument_name.clone()));
                }
            }
            debug!("discover_subscriptions: {} Deribit instruments from catalog", instruments.len());
        } else {
            // Fall back to cached state (legacy path)
            let state = self.state.read().await;
            for inst in &state.deribit_subscriptions {
                instruments.push(Instrument::Deribit(inst.clone()));
            }
        }

        let deribit_count = instruments.len();

        // Query Derive catalog if available
        if let Some(catalog) = &self.derive_catalog {
            let max_expiry_date = chrono::Utc::now() + chrono::Duration::days(max_expiry_days as i64);
            for instrument in catalog.current().values() {
                // Filter by currency and expiry
                if instrument.base_currency == *currency {
                    // Check expiry using the helper method
                    if let Some(expiry_dt) = instrument.expiration_datetime() {
                        if expiry_dt <= max_expiry_date {
                            instruments.push(Instrument::Derive(instrument.instrument_name.clone()));
                        }
                    }
                }
            }
            debug!(
                "discover_subscriptions: {} Derive instruments from catalog",
                instruments.len() - deribit_count
            );
        } else {
            // Fall back to cached state (legacy path)
            let state = self.state.read().await;
            for inst in &state.derive_subscriptions {
                instruments.push(Instrument::Derive(inst.clone()));
            }
        }

        let derive_count = instruments.len() - deribit_count;

        // Query Polymarket catalog if available
        if let Some(catalog) = &self.polymarket_catalog {
            let pattern = &self.config.polymarket_pattern;
            match catalog.find_by_slug_regex(pattern) {
                Ok(markets) => {
                    let mut polymarket_count = 0;
                    for market in markets {
                        // Must have "bitcoin" in slug
                        let slug = match &market.slug {
                            Some(s) if s.to_lowercase().contains("bitcoin") => s,
                            _ => continue,
                        };

                        // Parse strike and expiry from market
                        let strike = match Self::parse_strike_from_market(&market) {
                            Some(s) if s >= 10_000.0 && s <= 1_000_000.0 => s,
                            _ => continue,
                        };
                        let expiry_ms = match Self::parse_expiry_from_market(&market) {
                            Some(e) if e > now_ms => e,
                            _ => continue,
                        };

                        // Skip closed markets
                        if let Some(closed) = market.extra.get("closed").and_then(|v| v.as_bool()) {
                            if closed {
                                continue;
                            }
                        }

                        // Get YES/NO tokens
                        let yes_token = match market.yes_token() {
                            Some(t) => t.token_id.clone(),
                            None => continue,
                        };
                        let no_token = match market.no_token() {
                            Some(t) => t.token_id.clone(),
                            None => continue,
                        };

                        // Register market in state for price tracking
                        self.register_polymarket_market(
                            &market.id,
                            market.question.as_deref().unwrap_or(slug),
                            strike,
                            expiry_ms,
                            &yes_token,
                            &no_token,
                            market.minimum_order_size(),
                            market.minimum_tick_size(),
                        ).await;

                        instruments.push(Instrument::Polymarket(yes_token));
                        instruments.push(Instrument::Polymarket(no_token));
                        polymarket_count += 2;
                    }
                    debug!("discover_subscriptions: {} Polymarket tokens from catalog", polymarket_count);
                }
                Err(e) => {
                    warn!("discover_subscriptions: Polymarket regex failed: {}", e);
                    // Fall back to cached state
                    let state = self.state.read().await;
                    for market in state.polymarket_markets.values() {
                        instruments.push(Instrument::Polymarket(market.yes_token_id.clone()));
                        instruments.push(Instrument::Polymarket(market.no_token_id.clone()));
                    }
                }
            }
        } else {
            // Fall back to cached state (legacy path)
            let state = self.state.read().await;
            for market in state.polymarket_markets.values() {
                instruments.push(Instrument::Polymarket(market.yes_token_id.clone()));
                instruments.push(Instrument::Polymarket(market.no_token_id.clone()));
            }
        }

        info!(
            "discover_subscriptions: {} total ({} Deribit, {} Derive, {} Polymarket)",
            instruments.len(),
            deribit_count,
            derive_count,
            instruments.len() - deribit_count - derive_count
        );

        instruments
    }

    async fn on_event(&self, event: MarketEvent) {
        let now_ms = event.timestamp;

        match &event.instrument {
            Instrument::Deribit(name) => {
                self.handle_deribit_event(&event, name).await;
            }
            Instrument::Derive(name) => {
                self.handle_derive_event(&event, name).await;
            }
            Instrument::Polymarket(token_id) => {
                // Update Polymarket prices
                // NOTE: For BUYING opportunities, we care about the ASK (what we'd pay)
                // For SELLING opportunities, we care about the BID (what we'd receive)
                let mut state = self.state.write().await;
                
                // Increment event counter for throttling
                state.event_counter += 1;
                let event_count = state.event_counter;

                // HEARTBEAT: Log that we received an event (throttle to avoid spam)
                if event_count % 50 == 0 {
                    debug!("RX PM Event: {} | bid={:?} ask={:?}", token_id, event.best_bid, event.best_ask);
                }
                
                // Ensure reverse lookup is populated (handle deserialization from old state)
                // We check if the map length is consistent with markets (2 tokens per market)
                if state.token_to_market_key.len() != state.polymarket_markets.len() * 2 {
                    if !state.polymarket_markets.is_empty() {
                         warn!("Rebuilding token_to_market_key map ({} markets, {} keys -> expecting {})", 
                             state.polymarket_markets.len(), 
                             state.token_to_market_key.len(),
                             state.polymarket_markets.len() * 2
                         );
                        
                        // Clear potentially partial map to ensure clean rebuild
                        state.token_to_market_key.clear();
                        
                        let mappings: Vec<(String, String)> = state.polymarket_markets
                            .iter()
                            .flat_map(|(id, m)| vec![(m.yes_token_id.clone(), id.clone()), (m.no_token_id.clone(), id.clone())])
                            .collect();
                        for (token, id) in mappings {
                            state.token_to_market_key.insert(token, id);
                        }
                        info!("Map rebuild complete. New Token->Market entries: {}", state.token_to_market_key.len());
                    }
                }
                
                // O(1) lookup using token_to_market_key
                let market_key = state.token_to_market_key.get(token_id).cloned();
                
                if let Some(key) = market_key {
                    if let Some(market) = state.polymarket_markets.get_mut(&key) {
                        // Use ask for buy opportunities (we pay the ask)
                        if market.yes_token_id == *token_id {
                            if let Some(ask) = event.best_ask {
                                market.yes_price = ask;
                                market.last_updated = now_ms;
                            } else {
                                if event_count % 50 == 0 {
                                    debug!("PM UPDATE YES: No ask price for {} (bid={:?})", token_id, event.best_bid);
                                }
                            }
                            if let Some(bid) = event.best_bid {
                                market.yes_liquidity = bid * 100.0;
                            }
                        } else if market.no_token_id == *token_id {
                            if let Some(ask) = event.best_ask {
                                market.no_price = ask;
                                market.last_updated = now_ms;
                            } else {
                                if event_count % 50 == 0 {
                                     debug!("PM UPDATE NO: No ask price for {} (bid={:?})", token_id, event.best_bid);
                                }
                            }
                            if let Some(bid) = event.best_bid {
                                market.no_liquidity = bid * 100.0;
                            }
                        }
                    } else {
                         error!("CRITICAL: Token mapped to key {} but market not found!", key);
                    }
                } else {
                    // O(1) failed. Try O(N) fallback to check if we are desync'd
                    let mut found_slow = None;
                    for (id, m) in &mut state.polymarket_markets {
                        if m.yes_token_id == *token_id || m.no_token_id == *token_id {
                            found_slow = Some(id.clone());
                            break;
                        }
                    }

                    if let Some(id) = found_slow {
                        // Found via O(N)! Map is broken. Repair it.
                        warn!("CRITICAL: Map desync detected! Token {} not in map but found in market {}. Repairing...", token_id, id);
                        state.token_to_market_key.insert(token_id.clone(), id.clone());
                        
                        // Retry with the found ID
                        if let Some(market) = state.polymarket_markets.get_mut(&id) {
                            if market.yes_token_id == *token_id {
                                if let Some(ask) = event.best_ask {
                                    market.yes_price = ask;
                                    market.last_updated = now_ms;
                                }
                                if let Some(bid) = event.best_bid {
                                    market.yes_liquidity = bid * 100.0;
                                }
                            } else if market.no_token_id == *token_id {
                                if let Some(ask) = event.best_ask {
                                    market.no_price = ask;
                                    market.last_updated = now_ms;
                                }
                                if let Some(bid) = event.best_bid {
                                    market.no_liquidity = bid * 100.0;
                                }
                            }
                        }
                    } else {
                        // Truly unmatched
                        if state.event_counter % 100 == 0 { 
                             debug!("PM UNMATCHED token: {} (Map size: {}, Markets: {})", 
                                 token_id, state.token_to_market_key.len(), state.polymarket_markets.len());
                        }
                    }
                }
            }
        }

        // Check if we need to recalculate
        let should_recalc = {
            let state = self.state.read().await;
            let elapsed = (now_ms - state.last_recalc) / 1000;
            elapsed >= self.config.recalc_interval_secs as i64
        };

        if should_recalc {
            self.recalculate(now_ms).await;
        }
    }

    async fn initialize(&self) {
        self.initialize_subscriptions(self.config.max_expiry_days).await;
    }

    async fn get_portfolio_metrics(&self) -> Option<crate::traits::PortfolioMetrics> {
        let state = self.state.read().await;
        state.portfolio.as_ref().map(|p| crate::traits::PortfolioMetrics {
            expected_utility: p.expected_utility,
            expected_return: p.expected_return,
            prob_loss: p.prob_loss,
        })
    }
}

/// Helper function to set up the strategy with Deribit instruments.
pub async fn setup_deribit_subscriptions(
    strategy: &CrossMarketStrategy,
    catalog: &DeribitCatalog,
    currency: &str,
    max_expiry_days: u32,
) {
    let options = catalog.get_options(currency);
    let now = chrono::Utc::now();

    let mut state = strategy.state.write().await;

    for opt in options {
        // Filter by expiry
        let expiry = chrono::DateTime::from_timestamp_millis(opt.expiration_timestamp)
            .unwrap_or(now);
        let days_until = (expiry - now).num_days();

        if days_until < 0 || days_until > max_expiry_days as i64 {
            continue;
        }

        state.deribit_subscriptions.insert(opt.instrument_name);
    }

    info!(
        "CrossMarket: Set up {} Deribit subscriptions for {}",
        state.deribit_subscriptions.len(),
        currency
    );
}

/// Helper function to set up Derive subscriptions.
pub async fn setup_derive_subscriptions(
    strategy: &CrossMarketStrategy,
    catalog: &DeriveCatalog,
    currency: &str,
    max_expiry_days: u32,
) {
    let options = catalog.get_options(currency);
    let now = chrono::Utc::now();

    let mut state = strategy.state.write().await;

    for opt in options {
        // Filter by expiry
        if let Some(expiry_str) = &opt.expiry {
            if let Ok(expiry) = chrono::NaiveDate::parse_from_str(expiry_str, "%Y%m%d") {
                let expiry_dt = expiry.and_hms_opt(8, 0, 0).unwrap();
                let expiry_utc = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                    expiry_dt,
                    chrono::Utc,
                );
                let days_until = (expiry_utc - now).num_days();

                if days_until < 0 || days_until > max_expiry_days as i64 {
                    continue;
                }
            }
        }

        state.derive_subscriptions.insert(opt.instrument_name);
    }

    info!(
        "CrossMarket: Set up {} Derive subscriptions for {}",
        state.derive_subscriptions.len(),
        currency
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::ExecutionClient;
    use crate::catalog::{MarketInfo, TokenInfo};
    use crate::traits::ExecutionRouter;

    #[tokio::test]
    async fn test_strategy_creation() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test_cross_market", exec);

        assert_eq!(strategy.name(), "test_cross_market");
        
        let exchanges = strategy.required_exchanges();
        assert!(exchanges.contains(&Exchange::Deribit));
        assert!(exchanges.contains(&Exchange::Derive));
        assert!(exchanges.contains(&Exchange::Polymarket));
    }

    #[tokio::test]
    async fn test_polymarket_registration() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        strategy.register_polymarket_market(
            "condition_123",
            "Will BTC be above $100k on Dec 31?",
            100_000.0,
            1735689600000, // Dec 31, 2024
            "yes_token_123",
            "no_token_123",
            Some(15.0),  // typical minimum_order_size
            Some(0.01),  // typical minimum_tick_size
        ).await;

        strategy.update_polymarket_prices(
            "condition_123",
            0.45,
            0.55,
            1000.0,
            1000.0,
        ).await;

        let state = strategy.state.read().await;
        assert_eq!(state.polymarket_markets.len(), 1);
        let market = state.polymarket_markets.get("condition_123").unwrap();
        assert_eq!(market.yes_price, 0.45);
    }

    // ==========================================================================
    // Strike Parsing Tests
    // ==========================================================================

    #[test]
    fn test_parse_strike_from_slug_with_full_number() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: Some("bitcoin-above-100000-on-december-31".to_string()),
            question: None,
            description: None,
            tags: None,
            tokens: vec![],
            extra: serde_json::Value::Null,
        };
        
        let strike = CrossMarketStrategy::parse_strike_from_market(&market);
        assert_eq!(strike, Some(100000.0));
    }

    #[test]
    fn test_parse_strike_from_slug_with_k_suffix() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: Some("bitcoin-above-84k-on-january-1".to_string()),
            question: None,
            description: None,
            tags: None,
            tokens: vec![],
            extra: serde_json::Value::Null,
        };
        
        let strike = CrossMarketStrategy::parse_strike_from_market(&market);
        assert_eq!(strike, Some(84000.0));
    }

    #[test]
    fn test_parse_strike_from_question() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: Some("some-random-slug".to_string()),
            question: Some("Will the price of Bitcoin be above $150,000 on March 15?".to_string()),
            description: None,
            tags: None,
            tokens: vec![],
            extra: serde_json::Value::Null,
        };
        
        let strike = CrossMarketStrategy::parse_strike_from_market(&market);
        assert_eq!(strike, Some(150000.0));
    }

    #[test]
    fn test_parse_strike_no_match() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: Some("will-trump-win".to_string()),
            question: Some("Will Trump win the election?".to_string()),
            description: None,
            tags: None,
            tokens: vec![],
            extra: serde_json::Value::Null,
        };
        
        let strike = CrossMarketStrategy::parse_strike_from_market(&market);
        assert_eq!(strike, None);
    }

    // ==========================================================================
    // Expiry Parsing Tests
    // ==========================================================================

    #[test]
    fn test_parse_expiry_from_extra() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: None,
            question: None,
            description: None,
            tags: None,
            tokens: vec![],
            extra: serde_json::json!({
                "end_date_iso": "2025-12-31T00:00:00Z"
            }),
        };
        
        let expiry = CrossMarketStrategy::parse_expiry_from_market(&market);
        assert!(expiry.is_some());
        
        let dt = chrono::DateTime::from_timestamp_millis(expiry.unwrap()).unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2025-12-31");
    }

    #[test]
    fn test_parse_expiry_missing() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: None,
            question: None,
            description: None,
            tags: None,
            tokens: vec![],
            extra: serde_json::json!({}),
        };
        
        let expiry = CrossMarketStrategy::parse_expiry_from_market(&market);
        assert_eq!(expiry, None);
    }

    // ==========================================================================
    // Token Parsing Tests
    // ==========================================================================

    #[test]
    fn test_parse_tokens_with_outcomes() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: None,
            question: None,
            description: None,
            tags: None,
            tokens: vec![
                TokenInfo {
                    token_id: "yes_token_123".to_string(),
                    outcome: Some("Yes".to_string()),
                },
                TokenInfo {
                    token_id: "no_token_456".to_string(),
                    outcome: Some("No".to_string()),
                },
            ],
            extra: serde_json::Value::Null,
        };
        
        let tokens = CrossMarketStrategy::parse_tokens_from_market(&market);
        assert_eq!(tokens, Some(("yes_token_123".to_string(), "no_token_456".to_string())));
    }

    #[test]
    fn test_parse_tokens_without_outcomes() {
        // Falls back to first=YES, second=NO
        let market = MarketInfo {
            id: "test".to_string(),
            slug: None,
            question: None,
            description: None,
            tags: None,
            tokens: vec![
                TokenInfo {
                    token_id: "token_a".to_string(),
                    outcome: None,
                },
                TokenInfo {
                    token_id: "token_b".to_string(),
                    outcome: None,
                },
            ],
            extra: serde_json::Value::Null,
        };
        
        let tokens = CrossMarketStrategy::parse_tokens_from_market(&market);
        assert_eq!(tokens, Some(("token_a".to_string(), "token_b".to_string())));
    }

    #[test]
    fn test_parse_tokens_single_token_fails() {
        let market = MarketInfo {
            id: "test".to_string(),
            slug: None,
            question: None,
            description: None,
            tags: None,
            tokens: vec![
                TokenInfo {
                    token_id: "only_one".to_string(),
                    outcome: Some("Yes".to_string()),
                },
            ],
            extra: serde_json::Value::Null,
        };
        
        let tokens = CrossMarketStrategy::parse_tokens_from_market(&market);
        assert_eq!(tokens, None);
    }

    // ==========================================================================
    // Deribit Event Handling Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_deribit_event_updates_vol_surface() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // Add a Deribit subscription
        {
            let mut state = strategy.state.write().await;
            state.deribit_subscriptions.insert("BTC-27DEC24-100000-C".to_string());
        }

        // Simulate a Deribit market event with IV data
        let event = MarketEvent {
            timestamp: chrono::Utc::now().timestamp_millis(),
            instrument: Instrument::Deribit("BTC-27DEC24-100000-C".to_string()),
            best_bid: Some(0.05),
            best_ask: Some(0.055),
            delta: Some(0.45),
            mark_iv: Some(55.0),
            bid_iv: Some(54.0),
            ask_iv: Some(56.0),
            underlying_price: Some(95000.0),
        };

        strategy.on_event(event).await;

        // Check that the ticker was recorded
        let state = strategy.state.read().await;
        assert!(state.deribit_tickers.contains_key("BTC-27DEC24-100000-C"));
    }

    // ==========================================================================
    // Dashboard Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_dashboard_schema() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // Access via Dashboard trait
        let dashboard: &dyn Dashboard = strategy.as_ref();
        let schema = dashboard.dashboard_schema();
        
        // Should have multiple widgets
        assert!(!schema.widgets.is_empty(), "Dashboard should have widgets");
        
        // Should have at least 5 widgets (spot price, vol surface info, tables, etc.)
        assert!(schema.widgets.len() >= 5, "Dashboard should have at least 5 widgets");
    }

    #[tokio::test]
    async fn test_dashboard_state_json() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // Register a market and update prices
        strategy.register_polymarket_market(
            "test_condition",
            "Test question",
            100_000.0,
            chrono::Utc::now().timestamp_millis() + 86400000, // Tomorrow
            "yes_token",
            "no_token",
            Some(15.0),
            Some(0.01),
        ).await;

        strategy.update_polymarket_prices("test_condition", 0.50, 0.50, 100.0, 100.0).await;

        // Access via Dashboard trait
        let dashboard: &dyn Dashboard = strategy.as_ref();
        let state_json: Value = dashboard.dashboard_state().await;
        
        // Should be valid JSON
        assert!(state_json.is_object());
        
        // Check for expected fields (matching actual dashboard_state output)
        assert!(state_json.get("spot_price").is_some());
        assert!(state_json.get("num_expiries").is_some());
        assert!(state_json.get("polymarket_markets").is_some());
        assert!(state_json.get("opportunities").is_some());
        assert!(state_json.get("positions").is_some());
        
        // Should have 1 polymarket market
        assert_eq!(state_json.get("polymarket_markets").unwrap().as_i64(), Some(1));
    }

    // ==========================================================================
    // Config Tests
    // ==========================================================================

    #[test]
    fn test_default_config() {
        let config = CrossMarketConfig::default();
        
        assert_eq!(config.currency, "BTC");
        assert_eq!(config.min_edge, 0.02);
        assert_eq!(config.max_expiry_days, 90);
        assert!(config.polymarket_pattern.contains("bitcoin"));
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = CrossMarketConfig {
            currency: "ETH".to_string(),
            min_edge: 0.05,
            max_time_to_expiry: 0.25,
            max_expiry_days: 30,
            rate: 0.05,
            kelly_config: KellyConfig::default(),
            scanner_config: ScannerConfig::default(),
            polymarket_pattern: "ethereum-above-\\d+".to_string(),
            recalc_interval_secs: 120,
            vol_time_strategy: "calendar".to_string(),
            hourly_vols: Vec::new(),
            regime_scaler: 1.0,
        };

        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::new("eth_strategy", config, exec);

        assert_eq!(strategy.name(), "eth_strategy");
    }

    // ==========================================================================
    // Subscription Discovery Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_discover_subscriptions_includes_all_exchanges() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // Add test subscriptions
        {
            let mut state = strategy.state.write().await;
            state.deribit_subscriptions.insert("BTC-27DEC24-100000-C".to_string());
            state.derive_subscriptions.insert("BTC-20241227-100000-C".to_string());
        }

        strategy.register_polymarket_market(
            "pm_test",
            "Test",
            100_000.0,
            chrono::Utc::now().timestamp_millis() + 86400000,
            "yes_tok",
            "no_tok",
            Some(15.0),
            Some(0.01),
        ).await;

        let subs = strategy.discover_subscriptions().await;

        // Should have instruments from all three exchanges
        let has_deribit = subs.iter().any(|i| matches!(i, Instrument::Deribit(_)));
        let has_derive = subs.iter().any(|i| matches!(i, Instrument::Derive(_)));
        let has_polymarket = subs.iter().any(|i| matches!(i, Instrument::Polymarket(_)));

        assert!(has_deribit, "Should have Deribit subscriptions");
        assert!(has_derive, "Should have Derive subscriptions");
        assert!(has_polymarket, "Should have Polymarket subscriptions");
    }

    // ==========================================================================
    // Edge Case Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_update_nonexistent_polymarket() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // This should not panic
        strategy.update_polymarket_prices(
            "nonexistent_condition",
            0.50,
            0.50,
            100.0,
            100.0,
        ).await;

        let state = strategy.state.read().await;
        assert!(state.polymarket_markets.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_polymarket_registrations() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        for i in 0..10 {
            strategy.register_polymarket_market(
                &format!("condition_{}", i),
                &format!("BTC above ${}k?", 90 + i),
                (90000 + i * 1000) as f64,
                chrono::Utc::now().timestamp_millis() + 86400000 * (i as i64 + 1),
                &format!("yes_{}", i),
                &format!("no_{}", i),
                Some(15.0),
                Some(0.01),
            ).await;
        }

        let state = strategy.state.read().await;
        assert_eq!(state.polymarket_markets.len(), 10);
    }

    #[tokio::test]
    async fn test_log_entries_are_capped() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // Add more than MAX_LOG_ENTRIES
        for i in 0..MAX_LOG_ENTRIES + 50 {
            strategy.add_log("info", format!("Test log {}", i)).await;
        }

        let state = strategy.state.read().await;
        assert!(state.log.len() <= MAX_LOG_ENTRIES);
    }

    #[tokio::test]
    async fn test_recalc_update_without_opportunities() {
        use crate::traits::ExecutionRouter; 
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);

        // Ensure initially opportunities are empty
        {
            let state = strategy.state.read().await;
            assert!(state.opportunities.is_empty());
            assert_eq!(state.last_recalc, 0);
        }

        // Run recalculate
        let now = 1234567890;
        strategy.recalculate(now).await;

        // Verify last_recalc is updated even with 0 opportunities
        {
            let state = strategy.state.read().await;
            assert!(state.opportunities.is_empty());
            assert_eq!(state.last_recalc, now, "last_recalc should be updated even if no opportunities found");
        }
    }

    #[tokio::test]
    async fn test_event_throttling_counter() {
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);
        
        // Create a dummy event
        let event = MarketEvent {
            timestamp: 1000,
            instrument: Instrument::Polymarket("token_123".to_string()),
            best_bid: Some(0.5),
            best_ask: Some(0.6),
            delta: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            underlying_price: None,
        };

        // Send 250 events
        for _ in 0..250 {
            strategy.on_event(event.clone()).await;
        }

        let state = strategy.state.read().await;
        assert_eq!(state.event_counter, 250);
        // Log should be capped at 200
        assert!(state.log.len() <= MAX_LOG_ENTRIES);
    }

    #[tokio::test]
    async fn test_pnl_calculation_sell_flow() {
        use std::collections::HashMap;
        use crate::optimizer::opportunity::{Opportunity, OpportunityType, TradeDirection};
        use crate::optimizer::kelly::PositionAllocation;
        use crate::simulation::execution::SimulatedExecutionClient;
        
        // 1. Setup Simulation Client
        let sim_client =  Arc::new(SimulatedExecutionClient::new(100_000.0));
        let exec = Arc::new(ExecutionRouter::new(
            std::collections::HashMap::from([
                (Exchange::Derive, sim_client.clone() as Arc<dyn ExecutionClient>)
            ])
        ));
        
        let strategy = CrossMarketStrategy::with_defaults("test", exec);
        
        // 2. Manually insert a "Sell" opportunity
        let opp = Opportunity {
            id: "BTC-TEST-C_sell".to_string(),
            opportunity_type: OpportunityType::VanillaCall,
            exchange: "derive".to_string(),
            instrument_id: "BTC-TEST-C".to_string(),
            description: "Sell Call".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: 1234567890000,
            time_to_expiry: 0.1,
            direction: TradeDirection::Sell, // SHORT position
            market_price: 1000.0, // Selling for $1000 credit
            fair_value: 800.0,
            edge: 0.2,
            max_profit: 1000.0,
            max_loss: 10000.0,
            liquidity: 10.0,
            implied_probability: None,
            model_probability: None,
            model_iv: None,
            token_id: None,
            minimum_order_size: None,
            minimum_tick_size: None,
        };
        
        let opportunities = vec![opp.clone()];
        
        // 3. Simulate rebalance to OPEN position (size 0 -> 1)
        let positions = vec![
            PositionAllocation {
                opportunity_id: "BTC-TEST-C_sell".to_string(),
                size: 1.0,
                dollar_value: 1000.0,
                expected_profit: 200.0,
                utility_contribution: 0.0,
            }
        ];
        
        {
            // Update price in valid exchanges so market order works
            sim_client.update_price(Instrument::Derive("BTC-TEST-C".to_string()), 1000.0);
            
            // Current positions map (Empty)
            let current_positions = HashMap::new();
            
            // Call rebalance
            strategy.rebalance_portfolio(&current_positions, &positions, &opportunities).await;
            
            // Assert CORRECT behavior (Increase)
            let cash: f64 = sim_client.get_balance().await.unwrap();
            assert_eq!(cash, 101_000.0, "Cash should increase when opening a short position (receiving premium)");
        }
        
        // 4. Simulate rebalance to CLOSE position (size 1 -> 0)
        let empty_positions: Vec<PositionAllocation> = Vec::new();
        
        {
            // Current positions map: We are Short 1.0 "test_sell_opp"
            // Wait, "test_sell_opp" corresponds to "Short Instrument".
            // So if size is 1.0, we have -1.0 Instrument?
            // Let's verify our get_opportunity_positions logic or manual map.
            // Tests manually construct the map usually.
            
            // To mimic closing, we pass "Current State" = { "test_sell_opp": 1.0 }
            let mut current_positions = HashMap::new();
            current_positions.insert("BTC-TEST-C_sell".to_string(), 1.0);
            
            strategy.rebalance_portfolio(&current_positions, &empty_positions, &opportunities).await;
            
            // Assert CORRECT behavior (Decrease to close)
            let cash: f64 = sim_client.get_balance().await.unwrap();
            assert_eq!(cash, 100_000.0, "Cash should decrease when closing a short position (buying back)");
        }
    }
}


#[cfg(test)]
mod regression_tests {
    use super::*;
    use std::sync::Arc;
    use crate::optimizer::kelly::PositionAllocation;
    use crate::traits::{ExecutionClient, ExecutionRouter};

    #[tokio::test]
    async fn test_pnl_close_missing_opportunity() {
        use crate::simulation::execution::SimulatedExecutionClient;
        let sim_client =  Arc::new(SimulatedExecutionClient::new(10_000.0));
        let exec = Arc::new(ExecutionRouter::new(
            std::collections::HashMap::from([
                (Exchange::Polymarket, sim_client.clone() as Arc<dyn ExecutionClient>)
            ])
        ));
        
        let strategy = CrossMarketStrategy::with_defaults("test", exec);
        
        // 1. Setup Market State (via public API)
        let condition_id = "0x1234567890abcdef";
        strategy.register_polymarket_market(
            condition_id,
            "Will BTC be > 100k?",
            100_000.0,
            1700000000000,
            "yes_token",
            "no_token",
            None,
            None,
        ).await;
        
        // Set price to 0.50
        strategy.update_polymarket_prices(
            condition_id,
            0.50, // YES price
            0.50, // NO price
            1000.0,
            1000.0
        ).await;
        
        // Also update simulator price so execution works
        sim_client.update_price(Instrument::Polymarket("yes_token".to_string()), 0.50);
        
        // 2. Simulate holding a position
        let opp_id = format!("{}_yes", condition_id);
        
        // 3. Close position with NO opportunities present (empty scan)
        let positions: Vec<PositionAllocation> = Vec::new(); // Empty target portfolio
        let opportunities = Vec::new(); // Empty scan results
        
        {
            // Current positions map: We hold 100 contracts
            let mut current_positions = std::collections::HashMap::new();
            current_positions.insert(opp_id.clone(), 100.0);
            
            // Call private rebalance method
            strategy.rebalance_portfolio(&current_positions, &positions, &opportunities).await;
            
            // 4. Assert correctness
            // Value of position = 100 contracts * $0.50 = $50.0
            // Cash should increase from 10,000 to 10,050
            let cash = sim_client.get_balance().await.unwrap();
            let cash_diff = cash - 10_000.0;
            assert!((cash - 10_050.0).abs() < 1e-6, 
                "Cash did not increase correctly on close. Expected 10050 (+50), got {} (+{}). The position was closed at price 0.0 instead of 0.50!", 
                cash, cash_diff);
        }
    }

    #[tokio::test]
    async fn test_calculate_holdings_value_logic() {
        use crate::optimizer::opportunity::{Opportunity, OpportunityType, TradeDirection};
        
        let exec = Arc::new(ExecutionRouter::empty());
        let strategy = CrossMarketStrategy::with_defaults("test", exec);
        
        // 1. Create Opportunities
        let long_opp = Opportunity {
            id: "long_opp".to_string(),
            opportunity_type: OpportunityType::VanillaCall,
            exchange: "derive".to_string(),
            instrument_id: "BTC-LONG-C".to_string(),
            description: "Long Call".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: 1234567890000,
            time_to_expiry: 0.1,
            direction: TradeDirection::Buy, 
            market_price: 50.0, // Value = +50 per unit
            fair_value: 60.0,
            edge: 0.2,
            max_profit: 1000.0,
            max_loss: 50.0,
            liquidity: 10.0,
            implied_probability: None,
            model_probability: None,
            model_iv: None,
            token_id: None,
            minimum_order_size: None,
            minimum_tick_size: None,
        };
        
        let short_opp = Opportunity {
            id: "short_opp_sell".to_string(), // Suffix helps fallback inference too
            opportunity_type: OpportunityType::VanillaCall,
            exchange: "derive".to_string(),
            instrument_id: "BTC-SHORT-C".to_string(),
            description: "Short Call".to_string(),
            strike: 110_000.0,
            strike2: None,
            expiry_timestamp: 1234567890000,
            time_to_expiry: 0.1,
            direction: TradeDirection::Sell, 
            market_price: 20.0, // Liability = -20 per unit
            fair_value: 10.0,
            edge: 0.2,
            max_profit: 20.0,
            max_loss: 1000.0,
            liquidity: 10.0,
            implied_probability: None,
            model_probability: None,
            model_iv: None,
            token_id: None,
            minimum_order_size: None,
            minimum_tick_size: None,
        };
        
        let opportunities = vec![long_opp.clone(), short_opp.clone()];
        
        // 2. Setup Position State
        // (Just a map now)
        let mut current_positions = std::collections::HashMap::new();
        current_positions.insert("long_opp".to_string(), 10.0);
        current_positions.insert("short_opp_sell".to_string(), 5.0);
        
        // 3. Test Calculation (Active Scan)
        {
            let state = strategy.state.read().await;
            let value = strategy.calculate_holdings_value(&current_positions, &opportunities, 0, &state);
            
            // Expected: (10.0 * 50.0) + (5.0 * -20.0) = 500.0 - 100.0 = 400.0
            assert!((value - 400.0).abs() < 1e-6, "Holdings value incorrect with opportunities present. Expected 400.0, got {}", value);
        }
        
        // 4. Test Calculation (Fallback - No Opportunities in scan)
        {
            let state = strategy.state.read().await;
            let empty_opps: Vec<Opportunity> = Vec::new();
            
            let value = strategy.calculate_holdings_value(&current_positions, &empty_opps, 0, &state);
            assert_eq!(value, 0.0);
        }
    }
}


#[tokio::test]
async fn test_repro_spread_valuation_mismatch() {
    use crate::traits::ExecutionRouter;
    use std::sync::Arc;
    let exec = Arc::new(ExecutionRouter::empty());
    let strategy = CrossMarketStrategy::with_defaults("test", exec);
    
    let ticker_id = "simulation_spread";
    let opp_id = "simulation_spread_buy"; 
    
    // 1. Setup Ticker and Position
    {
            let mut state = strategy.state.write().await;
            
            // Insert Ticker
            state.derive_tickers.insert(ticker_id.to_string(), DeriveTicker {
            instrument_name: ticker_id.to_string(),
            timestamp: 0,
            underlying_price: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            strike: 0.0,
            expiry_timestamp: 0,
            best_bid: Some(100.0),
            best_ask: Some(110.0),
            best_bid_amount: None,
            best_ask_amount: None,
            });
            
            // Insert Position (Implicit in map)
            // state.simulated_positions.insert(opp_id.to_string(), 1.0); 
    }
    
    // 2. Calc Holdings Value (Fallback)
    let state = strategy.state.read().await;
    let empty_opps: Vec<crate::optimizer::opportunity::Opportunity> = Vec::new();
    
    let mut current_positions = std::collections::HashMap::new();
    current_positions.insert(opp_id.to_string(), 1.0);
    
    let value = strategy.calculate_holdings_value(&current_positions, &empty_opps, 0, &state);
    
    // 3. Assert
    println!("Calculated Value: {}", value);
    
    // We assert the CORRECT behavior (Negative value)
    assert_eq!(value, -100.0, "Spread position should be valued as liability (negative)");
}

#[tokio::test]
async fn test_spread_fallback_valuation() {
    use std::sync::Arc;
    use crate::traits::ExecutionRouter;
    
    let exec = Arc::new(ExecutionRouter::empty());
    let strategy = CrossMarketStrategy::with_defaults("test_spread", exec);
    
    let short_inst = "BTC-20240927-60000-C";
    let long_inst = "BTC-20240927-65000-C";
    let spread_id = format!("{}_{}_spread", short_inst, long_inst);
    
    // 1. Setup Tickers
    {
        let mut state = strategy.state.write().await;
        
        // Short Leg (Legacy Short): We are Short this, need to Buy back.
        // Current Market Ask = 110.0
        state.derive_tickers.insert(short_inst.to_string(), DeriveTicker {
            instrument_name: short_inst.to_string(),
            timestamp: 0,
            underlying_price: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            strike: 60000.0,
            expiry_timestamp: 0,
            best_bid: Some(100.0),
            best_ask: Some(110.0), // Cost to Buy
            best_bid_amount: None,
            best_ask_amount: None,

        });
        
        // Long Leg (Legacy Long): We are Long this, need to Sell.
        // Current Market Bid = 95.0
        state.derive_tickers.insert(long_inst.to_string(), DeriveTicker {
            instrument_name: long_inst.to_string(),
            timestamp: 0,
            underlying_price: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            strike: 65000.0,
            expiry_timestamp: 0,
            best_bid: Some(95.0), // Proceeds from Sell
            best_ask: Some(105.0),
            best_bid_amount: None,
            best_ask_amount: None,

        });
        
        // 2. Setup Position
        // We hold 1.0 unit of the Credit Spread (Short Position)
        // state.simulated_positions.insert(spread_id.clone(), 1.0);
    }
    
    // 3. Calculate Value
    let state = strategy.state.read().await;
    let empty_opps: Vec<crate::optimizer::opportunity::Opportunity> = Vec::new();
    
    let mut current_positions = std::collections::HashMap::new();
    current_positions.insert(spread_id.clone(), 1.0);

    // Should use fallback pricing
    let value = strategy.calculate_holdings_value(&current_positions, &empty_opps, 0, &state);
    
    // 4. Assert
    println!("Spread Value: {}", value);
    
    // Expected Cost to Close = Short_Ask (110) - Long_Bid (95) = 15.0
    // Since we are Short the spread, value should be -15.0
    assert!((value - (-15.0)).abs() < 1e-6, "Expected -15.0, got {}", value);
}
