// src/pricing/vol_surface.rs
//
// Implied volatility surface construction and interpolation.
// Built from Deribit option snapshots using exchange-provided IVs.
//
// Key features:
// - Strike interpolation: linear between known strikes, flat extrapolation
// - Time interpolation: uses total variance (σ²T) for calendar arbitrage-free
// - Supports multiple expiries with independent strike smiles

use std::collections::{BTreeMap, HashMap};
use serde::{Deserialize, Serialize};
use log::{debug, warn};

/// A single IV point on the surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IVPoint {
    /// Strike price
    pub strike: f64,
    /// Implied volatility (decimal, e.g., 0.50 for 50%)
    pub iv: f64,
    /// Bid IV if available
    pub bid_iv: Option<f64>,
    /// Ask IV if available
    pub ask_iv: Option<f64>,
}

/// A smile (single expiry slice of the surface).
#[derive(Debug, Clone, Default)]
pub struct VolSmile {
    /// Sorted mapping of strike → IV point
    points: BTreeMap<u64, IVPoint>,
    /// Time to expiry in years
    pub time_to_expiry: f64,
    /// Underlying price at this expiry (forward or spot)
    pub underlying_price: f64,
}

impl VolSmile {
    /// Creates a new empty smile.
    pub fn new(time_to_expiry: f64, underlying_price: f64) -> Self {
        Self {
            points: BTreeMap::new(),
            time_to_expiry,
            underlying_price,
        }
    }

    /// Adds an IV point to the smile.
    pub fn add_point(&mut self, strike: f64, iv: f64, bid_iv: Option<f64>, ask_iv: Option<f64>) {
        let key = (strike * 100.0) as u64; // Store at cent precision
        self.points.insert(key, IVPoint { strike, iv, bid_iv, ask_iv });
    }

    /// Gets the interpolated IV at a given strike.
    /// Uses linear interpolation between known strikes, flat extrapolation beyond.
    pub fn get_iv(&self, strike: f64) -> Option<f64> {
        if self.points.is_empty() {
            return None;
        }

        let key = (strike * 100.0) as u64;

        // Check for exact match
        if let Some(point) = self.points.get(&key) {
            return Some(point.iv);
        }

        // Find surrounding points for interpolation
        let mut lower: Option<&IVPoint> = None;
        let mut upper: Option<&IVPoint> = None;

        for (_, point) in &self.points {
            if point.strike <= strike {
                lower = Some(point);
            }
            if point.strike >= strike && upper.is_none() {
                upper = Some(point);
                break;
            }
        }

        match (lower, upper) {
            // Both bounds exist - linear interpolation
            (Some(l), Some(u)) if (u.strike - l.strike).abs() > 1e-10 => {
                let t = (strike - l.strike) / (u.strike - l.strike);
                Some(l.iv + t * (u.iv - l.iv))
            }
            // Only lower bound - flat extrapolation (OTM puts)
            (Some(l), None) => Some(l.iv),
            // Only upper bound - flat extrapolation (OTM calls)
            (None, Some(u)) => Some(u.iv),
            // Same point (strike very close to existing)
            (Some(l), Some(_)) => Some(l.iv),
            _ => None,
        }
    }

    /// Returns the ATM IV (at underlying price).
    pub fn atm_iv(&self) -> Option<f64> {
        self.get_iv(self.underlying_price)
    }

    /// Returns all strike points.
    pub fn strikes(&self) -> Vec<f64> {
        self.points.values().map(|p| p.strike).collect()
    }

    /// Returns the number of points in this smile.
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Returns true if the smile has no points.
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Returns the min and max strikes.
    pub fn strike_range(&self) -> Option<(f64, f64)> {
        let min = self.points.values().next()?.strike;
        let max = self.points.values().last()?.strike;
        Some((min, max))
    }
}

/// The complete implied volatility surface.
/// 
/// Indexed by expiry timestamp, containing smiles for each expiration date.
/// Supports interpolation in both strike and time dimensions.
#[derive(Debug, Clone, Default)]
pub struct VolatilitySurface {
    /// Smiles indexed by expiration timestamp (ms)
    smiles: BTreeMap<i64, VolSmile>,
    /// Underlying spot price
    spot_price: f64,
    /// Last update timestamp
    last_updated: i64,
}

impl VolatilitySurface {
    /// Creates a new empty surface.
    pub fn new(spot_price: f64) -> Self {
        Self {
            smiles: BTreeMap::new(),
            spot_price,
            last_updated: 0,
        }
    }

    /// Updates the spot price.
    pub fn set_spot(&mut self, spot: f64) {
        self.spot_price = spot;
    }

    /// Returns the current spot price.
    pub fn spot(&self) -> f64 {
        self.spot_price
    }

    /// Adds or updates a smile for a given expiry.
    pub fn add_smile(&mut self, expiry_timestamp: i64, smile: VolSmile) {
        self.smiles.insert(expiry_timestamp, smile);
        self.last_updated = chrono::Utc::now().timestamp_millis();
    }

    /// Gets the smile for a specific expiry.
    pub fn get_smile(&self, expiry_timestamp: i64) -> Option<&VolSmile> {
        self.smiles.get(&expiry_timestamp)
    }

    /// Gets the smile for a specific expiry mutably.
    pub fn get_smile_mut(&mut self, expiry_timestamp: i64) -> Option<&mut VolSmile> {
        self.smiles.get_mut(&expiry_timestamp)
    }

    /// Gets or creates a smile for a given expiry.
    pub fn get_or_create_smile(&mut self, expiry_timestamp: i64, time_to_expiry: f64, underlying: f64) -> &mut VolSmile {
        self.smiles.entry(expiry_timestamp).or_insert_with(|| {
            VolSmile::new(time_to_expiry, underlying)
        })
    }

    /// Returns all expiry timestamps.
    pub fn expiries(&self) -> Vec<i64> {
        self.smiles.keys().copied().collect()
    }

    /// Gets the IV at a specific strike and expiry.
    pub fn get_iv(&self, strike: f64, expiry_timestamp: i64) -> Option<f64> {
        self.smiles.get(&expiry_timestamp)?.get_iv(strike)
    }

    /// Gets the IV at a specific strike and time to expiry.
    /// Uses total variance interpolation between expiries for calendar arbitrage-free pricing.
    pub fn get_iv_interpolated(&self, strike: f64, time_to_expiry: f64, now_ms: i64) -> Option<f64> {
        if self.smiles.is_empty() {
            return None;
        }

        // Target expiry timestamp
        let target_expiry_ms = now_ms + (time_to_expiry * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        // Find surrounding expiries
        let mut near_expiry: Option<(i64, &VolSmile)> = None;
        let mut far_expiry: Option<(i64, &VolSmile)> = None;

        for (&exp_ts, smile) in &self.smiles {
            if exp_ts <= target_expiry_ms {
                near_expiry = Some((exp_ts, smile));
            }
            if exp_ts >= target_expiry_ms && far_expiry.is_none() {
                far_expiry = Some((exp_ts, smile));
                break;
            }
        }

        match (near_expiry, far_expiry) {
            // Exact match or very close
            (Some((near_ts, near_smile)), Some((far_ts, _))) if near_ts == far_ts => {
                near_smile.get_iv(strike)
            }
            // Both bounds exist - interpolate using total variance
            (Some((_near_ts, near_smile)), Some((_far_ts, far_smile))) => {
                let near_iv = near_smile.get_iv(strike)?;
                let far_iv = far_smile.get_iv(strike)?;

                let t_near = near_smile.time_to_expiry;
                let t_far = far_smile.time_to_expiry;

                if (t_far - t_near).abs() < 1e-10 {
                    return Some(near_iv);
                }

                // Total variance interpolation: Var = σ² × T
                let var_near = near_iv.powi(2) * t_near;
                let var_far = far_iv.powi(2) * t_far;

                // Interpolation weight
                let w = (time_to_expiry - t_near) / (t_far - t_near);
                let w = w.clamp(0.0, 1.0);

                let var_target = var_near * (1.0 - w) + var_far * w;

                // Recover IV from total variance
                if time_to_expiry > 1e-10 && var_target >= 0.0 {
                    Some((var_target / time_to_expiry).sqrt())
                } else {
                    Some(near_iv)
                }
            }
            // Only near expiry - extrapolate flat
            (Some((_, near_smile)), None) => {
                debug!("Vol surface: extrapolating beyond far expiry");
                near_smile.get_iv(strike)
            }
            // Only far expiry - extrapolate flat
            (None, Some((_, far_smile))) => {
                debug!("Vol surface: extrapolating before near expiry");
                far_smile.get_iv(strike)
            }
            _ => None,
        }
    }

    /// Gets the forward price at a specific expiry.
    pub fn get_forward(&self, expiry_timestamp: i64) -> Option<f64> {
        self.smiles.get(&expiry_timestamp).map(|s| s.underlying_price)
    }

    /// Returns the number of expiries in the surface.
    pub fn num_expiries(&self) -> usize {
        self.smiles.len()
    }

    /// Returns the total number of IV points across all expiries.
    pub fn total_points(&self) -> usize {
        self.smiles.values().map(|s| s.len()).sum()
    }

    /// Returns the last update timestamp.
    pub fn last_updated(&self) -> i64 {
        self.last_updated
    }

    /// Clears all data from the surface.
    pub fn clear(&mut self) {
        self.smiles.clear();
    }

    /// Removes expired smiles (expiry < now_ms).
    pub fn remove_expired(&mut self, now_ms: i64) {
        let expired: Vec<_> = self.smiles.keys()
            .filter(|&&exp| exp < now_ms)
            .copied()
            .collect();
        
        for exp in expired {
            self.smiles.remove(&exp);
        }
    }

    /// Builds the surface from Deribit ticker data.
    pub fn from_deribit_tickers(
        tickers: &[DeribitTickerInput],
        now_ms: i64,
    ) -> Self {
        let mut surface = Self::new(0.0);
        let mut underlying_prices: HashMap<i64, Vec<f64>> = HashMap::new();

        for ticker in tickers {
            // Skip if missing essential data
            let iv = match (ticker.mark_iv, ticker.bid_iv, ticker.ask_iv) {
                (Some(mark), _, _) => mark,
                (None, Some(bid), Some(ask)) => (bid + ask) / 2.0,
                _ => continue,
            };

            // Skip if IV is unreasonable
            if iv <= 0.0 || iv > 5.0 {
                warn!("Invalid IV: {}", iv);
                continue;
            }

            let Some(strike) = ticker.strike else { continue };
            let expiry = ticker.expiry_timestamp;

            // Calculate time to expiry
            let time_to_expiry = (expiry - now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
            if time_to_expiry <= 0.0 {
                continue;
            }

            // Track underlying prices for averaging
            if let Some(underlying) = ticker.underlying_price {
                underlying_prices.entry(expiry).or_default().push(underlying);
            }

            // Get or create the smile
            let underlying = ticker.underlying_price.unwrap_or(surface.spot_price);
            let smile = surface.get_or_create_smile(expiry, time_to_expiry, underlying);

            // Add the point
            smile.add_point(strike, iv, ticker.bid_iv, ticker.ask_iv);
        }

        // Update underlying prices to average for each expiry
        for (expiry, prices) in underlying_prices {
            if let Some(smile) = surface.get_smile_mut(expiry) {
                let avg = prices.iter().sum::<f64>() / prices.len() as f64;
                smile.underlying_price = avg;
                if surface.spot_price == 0.0 {
                    surface.spot_price = avg;
                }
            }
        }

        surface.last_updated = now_ms;
        surface
    }
}

/// Input structure for building the surface from Deribit data.
#[derive(Debug, Clone)]
pub struct DeribitTickerInput {
    pub instrument_name: String,
    pub strike: Option<f64>,
    pub expiry_timestamp: i64,
    pub underlying_price: Option<f64>,
    pub mark_iv: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
}

/// Parses a Deribit instrument name to extract strike and option type.
/// Format: BTC-29MAR24-60000-C
pub fn parse_deribit_instrument(name: &str) -> Option<(f64, char)> {
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() < 4 {
        return None;
    }
    
    let strike: f64 = parts[2].parse().ok()?;
    let option_type = parts[3].chars().next()?;
    
    Some((strike, option_type))
}

/// Parses a Deribit expiry string to timestamp.
/// Format: 29MAR24 -> timestamp
pub fn parse_deribit_expiry(expiry_str: &str) -> Option<i64> {
    // Parse format: DDMMMYY (e.g., "29MAR24")
    if expiry_str.len() < 7 {
        return None;
    }

    let day: u32 = expiry_str[0..2].parse().ok()?;
    let month_str = &expiry_str[2..5];
    let year: i32 = expiry_str[5..7].parse::<i32>().ok()? + 2000;

    let month = match month_str.to_uppercase().as_str() {
        "JAN" => 1,
        "FEB" => 2,
        "MAR" => 3,
        "APR" => 4,
        "MAY" => 5,
        "JUN" => 6,
        "JUL" => 7,
        "AUG" => 8,
        "SEP" => 9,
        "OCT" => 10,
        "NOV" => 11,
        "DEC" => 12,
        _ => return None,
    };

    // Deribit options expire at 08:00 UTC
    let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)?
        .and_hms_opt(8, 0, 0)?;
    
    Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc).timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_smile_interpolation() {
        let mut smile = VolSmile::new(0.5, 100_000.0);
        smile.add_point(90_000.0, 0.50, None, None);
        smile.add_point(100_000.0, 0.45, None, None);
        smile.add_point(110_000.0, 0.48, None, None);

        // Exact match
        assert!((smile.get_iv(100_000.0).unwrap() - 0.45).abs() < 1e-6);

        // Linear interpolation between 100k and 110k
        let iv_105k = smile.get_iv(105_000.0).unwrap();
        assert!(iv_105k > 0.45 && iv_105k < 0.48);

        // Flat extrapolation below min strike
        assert!((smile.get_iv(80_000.0).unwrap() - 0.50).abs() < 1e-6);

        // Flat extrapolation above max strike
        assert!((smile.get_iv(120_000.0).unwrap() - 0.48).abs() < 1e-6);
    }

    #[test]
    fn test_total_variance_interpolation() {
        let mut surface = VolatilitySurface::new(100_000.0);

        // Near expiry: T=0.25 (3 months), IV=0.50
        let now_ms = 1704067200000i64; // Jan 1, 2024 00:00 UTC
        let near_expiry_ms = now_ms + (0.25 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;
        let far_expiry_ms = now_ms + (1.0 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut near_smile = VolSmile::new(0.25, 100_000.0);
        near_smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(near_expiry_ms, near_smile);

        // Far expiry: T=1.0 (1 year), IV=0.40
        let mut far_smile = VolSmile::new(1.0, 100_000.0);
        far_smile.add_point(100_000.0, 0.40, None, None);
        surface.add_smile(far_expiry_ms, far_smile);

        // Interpolate at T=0.5 (6 months)
        let iv_6m = surface.get_iv_interpolated(100_000.0, 0.5, now_ms);
        assert!(iv_6m.is_some());
        let iv = iv_6m.unwrap();
        
        // Should be between 0.40 and 0.50, following total variance
        assert!(iv > 0.40 && iv < 0.50, "IV at 6m: {}", iv);
    }

    #[test]
    fn test_parse_deribit_instrument() {
        let result = parse_deribit_instrument("BTC-29MAR24-60000-C");
        assert!(result.is_some());
        let (strike, opt_type) = result.unwrap();
        assert!((strike - 60000.0).abs() < 1e-6);
        assert_eq!(opt_type, 'C');
    }

    #[test]
    fn test_parse_deribit_expiry() {
        let ts = parse_deribit_expiry("29MAR24");
        assert!(ts.is_some());
        // Should be March 29, 2024, 08:00 UTC
        let expected = chrono::NaiveDate::from_ymd_opt(2024, 3, 29)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap();
        let expected_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(expected, chrono::Utc)
            .timestamp_millis();
        assert_eq!(ts.unwrap(), expected_ts);
    }

    #[test]
    fn test_parse_deribit_expiry_all_months() {
        // Test all month abbreviations
        let months = [
            ("15JAN25", 1), ("15FEB25", 2), ("15MAR25", 3), ("15APR25", 4),
            ("15MAY25", 5), ("15JUN25", 6), ("15JUL25", 7), ("15AUG25", 8),
            ("15SEP25", 9), ("15OCT25", 10), ("15NOV25", 11), ("15DEC25", 12),
        ];
        for (s, expected_month) in months {
            let ts = parse_deribit_expiry(s);
            assert!(ts.is_some(), "Failed to parse: {}", s);
            let dt = chrono::DateTime::from_timestamp_millis(ts.unwrap()).unwrap();
            assert_eq!(dt.month(), expected_month, "Wrong month for {}", s);
        }
    }

    #[test]
    fn test_parse_deribit_expiry_invalid() {
        assert!(parse_deribit_expiry("").is_none());
        assert!(parse_deribit_expiry("29XXX24").is_none());
        assert!(parse_deribit_expiry("ABCDEF").is_none());
        assert!(parse_deribit_expiry("29MAR").is_none()); // Too short
    }

    #[test]
    fn test_from_deribit_tickers_basic() {
        let now_ms = 1704067200000i64; // Jan 1, 2024
        let expiry_ms = now_ms + 30 * 24 * 3600 * 1000; // 30 days out

        let tickers = vec![
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-90000-C".to_string(),
                strike: Some(90_000.0),
                expiry_timestamp: expiry_ms,
                underlying_price: Some(95_000.0),
                mark_iv: Some(0.55),
                bid_iv: Some(0.54),
                ask_iv: Some(0.56),
            },
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-100000-C".to_string(),
                strike: Some(100_000.0),
                expiry_timestamp: expiry_ms,
                underlying_price: Some(95_000.0),
                mark_iv: Some(0.50),
                bid_iv: None,
                ask_iv: None,
            },
        ];

        let surface = VolatilitySurface::from_deribit_tickers(&tickers, now_ms);

        assert_eq!(surface.num_expiries(), 1);
        assert_eq!(surface.total_points(), 2);
        assert!((surface.spot() - 95_000.0).abs() < 1.0);

        // Check IV at 90k strike
        let iv = surface.get_iv(90_000.0, expiry_ms);
        assert!(iv.is_some());
        assert!((iv.unwrap() - 0.55).abs() < 0.01);
    }

    #[test]
    fn test_from_deribit_tickers_filters_invalid_iv() {
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + 30 * 24 * 3600 * 1000;

        let tickers = vec![
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-90000-C".to_string(),
                strike: Some(90_000.0),
                expiry_timestamp: expiry_ms,
                underlying_price: Some(95_000.0),
                mark_iv: Some(0.0), // Invalid: zero IV
                bid_iv: None,
                ask_iv: None,
            },
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-100000-C".to_string(),
                strike: Some(100_000.0),
                expiry_timestamp: expiry_ms,
                underlying_price: Some(95_000.0),
                mark_iv: Some(6.0), // Invalid: > 500% IV
                bid_iv: None,
                ask_iv: None,
            },
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-110000-C".to_string(),
                strike: Some(110_000.0),
                expiry_timestamp: expiry_ms,
                underlying_price: Some(95_000.0),
                mark_iv: Some(0.45), // Valid
                bid_iv: None,
                ask_iv: None,
            },
        ];

        let surface = VolatilitySurface::from_deribit_tickers(&tickers, now_ms);
        
        // Only the valid IV should be included
        assert_eq!(surface.total_points(), 1);
    }

    #[test]
    fn test_from_deribit_tickers_uses_bid_ask_mid() {
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + 30 * 24 * 3600 * 1000;

        let tickers = vec![
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-90000-C".to_string(),
                strike: Some(90_000.0),
                expiry_timestamp: expiry_ms,
                underlying_price: Some(95_000.0),
                mark_iv: None, // No mark IV
                bid_iv: Some(0.50),
                ask_iv: Some(0.60),
            },
        ];

        let surface = VolatilitySurface::from_deribit_tickers(&tickers, now_ms);
        
        // Should use mid of bid/ask = 0.55
        let iv = surface.get_iv(90_000.0, expiry_ms);
        assert!(iv.is_some());
        assert!((iv.unwrap() - 0.55).abs() < 0.01);
    }

    #[test]
    fn test_surface_multiple_expiries() {
        let now_ms = 1704067200000i64;
        let expiry1 = now_ms + 30 * 24 * 3600 * 1000; // 30 days
        let expiry2 = now_ms + 60 * 24 * 3600 * 1000; // 60 days

        let tickers = vec![
            DeribitTickerInput {
                instrument_name: "BTC-31JAN24-90000-C".to_string(),
                strike: Some(90_000.0),
                expiry_timestamp: expiry1,
                underlying_price: Some(95_000.0),
                mark_iv: Some(0.50),
                bid_iv: None,
                ask_iv: None,
            },
            DeribitTickerInput {
                instrument_name: "BTC-01MAR24-90000-C".to_string(),
                strike: Some(90_000.0),
                expiry_timestamp: expiry2,
                underlying_price: Some(95_000.0),
                mark_iv: Some(0.45),
                bid_iv: None,
                ask_iv: None,
            },
        ];

        let surface = VolatilitySurface::from_deribit_tickers(&tickers, now_ms);

        assert_eq!(surface.num_expiries(), 2);
        
        // Check IVs at each expiry
        assert!((surface.get_iv(90_000.0, expiry1).unwrap() - 0.50).abs() < 0.01);
        assert!((surface.get_iv(90_000.0, expiry2).unwrap() - 0.45).abs() < 0.01);
    }

    #[test]
    fn test_atm_iv() {
        let mut smile = VolSmile::new(0.5, 100_000.0);
        smile.add_point(90_000.0, 0.55, None, None);
        smile.add_point(100_000.0, 0.50, None, None);  // ATM
        smile.add_point(110_000.0, 0.52, None, None);

        let atm = smile.atm_iv();
        assert!(atm.is_some());
        assert!((atm.unwrap() - 0.50).abs() < 0.01);
    }

    #[test]
    fn test_smile_empty() {
        let smile = VolSmile::new(0.5, 100_000.0);
        assert!(smile.get_iv(100_000.0).is_none());
        assert!(smile.atm_iv().is_none());
        assert!(smile.is_empty());
    }
}

