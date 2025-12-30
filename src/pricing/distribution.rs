// src/pricing/distribution.rs
//
// Price distribution functions for Monte Carlo simulation.
// Computes CDF and inverse CDF (PPF) using the volatility surface.

use super::black_scholes::norm_cdf;
use super::vol_surface::VolatilitySurface;

/// Number of points for the price grid (higher = more accuracy, more memory).
const DEFAULT_GRID_SIZE: usize = 500;

/// Number of standard deviations to cover in the price grid.
const NUM_STD_DEVS: f64 = 4.0;

/// A precomputed inverse CDF (PPF) for a specific expiry.
/// Maps percentiles [0, 1] to prices.
#[derive(Debug, Clone)]
pub struct ExpiryDistribution {
    /// Expiry timestamp (ms)
    pub expiry_timestamp: i64,
    /// Time to expiry in years
    pub time_to_expiry: f64,
    /// Forward/underlying price at this expiry
    pub forward_price: f64,
    /// ATM IV used for this distribution
    pub atm_iv: f64,
    /// Price grid (sorted ascending)
    prices: Vec<f64>,
    /// CDF values at each price point
    cdf_values: Vec<f64>,
}

impl ExpiryDistribution {
    /// Creates a new distribution for a specific expiry from the vol surface.
    pub fn from_vol_surface(
        surface: &VolatilitySurface,
        expiry_timestamp: i64,
        now_ms: i64,
        rate: f64,
    ) -> Option<Self> {
        let smile = surface.get_smile(expiry_timestamp)?;
        let forward = smile.underlying_price;
        let time_to_expiry = smile.time_to_expiry;

        if time_to_expiry <= 0.0 || forward <= 0.0 {
            return None;
        }

        let atm_iv = smile.atm_iv().unwrap_or(0.5);
        
        Some(Self::build(
            expiry_timestamp,
            time_to_expiry,
            forward,
            atm_iv,
            surface,
            now_ms,
            rate,
        ))
    }

    /// Builds the distribution with a custom grid size.
    fn build(
        expiry_timestamp: i64,
        time_to_expiry: f64,
        forward_price: f64,
        atm_iv: f64,
        surface: &VolatilitySurface,
        now_ms: i64,
        rate: f64,
    ) -> Self {
        // Determine price range (±4 std devs in log space)
        let vol_sqrt_t = atm_iv * time_to_expiry.sqrt();
        let log_forward = forward_price.ln();
        
        let log_min = log_forward - NUM_STD_DEVS * vol_sqrt_t;
        let log_max = log_forward + NUM_STD_DEVS * vol_sqrt_t;

        let mut prices = Vec::with_capacity(DEFAULT_GRID_SIZE);
        let mut cdf_values = Vec::with_capacity(DEFAULT_GRID_SIZE);

        // Build price grid
        for i in 0..DEFAULT_GRID_SIZE {
            let t = i as f64 / (DEFAULT_GRID_SIZE - 1) as f64;
            let log_price = log_min + t * (log_max - log_min);
            let price = log_price.exp();
            prices.push(price);
        }

        // Calculate CDF at each price point using Black-Scholes
        for &price in &prices {
            // P(S_T < K) = N(-d2) for a put
            let iv = surface
                .get_iv_interpolated(price, time_to_expiry, now_ms)
                .unwrap_or(atm_iv);

            if iv <= 0.0 || time_to_expiry <= 0.0 {
                cdf_values.push(if price < forward_price { 0.5 } else { 0.5 });
                continue;
            }

            let d2 = Self::compute_d2(forward_price, price, time_to_expiry, rate, iv);
            let prob_below = norm_cdf(-d2); // P(S < K)
            cdf_values.push(prob_below);
        }

        // Ensure CDF is monotonically increasing
        for i in 1..cdf_values.len() {
            if cdf_values[i] < cdf_values[i - 1] {
                cdf_values[i] = cdf_values[i - 1];
            }
        }

        // Ensure CDF spans [0, 1]
        if let Some(first) = cdf_values.first_mut() {
            *first = first.max(0.0);
        }
        if let Some(last) = cdf_values.last_mut() {
            *last = last.min(1.0);
        }

        Self {
            expiry_timestamp,
            time_to_expiry,
            forward_price,
            atm_iv,
            prices,
            cdf_values,
        }
    }

    /// Computes d2 for Black-Scholes.
    fn compute_d2(spot: f64, strike: f64, t: f64, r: f64, sigma: f64) -> f64 {
        let sqrt_t = t.sqrt();
        let d1 = ((spot / strike).ln() + (r + 0.5 * sigma.powi(2)) * t) / (sigma * sqrt_t);
        d1 - sigma * sqrt_t
    }

    /// Gets the price at a given percentile (inverse CDF / PPF).
    /// percentile should be in [0, 1].
    pub fn ppf(&self, percentile: f64) -> f64 {
        let percentile = percentile.clamp(0.0001, 0.9999);

        // Binary search for the percentile
        let idx = match self.cdf_values.binary_search_by(|cdf| {
            cdf.partial_cmp(&percentile).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        if idx >= self.prices.len() - 1 {
            return *self.prices.last().unwrap();
        }
        if idx == 0 && self.cdf_values[0] > percentile {
            return self.prices[0];
        }

        // Linear interpolation between grid points
        let cdf_low = self.cdf_values[idx];
        let cdf_high = self.cdf_values[idx + 1];
        let price_low = self.prices[idx];
        let price_high = self.prices[idx + 1];

        if (cdf_high - cdf_low).abs() < 1e-10 {
            return price_low;
        }

        let t = (percentile - cdf_low) / (cdf_high - cdf_low);
        price_low + t * (price_high - price_low)
    }

    /// Gets the CDF value at a given price.
    /// Returns P(S_T < price).
    pub fn cdf(&self, price: f64) -> f64 {
        if price <= self.prices[0] {
            return 0.0;
        }
        if price >= *self.prices.last().unwrap() {
            return 1.0;
        }

        // Binary search for the price
        let idx = match self.prices.binary_search_by(|p| {
            p.partial_cmp(&price).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            Ok(i) => return self.cdf_values[i],
            Err(i) => i.saturating_sub(1),
        };

        // Linear interpolation
        let price_low = self.prices[idx];
        let price_high = self.prices[idx + 1];
        let cdf_low = self.cdf_values[idx];
        let cdf_high = self.cdf_values[idx + 1];

        let t = (price - price_low) / (price_high - price_low);
        cdf_low + t * (cdf_high - cdf_low)
    }

    /// Gets the probability that S > strike at expiry.
    pub fn probability_above(&self, strike: f64) -> f64 {
        1.0 - self.cdf(strike)
    }

    /// Gets the probability that S < strike at expiry.
    pub fn probability_below(&self, strike: f64) -> f64 {
        self.cdf(strike)
    }

    /// Returns the min and max prices in the grid.
    pub fn price_range(&self) -> (f64, f64) {
        (self.prices[0], *self.prices.last().unwrap())
    }
}

/// Complete price distribution for all expiries in the surface.
#[derive(Debug, Clone, Default)]
pub struct PriceDistribution {
    /// Distributions indexed by expiry timestamp (for PPF lookups)
    distributions: std::collections::BTreeMap<i64, ExpiryDistribution>,
    /// Spot price from the vol surface
    spot: f64,
    /// Creation timestamp
    now_ms: i64,
    /// Risk-free rate used
    rate: f64,
    /// Cached vol surface data for interpolation: (expiry_ms, time_to_expiry, atm_iv)
    expiry_data: Vec<(i64, f64, f64)>,
}

impl PriceDistribution {
    /// Creates a new price distribution from a volatility surface.
    pub fn from_vol_surface(
        surface: &VolatilitySurface,
        now_ms: i64,
        rate: f64,
    ) -> Self {
        let mut dist = Self {
            distributions: std::collections::BTreeMap::new(),
            spot: surface.spot(),
            now_ms,
            rate,
            expiry_data: Vec::new(),
        };

        for expiry in surface.expiries() {
            if let Some(expiry_dist) = ExpiryDistribution::from_vol_surface(surface, expiry, now_ms, rate) {
                // Cache expiry data for interpolation
                dist.expiry_data.push((expiry, expiry_dist.time_to_expiry, expiry_dist.atm_iv));
                dist.distributions.insert(expiry, expiry_dist);
            }
        }

        // Sort expiry data by time
        dist.expiry_data.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        dist
    }

    /// Gets the distribution for a specific expiry.
    pub fn get(&self, expiry_timestamp: i64) -> Option<&ExpiryDistribution> {
        self.distributions.get(&expiry_timestamp)
    }

    /// Interpolates IV for a given time to expiry using total variance method.
    /// This is calendar-arbitrage-free interpolation.
    fn interpolate_iv(&self, time_to_expiry: f64) -> Option<f64> {
        if self.expiry_data.is_empty() || time_to_expiry <= 0.0 {
            return None;
        }

        // Find bracketing expiries
        let mut before: Option<(f64, f64)> = None; // (T, σ)
        let mut after: Option<(f64, f64)> = None;

        for &(_exp, t, iv) in &self.expiry_data {
            if t <= time_to_expiry {
                before = Some((t, iv));
            } else if after.is_none() {
                after = Some((t, iv));
            }
        }

        match (before, after) {
            // Interpolate using total variance: Var = σ² × T
            (Some((t1, iv1)), Some((t2, iv2))) => {
                let var1 = iv1 * iv1 * t1;
                let var2 = iv2 * iv2 * t2;
                let w = (time_to_expiry - t1) / (t2 - t1);
                let var_interp = var1 + w * (var2 - var1);
                Some((var_interp / time_to_expiry).sqrt())
            }
            // Extrapolate flat (use nearest)
            (Some((_, iv)), None) => Some(iv),
            (None, Some((_, iv))) => Some(iv),
            (None, None) => None,
        }
    }

    /// Gets the probability that S > strike at a given expiry using interpolated IV.
    /// This properly handles expiries that don't match Deribit expiries exactly.
    pub fn probability_above(&self, strike: f64, expiry_timestamp: i64) -> Option<f64> {
        // First try exact match (fast path for Deribit expiries)
        if let Some(d) = self.distributions.get(&expiry_timestamp) {
            return Some(d.probability_above(strike));
        }
        
        // Interpolate for non-standard expiries (e.g., Polymarket)
        self.probability_above_interpolated(strike, expiry_timestamp)
    }

    /// Computes P(S > K) using interpolated IV and Black-Scholes.
    pub fn probability_above_interpolated(&self, strike: f64, expiry_timestamp: i64) -> Option<f64> {
        let time_to_expiry = (expiry_timestamp - self.now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
        
        if time_to_expiry <= 0.0 || self.spot <= 0.0 || strike <= 0.0 {
            return None;
        }

        let iv = self.interpolate_iv(time_to_expiry)?;
        
        // P(S > K) = N(d2) where d2 = (ln(S/K) + (r - 0.5σ²)T) / (σ√T)
        let sqrt_t = time_to_expiry.sqrt();
        let d2 = ((self.spot / strike).ln() + (self.rate - 0.5 * iv * iv) * time_to_expiry) 
                 / (iv * sqrt_t);
        
        Some(norm_cdf(d2))
    }

    /// Gets the probability that S < strike at a given expiry.
    pub fn probability_below(&self, strike: f64, expiry_timestamp: i64) -> Option<f64> {
        self.probability_above(strike, expiry_timestamp).map(|p| 1.0 - p)
    }

    /// Gets the PPF value at a given percentile and expiry.
    pub fn ppf(&self, percentile: f64, expiry_timestamp: i64) -> Option<f64> {
        self.distributions.get(&expiry_timestamp).map(|d| d.ppf(percentile))
    }

    /// Gets the PPF value for a given percentile and time to expiry using interpolation.
    /// 
    /// This handles Polymarket expiries that don't match Deribit expiries exactly.
    /// Uses interpolated IV and lognormal price assumption.
    pub fn ppf_interpolated(&self, percentile: f64, time_to_expiry: f64) -> Option<f64> {
        if time_to_expiry <= 0.0 || self.spot <= 0.0 {
            return None;
        }

        let iv = self.interpolate_iv(time_to_expiry)?;
        
        // For lognormal: S_T = S_0 * exp((r - 0.5σ²)T + σ√T * Z)
        // where Z = Φ^(-1)(percentile)
        let z = super::black_scholes::norm_ppf(percentile);
        let sqrt_t = time_to_expiry.sqrt();
        let drift = (self.rate - 0.5 * iv * iv) * time_to_expiry;
        let diffusion = iv * sqrt_t * z;
        
        Some(self.spot * (drift + diffusion).exp())
    }

    /// Returns the spot price used for this distribution.
    pub fn spot(&self) -> f64 {
        self.spot
    }

    /// Returns all expiry timestamps with distributions.
    pub fn expiries(&self) -> Vec<i64> {
        self.distributions.keys().copied().collect()
    }

    /// Returns the number of expiries.
    pub fn num_expiries(&self) -> usize {
        self.distributions.len()
    }

    /// Interpolates or finds the closest distribution for a given time to expiry.
    pub fn get_for_time(&self, time_to_expiry: f64, now_ms: i64) -> Option<&ExpiryDistribution> {
        let target_expiry = now_ms + (time_to_expiry * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;
        
        // Find the closest expiry
        let mut closest: Option<(i64, &ExpiryDistribution)> = None;
        let mut min_diff = i64::MAX;

        for (&exp, dist) in &self.distributions {
            let diff = (exp - target_expiry).abs();
            if diff < min_diff {
                min_diff = diff;
                closest = Some((exp, dist));
            }
        }

        closest.map(|(_, d)| d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::vol_surface::{VolSmile, VolatilitySurface};

    #[test]
    fn test_ppf_cdf_inverse() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.5 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.5, 100_000.0);
        smile.add_point(80_000.0, 0.55, None, None);
        smile.add_point(90_000.0, 0.50, None, None);
        smile.add_point(100_000.0, 0.45, None, None);
        smile.add_point(110_000.0, 0.48, None, None);
        smile.add_point(120_000.0, 0.52, None, None);
        surface.add_smile(expiry_ms, smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        let expiry_dist = dist.get(expiry_ms).unwrap();

        // Test that PPF and CDF are inverses
        for percentile in [0.1, 0.25, 0.5, 0.75, 0.9] {
            let price = expiry_dist.ppf(percentile);
            let recovered = expiry_dist.cdf(price);
            assert!(
                (percentile - recovered).abs() < 0.02,
                "percentile={}, price={}, recovered={}",
                percentile, price, recovered
            );
        }
    }

    #[test]
    fn test_probability_above_below() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.25 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.25, 100_000.0);
        smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(expiry_ms, smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);

        // ATM should be roughly 50/50
        let prob_above = dist.probability_above(100_000.0, expiry_ms).unwrap();
        let prob_below = dist.probability_below(100_000.0, expiry_ms).unwrap();

        assert!((prob_above + prob_below - 1.0).abs() < 0.01);
        assert!((prob_above - 0.5).abs() < 0.1);
    }

    #[test]
    fn test_ppf_monotonic() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.5 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.5, 100_000.0);
        smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(expiry_ms, smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        let expiry_dist = dist.get(expiry_ms).unwrap();

        // PPF should be monotonically increasing
        let mut prev_price = 0.0;
        for i in 1..100 {
            let percentile = i as f64 / 100.0;
            let price = expiry_dist.ppf(percentile);
            assert!(price >= prev_price, "PPF not monotonic at {}", percentile);
            prev_price = price;
        }
    }

    #[test]
    fn test_probability_interpolated_between_expiries() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        
        // Near expiry: 30 days, IV=50%
        let near_expiry = now_ms + 30 * 24 * 3600 * 1000;
        let mut near_smile = VolSmile::new(30.0 / 365.25, 100_000.0);
        near_smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(near_expiry, near_smile);

        // Far expiry: 90 days, IV=45%
        let far_expiry = now_ms + 90 * 24 * 3600 * 1000;
        let mut far_smile = VolSmile::new(90.0 / 365.25, 100_000.0);
        far_smile.add_point(100_000.0, 0.45, None, None);
        surface.add_smile(far_expiry, far_smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);

        // Query for an expiry in between (60 days)
        let target_expiry = now_ms + 60 * 24 * 3600 * 1000;
        let prob = dist.probability_above(100_000.0, target_expiry);
        
        // Should be able to interpolate
        assert!(prob.is_some());
        // ATM should still be ~50%
        assert!((prob.unwrap() - 0.5).abs() < 0.15, "ATM prob: {}", prob.unwrap());
    }

    #[test]
    fn test_probability_deep_itm_and_otm() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.25 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.25, 100_000.0);
        smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(expiry_ms, smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);

        // Deep ITM (strike way below spot) - high probability of being above
        let prob_deep_itm = dist.probability_above(50_000.0, expiry_ms).unwrap();
        assert!(prob_deep_itm > 0.95, "Deep ITM prob: {}", prob_deep_itm);

        // Deep OTM (strike way above spot) - low probability of being above
        let prob_deep_otm = dist.probability_above(200_000.0, expiry_ms).unwrap();
        assert!(prob_deep_otm < 0.05, "Deep OTM prob: {}", prob_deep_otm);
    }

    #[test]
    fn test_price_range() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.5 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.5, 100_000.0);
        smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(expiry_ms, smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        let expiry_dist = dist.get(expiry_ms).unwrap();

        let (min_price, max_price) = expiry_dist.price_range();
        
        // Grid should be centered around spot with reasonable range
        assert!(min_price < 100_000.0);
        assert!(max_price > 100_000.0);
        // For 50% IV, 6-month, ±4σ should give roughly 40k-250k range
        assert!(min_price > 20_000.0 && min_price < 80_000.0);
        assert!(max_price > 120_000.0 && max_price < 500_000.0);
    }

    #[test]
    fn test_num_expiries() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;

        for i in 1..=5 {
            let expiry_ms = now_ms + (i as i64 * 30 * 24 * 3600 * 1000);
            let mut smile = VolSmile::new(i as f64 * 30.0 / 365.25, 100_000.0);
            smile.add_point(100_000.0, 0.50, None, None);
            surface.add_smile(expiry_ms, smile);
        }

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        assert_eq!(dist.num_expiries(), 5);
        assert_eq!(dist.expiries().len(), 5);
    }

    #[test]
    fn test_empty_surface_gives_empty_distribution() {
        let surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        assert_eq!(dist.num_expiries(), 0);
        assert!(dist.probability_above(100_000.0, now_ms + 1000000).is_none());
    }
}

