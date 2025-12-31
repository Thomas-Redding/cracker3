// src/pricing/vol_time.rs
//
// Volatility-weighted time calculator.
// Converts calendar time to "vol-weighted seconds" based on historical intraday/weekly patterns.
//
// Key features:
// - Hourly weights for each hour of the week (168 hours total)
// - Regime scaler for recent vs long-term volatility (GARCH-lite)
// - Event overrides for specific time windows (e.g., FOMC announcements)

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

/// Calculates volatility-weighted time instead of calendar time.
/// 
/// This is useful for option pricing when volatility varies significantly
/// throughout the week (e.g., higher volatility during US trading hours,
/// lower volatility on weekends).
/// 
/// Example:
/// ```
/// use chrono::Utc;
/// use trading_bot::pricing::vol_time::VolTimeCalculator;
/// 
/// let historical_vols = vec![0.5; 168]; // Flat 50% vol for all hours
/// let calculator = VolTimeCalculator::new(historical_vols, 1.0, vec![]);
/// 
/// let start = Utc::now();
/// let end = start + chrono::Duration::hours(24);
/// let vol_time = calculator.get_vol_time(start, end);
/// ```
pub struct VolTimeCalculator {
    /// Weights for each hour of the week (0..167)
    /// Monday 00:00 = 0, Sunday 23:00 = 167
    /// Normalized so mean = 1.0 (ensures vol-weighted seconds ≈ calendar seconds on average)
    hourly_weights: Vec<f64>,
    
    /// Recent volatility / Long-term average (GARCH-lite)
    /// 1.0 = normal regime, >1.0 = high vol regime, <1.0 = low vol regime
    regime_scaler: f64,
    
    /// Specific windows (Start, End, Multiplier)
    /// Checked first (highest priority), should be sorted by start time for efficiency
    event_overrides: Vec<(DateTime<Utc>, DateTime<Utc>, f64)>,
}

impl VolTimeCalculator {
    /// Creates a new volatility time calculator.
    /// 
    /// # Arguments
    /// 
    /// * `historical_vols` - Raw hourly volatilities from historical data (should be 168 values)
    /// * `regime_scaler` - Current regime multiplier (recent vol / long-term avg)
    /// * `event_overrides` - Specific time windows with custom multipliers
    /// 
    /// # Panics
    /// 
    /// Panics if `historical_vols` is empty (cannot normalize).
    pub fn new(
        historical_vols: Vec<f64>,
        regime_scaler: f64,
        event_overrides: Vec<(DateTime<Utc>, DateTime<Utc>, f64)>,
    ) -> Self {
        if historical_vols.is_empty() {
            panic!("historical_vols cannot be empty");
        }

        // Normalize hourly weights so the mean is 1.0
        // This ensures 'vol-weighted seconds' are roughly comparable to 'calendar seconds'
        let mean_vol: f64 = historical_vols.iter().sum::<f64>() / historical_vols.len() as f64;
        
        if mean_vol <= 0.0 {
            // Fallback: use uniform weights if mean is invalid
            let uniform_weight = 1.0;
            let normalized_weights = vec![uniform_weight; historical_vols.len()];
            return Self {
                hourly_weights: normalized_weights,
                regime_scaler,
                event_overrides,
            };
        }

        let normalized_weights: Vec<f64> = historical_vols
            .iter()
            .map(|v| v / mean_vol)
            .collect();

        Self {
            hourly_weights: normalized_weights,
            regime_scaler: regime_scaler.max(0.0), // Ensure non-negative
            event_overrides,
        }
    }

    /// Creates a default calculator with uniform weights (no intraday/weekly patterns).
    /// 
    /// Useful for testing or when historical data is unavailable.
    pub fn uniform(regime_scaler: f64) -> Self {
        Self {
            hourly_weights: vec![1.0; 168],
            regime_scaler: regime_scaler.max(0.0),
            event_overrides: vec![],
        }
    }

    /// Calculates the total "vol-weighted seconds" between two timestamps.
    /// 
    /// This iterates through the time period, splitting at hour boundaries and
    /// event override boundaries, applying the appropriate weight for each segment:
    /// 1. Event overrides (if any match)
    /// 2. Hourly weights (day of week + hour of day)
    /// 3. Regime scaler (applied globally)
    /// 
    /// Returns 0.0 if start >= end.
    pub fn get_vol_time(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> f64 {
        if start >= end {
            return 0.0;
        }

        let mut total_weighted_seconds = 0.0;
        let mut current_cursor = start;
        
        // Collect all boundaries (hour boundaries + event override boundaries)
        let mut boundaries = Vec::new();
        
        // Add hour boundaries
        let mut hour_boundary = (start + Duration::hours(1))
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        while hour_boundary < end {
            boundaries.push(hour_boundary);
            hour_boundary = hour_boundary + Duration::hours(1);
        }
        
        // Add event override boundaries
        for (override_start, override_end, _) in &self.event_overrides {
            if *override_start > start && *override_start < end {
                boundaries.push(*override_start);
            }
            if *override_end > start && *override_end < end {
                boundaries.push(*override_end);
            }
        }
        
        // Sort boundaries
        boundaries.sort();
        boundaries.push(end); // Add end as final boundary
        
        // Process each segment
        for &boundary in &boundaries {
            if boundary <= current_cursor {
                continue;
            }
            
            let duration_secs = (boundary - current_cursor).num_seconds() as f64;
            let weight = self.get_weight_at(current_cursor);
            total_weighted_seconds += duration_secs * weight * self.regime_scaler;
            
            current_cursor = boundary;
        }

        total_weighted_seconds
    }

    /// Converts vol-weighted seconds back to calendar seconds (approximate).
    /// 
    /// This is useful for comparing vol-weighted time to calendar time.
    /// The conversion assumes uniform weights, so it's approximate.
    pub fn vol_time_to_calendar_time(&self, vol_seconds: f64) -> f64 {
        vol_seconds / (self.regime_scaler * 1.0) // Simplified: assumes mean weight = 1.0
    }

    /// Gets the weight multiplier for a specific timestamp.
    /// 
    /// Priority order:
    /// 1. Event overrides (if timestamp falls within any override window)
    /// 2. Hourly weights (based on day of week and hour)
    /// 3. Default weight of 1.0 (if hourly_weights is empty or index out of bounds)
    fn get_weight_at(&self, dt: DateTime<Utc>) -> f64 {
        // 1. Check Event Overrides first (highest priority)
        for (start, end, mult) in &self.event_overrides {
            if dt >= *start && dt < *end {
                return *mult;
            }
        }

        // 2. Fallback to Hourly Map
        // Monday is 0 in chrono's weekday().num_days_from_monday()
        let day_idx = dt.weekday().num_days_from_monday();
        let hour_idx = (day_idx * 24 + dt.hour()) as usize;

        self.hourly_weights.get(hour_idx).copied().unwrap_or(1.0)
    }

    /// Updates the regime scaler (e.g., when new volatility data arrives).
    pub fn set_regime_scaler(&mut self, scaler: f64) {
        self.regime_scaler = scaler.max(0.0);
    }

    /// Returns the current regime scaler.
    pub fn regime_scaler(&self) -> f64 {
        self.regime_scaler
    }

    /// Adds an event override for a specific time window.
    pub fn add_event_override(&mut self, start: DateTime<Utc>, end: DateTime<Utc>, multiplier: f64) {
        self.event_overrides.push((start, end, multiplier));
        // Could sort here for efficiency, but keeping simple for now
    }

    /// Clears all event overrides.
    pub fn clear_event_overrides(&mut self) {
        self.event_overrides.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_calculator() {
        let calc = VolTimeCalculator::uniform(1.0);
        // Use exact timestamp to avoid rounding issues
        let start = Utc::now()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let end = start + Duration::hours(24);
        
        let vol_time = calc.get_vol_time(start, end);
        let calendar_time = (end - start).num_seconds() as f64;
        
        // With uniform weights and regime_scaler=1.0, should be approximately equal
        // Allow small tolerance for floating point precision
        assert!((vol_time - calendar_time).abs() < 10.0, "vol_time={}, calendar_time={}", vol_time, calendar_time);
    }

    #[test]
    fn test_regime_scaler() {
        let calc = VolTimeCalculator::uniform(2.0); // 2x regime
        // Use exact timestamp
        let start = Utc::now()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let end = start + Duration::hours(1);
        
        let vol_time = calc.get_vol_time(start, end);
        let calendar_time = (end - start).num_seconds() as f64;
        
        // Should be approximately 2x calendar time
        // Allow tolerance for rounding in hour boundary calculations
        assert!((vol_time - 2.0 * calendar_time).abs() < 10.0, "vol_time={}, expected={}", vol_time, 2.0 * calendar_time);
    }

    #[test]
    fn test_event_override() {
        let mut calc = VolTimeCalculator::uniform(1.0);
        // Use exact timestamp
        let start = Utc::now()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let override_start = start + Duration::minutes(30);
        let override_end = start + Duration::hours(1);
        let end = start + Duration::hours(2);
        
        // Add override: 2x multiplier for middle hour
        calc.add_event_override(override_start, override_end, 2.0);
        
        let vol_time = calc.get_vol_time(start, end);
        
        // First 30 min: 1.0x, next 30 min: 2.0x, last hour: 1.0x
        let expected = 30.0 * 60.0 * 1.0 + 30.0 * 60.0 * 2.0 + 60.0 * 60.0 * 1.0;
        
        // Allow tolerance for rounding in hour boundary calculations
        assert!((vol_time - expected).abs() < 100.0, "vol_time={}, expected={}", vol_time, expected);
    }

    #[test]
    fn test_hourly_weights() {
        // Create weights: higher for weekday hours 9-17 (US trading hours)
        let mut weights = vec![0.5; 168]; // Low base weight
        for day in 0..5 { // Monday-Friday
            for hour in 9..17 { // 9 AM - 5 PM
                weights[day * 24 + hour] = 2.0; // High weight
            }
        }
        
        let calc = VolTimeCalculator::new(weights, 1.0, vec![]);
        
        // Test Monday 10 AM (should have high weight)
        let monday_10am = Utc::now()
            .date_naive()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        // Adjust to Monday
        let days_until_monday = (monday_10am.weekday().num_days_from_monday() as i64) % 7;
        let monday = monday_10am - Duration::days(days_until_monday);
        let monday_10am = monday.with_hour(10).unwrap();
        
        let weight = calc.get_weight_at(monday_10am);
        assert!(weight > 1.5, "Expected high weight for trading hours, got {}", weight);
    }

    #[test]
    fn test_empty_range() {
        let calc = VolTimeCalculator::uniform(1.0);
        let start = Utc::now();
        let end = start;
        
        assert_eq!(calc.get_vol_time(start, end), 0.0);
    }

    #[test]
    fn test_reverse_range() {
        let calc = VolTimeCalculator::uniform(1.0);
        let start = Utc::now();
        let end = start - Duration::hours(1);
        
        assert_eq!(calc.get_vol_time(start, end), 0.0);
    }

    #[test]
    fn test_normalization() {
        // Test that normalization works correctly
        let historical_vols = vec![0.5, 1.0, 1.5, 2.0]; // Mean = 1.25
        let calc = VolTimeCalculator::new(historical_vols.clone(), 1.0, vec![]);
        
        // After normalization, mean should be 1.0
        let mean_weight: f64 = calc.hourly_weights.iter().sum::<f64>() / calc.hourly_weights.len() as f64;
        assert!((mean_weight - 1.0).abs() < 1e-10, "Mean weight should be 1.0, got {}", mean_weight);
    }

    #[test]
    #[should_panic(expected = "historical_vols cannot be empty")]
    fn test_empty_historical_vols() {
        VolTimeCalculator::new(vec![], 1.0, vec![]);
    }

    #[test]
    fn test_regime_scaler_update() {
        let mut calc = VolTimeCalculator::uniform(1.0);
        assert_eq!(calc.regime_scaler(), 1.0);
        
        calc.set_regime_scaler(1.5);
        assert_eq!(calc.regime_scaler(), 1.5);
        
        // Negative scaler should be clamped to 0
        calc.set_regime_scaler(-1.0);
        assert_eq!(calc.regime_scaler(), 0.0);
    }
}

