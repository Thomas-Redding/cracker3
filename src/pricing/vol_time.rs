// src/pricing/vol_time.rs
//
// Volatility-weighted time calculation for improved interpolation.
// Accounts for intraday/weekly volatility patterns and regime changes.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

/// Calculates volatility-weighted time between timestamps.
/// 
/// This improves interpolation logic by accounting for:
/// - Intraday/weekly volatility patterns (e.g., higher vol during market hours)
/// - Regime changes (GARCH-lite scaling)
/// - Event-specific overrides (e.g., FOMC announcements)
pub struct VolTimeCalculator {
    /// Weights for each hour of the week (0..167)
    /// Monday 00:00 = 0, Sunday 23:00 = 167
    hourly_weights: Vec<f64>,
    
    /// Recent volatility / Long-term average (GARCH-lite)
    regime_scaler: f64,
    
    /// Specific windows (Start, End, Multiplier)
    event_overrides: Vec<(DateTime<Utc>, DateTime<Utc>, f64)>,
}

impl VolTimeCalculator {
    /// Creates a new VolTimeCalculator.
    /// 
    /// # Arguments
    /// * `historical_vols` - Raw hourly vols from Deribit (should be 168 values for full week)
    /// * `regime_scaler` - Recent volatility / Long-term average (typically 0.5-2.0)
    /// * `event_overrides` - Specific time windows with custom multipliers
    pub fn new(
        historical_vols: Vec<f64>, // Raw hourly vols from Deribit
        regime_scaler: f64,
        event_overrides: Vec<(DateTime<Utc>, DateTime<Utc>, f64)>,
    ) -> Self {
        // If we have data, we normalize the hourly_weights so the mean is 1.0
        // This ensures 'weighted seconds' are roughly comparable to 'calendar seconds'
        let mean_vol: f64 = if historical_vols.is_empty() {
            1.0
        } else {
            historical_vols.iter().sum::<f64>() / historical_vols.len() as f64
        };
        
        let normalized_weights = if mean_vol > 1e-10 {
            historical_vols.iter().map(|v| v / mean_vol).collect()
        } else {
            // Fallback: uniform weights if mean is too small
            vec![1.0; historical_vols.len()]
        };

        Self {
            hourly_weights: normalized_weights,
            regime_scaler,
            event_overrides,
        }
    }

    /// Calculates the total "vol-weighted seconds" between two timestamps.
    /// 
    /// This converts calendar time to volatility-weighted time, accounting for
    /// intraday patterns, regime changes, and event overrides.
    /// 
    /// # Returns
    /// The vol-weighted time in seconds. This can be converted to years by dividing
    /// by the number of seconds in a year (365.25 * 24 * 3600).
    pub fn get_vol_time(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> f64 {
        if start >= end {
            return 0.0;
        }

        let mut total_weighted_seconds = 0.0;
        let mut current_cursor = start;
        
        // We iterate in 1-hour chunks for performance, 
        // then handle the remainder minutes/seconds.
        while current_cursor < end {
            let hour_end = (current_cursor + Duration::hours(1))
                .with_minute(0).unwrap()
                .with_second(0).unwrap()
                .with_nanosecond(0).unwrap();
            
            let chunk_end = if hour_end < end { hour_end } else { end };
            let duration_secs = (chunk_end - current_cursor).num_seconds() as f64;
            
            let weight = self.get_weight_at(current_cursor);
            total_weighted_seconds += duration_secs * weight * self.regime_scaler;
            
            current_cursor = chunk_end;
        }

        total_weighted_seconds
    }

    /// Gets the volatility weight at a specific timestamp.
    /// 
    /// Priority order:
    /// 1. Event overrides (highest priority)
    /// 2. Hourly weights based on day of week and hour
    /// 3. Default weight of 1.0 if no data available
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

        *self.hourly_weights.get(hour_idx).unwrap_or(&1.0)
    }

    /// Converts vol-weighted seconds to years.
    /// 
    /// Useful for converting the output of `get_vol_time` to a time-to-expiry
    /// that can be used with existing pricing models.
    pub fn vol_seconds_to_years(vol_seconds: f64) -> f64 {
        vol_seconds / (365.25 * 24.0 * 3600.0)
    }

    /// Converts years to vol-weighted seconds.
    /// 
    /// This is a helper for converting calendar time-to-expiry to vol-weighted time.
    pub fn years_to_vol_seconds(years: f64) -> f64 {
        years * 365.25 * 24.0 * 3600.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_weights() {
        // Uniform weights should give roughly calendar time
        let uniform_vols = vec![1.0; 168];
        let calc = VolTimeCalculator::new(uniform_vols, 1.0, vec![]);
        
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(start, end);
        // Should be approximately 3600 seconds (1 hour)
        assert!((vol_time - 3600.0).abs() < 1.0);
    }

    #[test]
    fn test_regime_scaler() {
        let uniform_vols = vec![1.0; 168];
        let calc = VolTimeCalculator::new(uniform_vols, 2.0, vec![]);
        
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(start, end);
        // Should be 2x calendar time
        assert!((vol_time - 7200.0).abs() < 1.0);
    }

    #[test]
    fn test_event_override() {
        let uniform_vols = vec![1.0; 168];
        let start_time = DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end_time = DateTime::parse_from_rfc3339("2024-01-01T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let calc = VolTimeCalculator::new(
            uniform_vols,
            1.0,
            vec![(start_time, end_time, 3.0)],
        );
        
        let vol_time = calc.get_vol_time(start_time, end_time);
        // Should be 3x calendar time due to override
        assert!((vol_time - 10800.0).abs() < 1.0);
    }

    #[test]
    fn test_hourly_weights() {
        // Higher weight for Monday 9am (index 9)
        let mut vols = vec![1.0; 168];
        vols[9] = 2.0; // Monday 9am
        
        let calc = VolTimeCalculator::new(vols, 1.0, vec![]);
        
        let monday_9am = DateTime::parse_from_rfc3339("2024-01-01T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let monday_10am = DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(monday_9am, monday_10am);
        // Should be approximately 2x calendar time (normalized weight)
        assert!(vol_time > 6000.0 && vol_time < 8000.0);
    }

    #[test]
    fn test_empty_weights() {
        let calc = VolTimeCalculator::new(vec![], 1.0, vec![]);
        
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(start, end);
        // Should default to calendar time
        assert!((vol_time - 3600.0).abs() < 1.0);
    }

    #[test]
    fn test_vol_seconds_to_years() {
        let seconds_per_year = 365.25 * 24.0 * 3600.0;
        let years = VolTimeCalculator::vol_seconds_to_years(seconds_per_year);
        assert!((years - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_years_to_vol_seconds() {
        let seconds = VolTimeCalculator::years_to_vol_seconds(1.0);
        let expected = 365.25 * 24.0 * 3600.0;
        assert!((seconds - expected).abs() < 1e-6);
    }
}

