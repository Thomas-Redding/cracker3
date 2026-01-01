// src/pricing/vol_time.rs
//
// Volatility-weighted time calculation for improved interpolation.
// Accounts for intraday/weekly volatility patterns and regime changes.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

/// Trait for calculating volatility-weighted time between timestamps.
/// 
/// Vol-weighted time adjusts calendar time based on historical volatility patterns.
/// This allows interpolation to correctly account for periods of higher/lower volatility
/// (e.g., market hours vs weekends, FOMC announcements, etc.)
pub trait VolTimeStrategy: Send + Sync {
    /// Calculates the total "vol-weighted seconds" between two timestamps.
    /// 
    /// This converts calendar time to volatility-weighted time.
    /// 
    /// # Returns
    /// The vol-weighted time in seconds. This can be converted to years by dividing
    /// by the number of seconds in a year (365.25 * 24 * 3600).
    fn get_vol_time(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> f64;
    
    /// Converts vol-weighted seconds to years.
    /// 
    /// Useful for converting the output of `get_vol_time` to a time-to-expiry
    /// that can be used with existing pricing models.
    fn vol_seconds_to_years(&self, vol_seconds: f64) -> f64 {
        vol_seconds / (365.25 * 24.0 * 3600.0)
    }
    
    /// Converts years to vol-weighted seconds.
    /// 
    /// This is a helper for converting calendar time-to-expiry to vol-weighted time.
    fn years_to_vol_seconds(&self, years: f64) -> f64 {
        years * 365.25 * 24.0 * 3600.0
    }
}

/// Simple calendar-based vol time strategy.
/// Uses uniform calendar time (no volatility weighting).
pub struct CalendarVolTimeStrategy;

impl VolTimeStrategy for CalendarVolTimeStrategy {
    fn get_vol_time(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> f64 {
        if start >= end {
            return 0.0;
        }
        (end - start).num_seconds() as f64
    }
}

/// Weighted volatility time strategy.
/// 
/// Accounts for:
/// - Intraday/weekly volatility patterns (e.g., higher vol during market hours)
/// - Regime changes (GARCH-lite scaling)
/// - Event-specific overrides (e.g., FOMC announcements)
pub struct WeightedVolTimeStrategy {
    /// Weights for each hour of the week (0..167)
    /// Monday 00:00 = 0, Sunday 23:00 = 167
    hourly_weights: Vec<f64>,
    
    /// Recent volatility / Long-term average (GARCH-lite)
    regime_scaler: f64,
    
    /// Specific windows (Start, End, Multiplier)
    event_overrides: Vec<(DateTime<Utc>, DateTime<Utc>, f64)>,
}

impl WeightedVolTimeStrategy {
    /// Creates a new WeightedVolTimeStrategy.
    /// 
    /// # Arguments
    /// * `historical_vols` - Raw hourly vols from Deribit (should be 168 values for full week)
    /// * `regime_scaler` - Recent volatility / Long-term average (typically 0.5-2.0)
    /// * `event_overrides` - Specific time windows with custom multipliers
    pub fn new(
        historical_vols: Vec<f64>,
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
}

impl VolTimeStrategy for WeightedVolTimeStrategy {
    fn get_vol_time(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> f64 {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calendar_strategy() {
        let strategy = CalendarVolTimeStrategy;
        
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = strategy.get_vol_time(start, end);
        // Should be exactly 3600 seconds (1 hour)
        assert!((vol_time - 3600.0).abs() < 1.0);
        
        let years = strategy.vol_seconds_to_years(vol_time);
        let hours = years * 365.25 * 24.0;
        assert!((hours - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_uniform_weights() {
        // Uniform weights should give roughly calendar time
        let uniform_vols = vec![1.0; 168];
        let calc = WeightedVolTimeStrategy::new(uniform_vols, 1.0, vec![]);
        
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
        let calc = WeightedVolTimeStrategy::new(uniform_vols, 2.0, vec![]);
        
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
        
        let calc = WeightedVolTimeStrategy::new(
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
        
        let calc = WeightedVolTimeStrategy::new(vols, 1.0, vec![]);
        
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
        let calc = WeightedVolTimeStrategy::new(vec![], 1.0, vec![]);
        
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
        let strategy = CalendarVolTimeStrategy;
        let seconds_per_year = 365.25 * 24.0 * 3600.0;
        let years = strategy.vol_seconds_to_years(seconds_per_year);
        assert!((years - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_years_to_vol_seconds() {
        let strategy = CalendarVolTimeStrategy;
        let seconds = strategy.years_to_vol_seconds(1.0);
        let expected = 365.25 * 24.0 * 3600.0;
        assert!((seconds - expected).abs() < 1e-6);
    }

    #[test]
    fn test_start_equals_end() {
        let strategy = CalendarVolTimeStrategy;
        let time = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = strategy.get_vol_time(time, time);
        assert!((vol_time - 0.0).abs() < 1e-10);
        
        // Also test weighted strategy
        let weighted = WeightedVolTimeStrategy::new(vec![1.0; 168], 1.0, vec![]);
        let vol_time_weighted = weighted.get_vol_time(time, time);
        assert!((vol_time_weighted - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_start_after_end_returns_zero() {
        let strategy = CalendarVolTimeStrategy;
        let start = DateTime::parse_from_rfc3339("2024-01-02T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = strategy.get_vol_time(start, end);
        assert!((vol_time - 0.0).abs() < 1e-10);
        
        // Also test weighted strategy
        let weighted = WeightedVolTimeStrategy::new(vec![1.0; 168], 1.0, vec![]);
        let vol_time_weighted = weighted.get_vol_time(start, end);
        assert!((vol_time_weighted - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_multi_day_span() {
        // Create weights where weekdays have 2x vol and weekends have 0.5x vol
        let mut vols = vec![0.0; 168];
        // Monday-Friday (0-119): higher vol
        for i in 0..120 {
            vols[i] = 2.0;
        }
        // Saturday-Sunday (120-167): lower vol
        for i in 120..168 {
            vols[i] = 0.5;
        }
        
        let calc = WeightedVolTimeStrategy::new(vols, 1.0, vec![]);
        
        // Monday 00:00 to Sunday 23:59 (full week)
        let monday = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sunday_end = DateTime::parse_from_rfc3339("2024-01-07T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(monday, sunday_end);
        let calendar_seconds = (sunday_end - monday).num_seconds() as f64;
        
        // Vol time should be different from calendar time due to weighting
        // With normalized weights, total should be roughly calendar time
        // (since mean of weights is normalized to 1.0)
        assert!(vol_time > 0.0);
        // Should be approximately calendar time since weights are normalized
        assert!((vol_time - calendar_seconds).abs() / calendar_seconds < 0.1);
    }

    #[test]
    fn test_partial_hour_handling() {
        let uniform_vols = vec![1.0; 168];
        let calc = WeightedVolTimeStrategy::new(uniform_vols, 1.0, vec![]);
        
        // Test 30 minutes
        let start = DateTime::parse_from_rfc3339("2024-01-01T10:15:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T10:45:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(start, end);
        // Should be 30 minutes = 1800 seconds
        assert!((vol_time - 1800.0).abs() < 1.0);
    }

    #[test]
    fn test_crossing_hour_boundary() {
        // Create weights where hour 10 has weight 1.0 and hour 11 has weight 2.0
        let mut vols = vec![1.0; 168];
        vols[10] = 1.0; // Monday 10:00 - hour index for Monday
        vols[11] = 2.0; // Monday 11:00
        
        let calc = WeightedVolTimeStrategy::new(vols, 1.0, vec![]);
        
        // 10:30 to 11:30 - crosses hour boundary
        let start = DateTime::parse_from_rfc3339("2024-01-01T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T11:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(start, end);
        
        // 30 mins at weight ~1.0 + 30 mins at weight ~2.0
        // With normalization, weights are adjusted, but the relative difference should show
        // Total calendar time is 3600 seconds
        // The weighted time should reflect the higher weight in the second half
        assert!(vol_time > 0.0);
        assert!(vol_time != 3600.0); // Should differ from uniform
    }

    #[test]
    fn test_event_override_partial_overlap() {
        let uniform_vols = vec![1.0; 168];
        
        // Event from 10:00-11:00 with 5x multiplier
        let event_start = DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let event_end = DateTime::parse_from_rfc3339("2024-01-01T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let calc = WeightedVolTimeStrategy::new(
            uniform_vols,
            1.0,
            vec![(event_start, event_end, 5.0)],
        );
        
        // Query 09:30-10:30 - only 30 mins overlap with event
        let query_start = DateTime::parse_from_rfc3339("2024-01-01T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let query_end = DateTime::parse_from_rfc3339("2024-01-01T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(query_start, query_end);
        
        // 30 mins at weight 1.0 = 1800 seconds
        // 30 mins at weight 5.0 = 1800 * 5 = 9000 seconds
        // Total = 10800 seconds (but weights are normalized, so this is approximate)
        assert!(vol_time > 3600.0); // Should be more than calendar time
    }

    #[test]
    fn test_multiple_event_overrides() {
        let uniform_vols = vec![1.0; 168];
        
        // Two events with different multipliers
        let event1_start = DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let event1_end = DateTime::parse_from_rfc3339("2024-01-01T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let event2_start = DateTime::parse_from_rfc3339("2024-01-01T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let event2_end = DateTime::parse_from_rfc3339("2024-01-01T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let calc = WeightedVolTimeStrategy::new(
            uniform_vols,
            1.0,
            vec![
                (event1_start, event1_end, 3.0),
                (event2_start, event2_end, 2.0),
            ],
        );
        
        // Query covers both events
        let query_start = DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let query_end = DateTime::parse_from_rfc3339("2024-01-01T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let vol_time = calc.get_vol_time(query_start, query_end);
        
        // 1 hour at 3x + 3 hours at 1x + 1 hour at 2x = 3 + 3 + 2 = 8 hours worth
        // Calendar time is 5 hours = 18000 seconds
        // Expected vol time ≈ 8 * 3600 = 28800 seconds
        let calendar_seconds = 5.0 * 3600.0;
        assert!(vol_time > calendar_seconds);
    }
}
