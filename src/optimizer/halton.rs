// src/optimizer/halton.rs
//
// Low-discrepancy sequence generators for quasi-Monte Carlo simulation.
// Provides sequences for more efficient sampling than pseudo-random numbers.

use crate::pricing::black_scholes::norm_ppf;

/// Prime numbers for Halton sequence bases.
const PRIMES: [u32; 21] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73];

/// Maximum dimensions supported.
pub const MAX_DIMENSIONS: usize = 21;

/// Low-discrepancy sequence generator using Halton sequences.
/// 
/// Generates quasi-random sequences that are more uniformly distributed
/// than pseudo-random numbers, providing better coverage for Monte Carlo.
#[derive(Debug, Clone)]
pub struct HaltonGenerator {
    dimensions: usize,
    /// Current sequence index
    index: u64,
    /// Bases for each dimension (prime numbers)
    bases: Vec<u32>,
}

impl HaltonGenerator {
    /// Creates a new generator for the given number of dimensions.
    /// 
    /// # Panics
    /// Panics if dimensions > MAX_DIMENSIONS (21).
    pub fn new(dimensions: usize) -> Self {
        assert!(dimensions > 0 && dimensions <= MAX_DIMENSIONS, 
            "Supports 1-{} dimensions, got {}", MAX_DIMENSIONS, dimensions);

        let bases = PRIMES[..dimensions].to_vec();

        Self {
            dimensions,
            index: 0,
            bases,
        }
    }

    /// Computes the Halton value for a given index and base.
    fn halton(index: u64, base: u32) -> f64 {
        let mut result = 0.0;
        let mut f = 1.0 / base as f64;
        let mut i = index;
        
        while i > 0 {
            result += f * (i % base as u64) as f64;
            i /= base as u64;
            f /= base as f64;
        }
        
        result
    }

    /// Generates the next point in [0, 1]^d.
    pub fn next(&mut self) -> Vec<f64> {
        self.index += 1;
        
        self.bases.iter()
            .map(|&base| Self::halton(self.index, base))
            .collect()
    }

    /// Generates n points in [0, 1]^d.
    pub fn generate(&mut self, n: usize) -> Vec<Vec<f64>> {
        (0..n).map(|_| self.next()).collect()
    }

    /// Generates n points transformed to standard normal (via inverse CDF).
    pub fn generate_normal(&mut self, n: usize) -> Vec<Vec<f64>> {
        self.generate(n)
            .into_iter()
            .map(|point| {
                point.into_iter()
                    .map(|u| norm_ppf(u.clamp(0.0001, 0.9999)))
                    .collect()
            })
            .collect()
    }

    /// Resets the generator to start from the beginning.
    pub fn reset(&mut self) {
        self.index = 0;
    }

    /// Skips ahead n points in the sequence.
    pub fn skip(&mut self, n: usize) {
        self.index += n as u64;
    }

    /// Returns the current sequence index.
    pub fn current_index(&self) -> u32 {
        self.index as u32
    }

    /// Returns the number of dimensions.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
}

/// Generates correlated price scenarios using Brownian Bridge construction.
/// 
/// # Arguments
/// * `n_scenarios` - Number of scenarios to generate
/// * `expiries` - Sorted list of (expiry_timestamp, time_to_expiry, ppf_func)
/// 
/// # Returns
/// Vec of scenarios, each containing prices at each expiry
pub fn generate_correlated_scenarios<F>(
    n_scenarios: usize,
    expiries: &[(i64, f64, F)],
) -> Vec<Vec<f64>>
where
    F: Fn(f64) -> f64,
{
    if expiries.is_empty() {
        return vec![];
    }

    let n_expiries = expiries.len();
    let mut generator = HaltonGenerator::new(n_expiries.min(MAX_DIMENSIONS));
    
    // Generate points and transform to normal
    let z_points = generator.generate_normal(n_scenarios);
    
    let mut scenarios = Vec::with_capacity(n_scenarios);

    for z_scenario in z_points {
        let mut prices = Vec::with_capacity(n_expiries);
        let mut cumulative_w = 0.0;

        for (i, (_expiry_ts, time_to_expiry, ppf)) in expiries.iter().enumerate() {
            // Brownian bridge construction
            let dt = if i == 0 {
                *time_to_expiry
            } else {
                time_to_expiry - expiries[i - 1].1
            };

            // Increment Brownian motion
            let dw = z_scenario[i] * dt.sqrt();
            cumulative_w += dw;

            // Marginal distribution at this expiry
            let z_marginal = cumulative_w / time_to_expiry.sqrt();
            let percentile = crate::pricing::black_scholes::norm_cdf(z_marginal);
            
            // Convert to price via PPF
            let price = ppf(percentile);
            prices.push(price);
        }

        scenarios.push(prices);
    }

    scenarios
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_halton_uniform_coverage() {
        let mut generator = HaltonGenerator::new(2);
        let points = generator.generate(1000);

        // Check all points are in [0, 1]
        for point in &points {
            for &coord in point {
                assert!(coord >= 0.0 && coord <= 1.0, "Point out of range: {}", coord);
            }
        }

        // Check reasonable coverage (low discrepancy)
        let mut count_quadrants = [0; 4];
        for point in &points {
            let quadrant = if point[0] < 0.5 {
                if point[1] < 0.5 { 0 } else { 1 }
            } else {
                if point[1] < 0.5 { 2 } else { 3 }
            };
            count_quadrants[quadrant] += 1;
        }

        // Halton sequences should give good coverage - each quadrant should have ~250 points
        for (i, count) in count_quadrants.iter().enumerate() {
            assert!(*count > 150 && *count < 350, "Quadrant {} count: {}", i, count);
        }
    }

    #[test]
    fn test_halton_dimensions() {
        let mut generator = HaltonGenerator::new(5);
        let point = generator.next();
        assert_eq!(point.len(), 5);
    }

    #[test]
    fn test_halton_normal_transform() {
        let mut generator = HaltonGenerator::new(2);
        let points = generator.generate_normal(1000);

        // Check points are approximately standard normal
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let n = (points.len() * 2) as f64;

        for point in &points {
            for &x in point {
                sum += x;
                sum_sq += x * x;
            }
        }

        let mean = sum / n;
        let variance = sum_sq / n - mean * mean;

        // Mean should be close to 0, variance close to 1
        assert!(mean.abs() < 0.1, "Mean: {}", mean);
        assert!((variance - 1.0).abs() < 0.2, "Variance: {}", variance);
    }

    #[test]
    fn test_correlated_scenarios() {
        // Simple test with linear PPF
        let expiries: Vec<(i64, f64, fn(f64) -> f64)> = vec![
            (1000, 0.25, |p: f64| p * 100_000.0),
            (2000, 0.50, |p: f64| p * 100_000.0),
        ];

        let scenarios = generate_correlated_scenarios(100, &expiries);

        assert_eq!(scenarios.len(), 100);
        for scenario in &scenarios {
            assert_eq!(scenario.len(), 2);
            // Prices should be positive
            assert!(scenario[0] > 0.0);
            assert!(scenario[1] > 0.0);
        }
    }

    #[test]
    fn test_halton_sequence_values() {
        // Test known Halton values
        // H(1, 2) = 1/2
        // H(2, 2) = 1/4
        // H(3, 2) = 3/4
        let mut gen = HaltonGenerator::new(1);
        
        let p1 = gen.next();
        assert!((p1[0] - 0.5).abs() < 1e-10, "H(1,2) = {}", p1[0]);
        
        let p2 = gen.next();
        assert!((p2[0] - 0.25).abs() < 1e-10, "H(2,2) = {}", p2[0]);
        
        let p3 = gen.next();
        assert!((p3[0] - 0.75).abs() < 1e-10, "H(3,2) = {}", p3[0]);
    }
}
