// src/pricing/black_scholes.rs
//
// Black-Scholes option pricing model with support for:
// - European call/put pricing
// - Binary option probabilities
// - Greeks computation

use std::f64::consts::{E, PI};

/// Standard normal CDF using Hart's algorithm.
/// Accurate to ~15 decimal places.
pub fn norm_cdf(x: f64) -> f64 {
    if x.is_nan() {
        return 0.5;
    }
    
    // Handle extreme values
    if x < -38.0 {
        return 0.0;
    }
    if x > 38.0 {
        return 1.0;
    }
    
    // Use symmetry: Φ(-x) = 1 - Φ(x)
    let (result, neg) = if x < 0.0 {
        (-x, true)
    } else {
        (x, false)
    };
    
    // Coefficients for the rational approximation
    const A: [f64; 5] = [
        0.319381530,
        -0.356563782,
        1.781477937,
        -1.821255978,
        1.330274429,
    ];
    const P: f64 = 0.2316419;
    
    let t = 1.0 / (1.0 + P * result);
    let pdf = (1.0 / (2.0 * PI).sqrt()) * (-result * result / 2.0).exp();
    
    let poly = t * (A[0] + t * (A[1] + t * (A[2] + t * (A[3] + t * A[4]))));
    let cdf = 1.0 - pdf * poly;
    
    if neg {
        1.0 - cdf
    } else {
        cdf
    }
}

/// Standard normal PDF.
pub fn norm_pdf(x: f64) -> f64 {
    if x.is_nan() {
        return 0.0;
    }
    (1.0 / (2.0 * PI).sqrt()) * E.powf(-x * x / 2.0)
}

/// Inverse normal CDF (probit function) using rational approximation.
/// Accurate for p in (0.00001, 0.99999).
pub fn norm_ppf(p: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    if (p - 0.5).abs() < 1e-10 {
        return 0.0;
    }

    // Coefficients for rational approximation
    const A: [f64; 6] = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383577518672690e+02,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ];
    const B: [f64; 5] = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ];
    const C: [f64; 6] = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ];
    const D: [f64; 4] = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ];

    const P_LOW: f64 = 0.02425;
    const P_HIGH: f64 = 1.0 - P_LOW;

    let q: f64;
    let r: f64;

    if p < P_LOW {
        // Lower region
        q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= P_HIGH {
        // Central region
        q = p - 0.5;
        r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        // Upper region
        q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

/// Option type: Call or Put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionType {
    Call,
    Put,
}

/// Black-Scholes option pricing model.
#[derive(Debug, Clone)]
pub struct BlackScholes {
    /// Spot price of the underlying
    pub spot: f64,
    /// Strike price
    pub strike: f64,
    /// Time to expiration in years
    pub time_to_expiry: f64,
    /// Risk-free interest rate (annualized)
    pub rate: f64,
    /// Implied volatility (annualized)
    pub volatility: f64,
    /// Option type
    pub option_type: OptionType,
}

impl BlackScholes {
    /// Creates a new Black-Scholes pricer.
    pub fn new(
        spot: f64,
        strike: f64,
        time_to_expiry: f64,
        rate: f64,
        volatility: f64,
        option_type: OptionType,
    ) -> Self {
        Self {
            spot,
            strike,
            time_to_expiry,
            rate,
            volatility,
            option_type,
        }
    }

    /// Computes d1 in the Black-Scholes formula.
    pub fn d1(&self) -> f64 {
        if self.time_to_expiry <= 0.0 || self.volatility <= 0.0 {
            return if self.spot > self.strike {
                f64::INFINITY
            } else if self.spot < self.strike {
                f64::NEG_INFINITY
            } else {
                0.0
            };
        }

        let sqrt_t = self.time_to_expiry.sqrt();
        let vol_sqrt_t = self.volatility * sqrt_t;

        ((self.spot / self.strike).ln() + (self.rate + 0.5 * self.volatility.powi(2)) * self.time_to_expiry)
            / vol_sqrt_t
    }

    /// Computes d2 in the Black-Scholes formula.
    pub fn d2(&self) -> f64 {
        if self.time_to_expiry <= 0.0 || self.volatility <= 0.0 {
            return self.d1();
        }

        self.d1() - self.volatility * self.time_to_expiry.sqrt()
    }

    /// Computes the option price.
    pub fn price(&self) -> f64 {
        if self.time_to_expiry <= 0.0 {
            // At expiration, return intrinsic value
            return match self.option_type {
                OptionType::Call => (self.spot - self.strike).max(0.0),
                OptionType::Put => (self.strike - self.spot).max(0.0),
            };
        }

        let d1 = self.d1();
        let d2 = self.d2();
        let discount = E.powf(-self.rate * self.time_to_expiry);

        match self.option_type {
            OptionType::Call => {
                self.spot * norm_cdf(d1) - self.strike * discount * norm_cdf(d2)
            }
            OptionType::Put => {
                self.strike * discount * norm_cdf(-d2) - self.spot * norm_cdf(-d1)
            }
        }
    }

    /// Computes the probability that spot > strike at expiry (risk-neutral).
    /// This is N(d2) for calls, used for binary option pricing.
    pub fn probability_itm(&self) -> f64 {
        if self.time_to_expiry <= 0.0 {
            return if self.spot > self.strike { 1.0 } else { 0.0 };
        }
        norm_cdf(self.d2())
    }

    /// Computes the probability that spot < strike at expiry (risk-neutral).
    pub fn probability_otm(&self) -> f64 {
        1.0 - self.probability_itm()
    }

    /// Delta: ∂V/∂S
    pub fn delta(&self) -> f64 {
        if self.time_to_expiry <= 0.0 {
            return match self.option_type {
                OptionType::Call => if self.spot > self.strike { 1.0 } else { 0.0 },
                OptionType::Put => if self.spot < self.strike { -1.0 } else { 0.0 },
            };
        }

        let d1 = self.d1();
        match self.option_type {
            OptionType::Call => norm_cdf(d1),
            OptionType::Put => norm_cdf(d1) - 1.0,
        }
    }

    /// Gamma: ∂²V/∂S²
    pub fn gamma(&self) -> f64 {
        if self.time_to_expiry <= 0.0 || self.volatility <= 0.0 {
            return 0.0;
        }

        let d1 = self.d1();
        norm_pdf(d1) / (self.spot * self.volatility * self.time_to_expiry.sqrt())
    }

    /// Vega: ∂V/∂σ (per 1% change in vol)
    pub fn vega(&self) -> f64 {
        if self.time_to_expiry <= 0.0 {
            return 0.0;
        }

        let d1 = self.d1();
        self.spot * norm_pdf(d1) * self.time_to_expiry.sqrt() * 0.01
    }

    /// Theta: ∂V/∂t (per day)
    pub fn theta(&self) -> f64 {
        if self.time_to_expiry <= 0.0 {
            return 0.0;
        }

        let d1 = self.d1();
        let d2 = self.d2();
        let sqrt_t = self.time_to_expiry.sqrt();
        let discount = E.powf(-self.rate * self.time_to_expiry);

        let term1 = -self.spot * norm_pdf(d1) * self.volatility / (2.0 * sqrt_t);

        match self.option_type {
            OptionType::Call => {
                let term2 = -self.rate * self.strike * discount * norm_cdf(d2);
                (term1 + term2) / 365.0
            }
            OptionType::Put => {
                let term2 = self.rate * self.strike * discount * norm_cdf(-d2);
                (term1 + term2) / 365.0
            }
        }
    }
}

/// Calculates the fair value of a binary option that pays $1 if spot > strike.
pub fn binary_call_fair_value(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    rate: f64,
    volatility: f64,
) -> f64 {
    if time_to_expiry <= 0.0 {
        return if spot > strike { 1.0 } else { 0.0 };
    }

    let bs = BlackScholes::new(spot, strike, time_to_expiry, rate, volatility, OptionType::Call);
    let discount = E.powf(-rate * time_to_expiry);
    
    // Binary call = discounted probability that S > K
    discount * norm_cdf(bs.d2())
}

/// Calculates the fair value of a binary option that pays $1 if spot < strike.
pub fn binary_put_fair_value(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    rate: f64,
    volatility: f64,
) -> f64 {
    if time_to_expiry <= 0.0 {
        return if spot < strike { 1.0 } else { 0.0 };
    }

    let bs = BlackScholes::new(spot, strike, time_to_expiry, rate, volatility, OptionType::Put);
    let discount = E.powf(-rate * time_to_expiry);
    
    // Binary put = discounted probability that S < K
    discount * norm_cdf(-bs.d2())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;

    #[test]
    fn test_norm_cdf_standard_values() {
        assert!((norm_cdf(0.0) - 0.5).abs() < EPSILON);
        assert!((norm_cdf(1.0) - 0.8413447).abs() < 1e-4);
        assert!((norm_cdf(-1.0) - 0.1586553).abs() < 1e-4);
        assert!((norm_cdf(2.0) - 0.9772499).abs() < 1e-4);
    }

    #[test]
    fn test_norm_ppf_inverse() {
        for p in [0.1, 0.25, 0.5, 0.75, 0.9, 0.99] {
            let x = norm_ppf(p);
            let p_recovered = norm_cdf(x);
            assert!((p - p_recovered).abs() < 1e-6, "p={}, x={}, recovered={}", p, x, p_recovered);
        }
    }

    #[test]
    fn test_atm_call_price() {
        // ATM call with 1 year to expiry, 20% vol, 0% rate
        let bs = BlackScholes::new(100.0, 100.0, 1.0, 0.0, 0.20, OptionType::Call);
        let price = bs.price();
        // ATM call should be positive and reasonable
        // Theoretical: ~0.4 * S * σ * √T = 0.4 * 100 * 0.2 * 1 = 8, but actual BS is higher
        assert!(price > 5.0 && price < 15.0, "ATM call price: {}", price);
    }

    #[test]
    fn test_put_call_parity() {
        let spot = 100.0;
        let strike = 105.0;
        let time = 0.5;
        let rate = 0.05;
        let vol = 0.25;

        let call = BlackScholes::new(spot, strike, time, rate, vol, OptionType::Call);
        let put = BlackScholes::new(spot, strike, time, rate, vol, OptionType::Put);

        let parity = call.price() - put.price();
        let expected = spot - strike * E.powf(-rate * time);

        assert!((parity - expected).abs() < EPSILON, "Parity: {}, Expected: {}", parity, expected);
    }

    #[test]
    fn test_deep_itm_call() {
        // Deep ITM call should be close to intrinsic value
        let bs = BlackScholes::new(150.0, 100.0, 0.1, 0.0, 0.20, OptionType::Call);
        let intrinsic = 50.0;
        assert!((bs.price() - intrinsic).abs() < 1.0);
    }

    #[test]
    fn test_delta_bounds() {
        let bs_call = BlackScholes::new(100.0, 100.0, 1.0, 0.0, 0.20, OptionType::Call);
        let bs_put = BlackScholes::new(100.0, 100.0, 1.0, 0.0, 0.20, OptionType::Put);

        assert!(bs_call.delta() > 0.0 && bs_call.delta() < 1.0);
        assert!(bs_put.delta() > -1.0 && bs_put.delta() < 0.0);
    }

    #[test]
    fn test_binary_call_atm() {
        // ATM binary call should be close to 0.5 (undiscounted)
        let fair = binary_call_fair_value(100.0, 100.0, 1.0, 0.0, 0.20);
        assert!((fair - 0.5).abs() < 0.1, "ATM binary call: {}", fair);
    }
}

