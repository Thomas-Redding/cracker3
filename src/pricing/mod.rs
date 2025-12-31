// src/pricing/mod.rs
//
// Pricing models and volatility surface construction.

pub mod black_scholes;
pub mod distribution;
pub mod vol_surface;
pub mod vol_time;

// Re-exports for convenience
pub use black_scholes::{BlackScholes, OptionType, norm_cdf, norm_ppf};
pub use distribution::{ExpiryDistribution, PriceDistribution};
pub use vol_surface::{VolSmile, VolatilitySurface, DeribitTickerInput, parse_deribit_expiry, parse_deribit_instrument};
pub use vol_time::VolTimeCalculator;

