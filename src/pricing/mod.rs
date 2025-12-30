// src/pricing/mod.rs
//
// Options pricing infrastructure with volatility surface interpolation,
// Black-Scholes pricing, and probability distribution functions.

pub mod black_scholes;
pub mod distribution;
pub mod vol_surface;

pub use black_scholes::{BlackScholes, OptionType};
pub use distribution::PriceDistribution;
pub use vol_surface::VolatilitySurface;

