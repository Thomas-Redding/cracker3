// src/strategy/mod.rs

// 1. Declare the child modules (the actual files in this folder)
pub mod gamma_scalp;
pub mod momentum;
pub mod cross_market;

// 2. Re-export the structs for cleaner imports
pub use gamma_scalp::GammaScalp;
pub use momentum::MomentumStrategy;
pub use cross_market::{
    CrossMarketStrategy, CrossMarketConfig,
    setup_deribit_subscriptions, setup_derive_subscriptions,
};
