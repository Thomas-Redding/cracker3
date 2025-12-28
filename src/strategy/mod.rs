// src/strategy/mod.rs

// 1. Declare the child modules (the actual files in this folder)
pub mod gamma_scalp;
pub mod momentum;

// 2. Re-export the structs for cleaner imports
pub use gamma_scalp::GammaScalp;
pub use momentum::MomentumStrategy;
