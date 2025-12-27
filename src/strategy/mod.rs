// src/strategy/mod.rs

// 1. Declare the child modules (the actual files in this folder)
pub mod gamma_scalp;
// pub mod arbitrage_bot;   <-- easier to add new strategies later

// 2. Re-export the struct for cleaner imports
// This allows 'use crate::strategy::GammaScalp'
pub use gamma_scalp::GammaScalp;

