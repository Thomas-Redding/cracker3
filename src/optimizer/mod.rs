// src/optimizer/mod.rs
//
// Kelly-style position optimizer with Monte Carlo simulation.
// Maximizes expected utility across correlated price scenarios.

pub mod kelly;
pub mod opportunity;
pub mod halton;

pub use kelly::{KellyOptimizer, KellyConfig, OptimizedPortfolio, UtilityFunction};
pub use opportunity::{Opportunity, OpportunityScanner, OpportunityType, ScannerConfig, TradeDirection};
pub use halton::HaltonGenerator;

