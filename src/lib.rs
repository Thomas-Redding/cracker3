// src/lib.rs

// 1. Data Structures (The "Nouns")
// explicit 'pub' makes them available to main.rs
pub mod models;

// 2. Interfaces (The "Contract")
pub mod traits;

// 3. Adapters (The "Plumbing")
pub mod connectors;

// 4. Market Discovery (The "Catalog")
pub mod catalog;

// 5. Business Logic (The "Brains")
pub mod strategy;

// 6. Multi-Strategy Engine (The "Orchestrator")
pub mod engine;

// 7. Dashboard Web Server (The "UI")
pub mod dashboard;

// 8. Configuration (The "Setup")
pub mod config;

// 9. Pricing Models (The "Math")
pub mod pricing;

// 10. Position Optimizer (The "Optimizer")
// 11. Simulation Framework
pub mod optimizer;

// 11. Simulation Framework
pub mod simulation;

// 12. Backtesting
pub mod backtest;
