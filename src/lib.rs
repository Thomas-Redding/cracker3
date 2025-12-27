// src/lib.rs

// 1. Data Structures (The "Nouns")
// explicit 'pub' makes them available to main.rs
pub mod models;

// 2. Interfaces (The "Contract")
pub mod traits;

// 3. Adapters (The "Plumbing")
pub mod connectors;

// 4. Business Logic (The "Brains")
pub mod strategy;
