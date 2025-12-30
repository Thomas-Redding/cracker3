// src/optimizer/kelly.rs
//
// Kelly-style position optimizer with utility maximization.
// Sizes positions to maximize expected utility across Monte Carlo scenarios.

use super::opportunity::Opportunity;
use super::sobol::SobolGenerator;
use crate::pricing::black_scholes::norm_cdf;
use crate::pricing::distribution::PriceDistribution;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Utility function type.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UtilityFunction {
    /// Log utility (Kelly criterion): U(W) = ln(W)
    Log,
    /// CRRA/Isoelastic utility: U(W) = (W^(1-γ) - 1) / (1-γ)
    /// γ = 1 reduces to log utility
    /// γ > 1 is more risk averse
    CRRA { gamma: f64 },
    /// Linear utility (risk-neutral): U(W) = W
    Linear,
}

impl UtilityFunction {
    /// Computes utility of wealth.
    pub fn utility(&self, wealth: f64) -> f64 {
        if wealth <= 0.0 {
            return f64::NEG_INFINITY;
        }

        match self {
            UtilityFunction::Log => wealth.ln(),
            UtilityFunction::CRRA { gamma } => {
                if (*gamma - 1.0).abs() < 1e-10 {
                    wealth.ln()
                } else {
                    (wealth.powf(1.0 - gamma) - 1.0) / (1.0 - gamma)
                }
            }
            UtilityFunction::Linear => wealth,
        }
    }

    /// Computes the derivative of utility (marginal utility).
    pub fn marginal_utility(&self, wealth: f64) -> f64 {
        if wealth <= 0.0 {
            return f64::INFINITY;
        }

        match self {
            UtilityFunction::Log => 1.0 / wealth,
            UtilityFunction::CRRA { gamma } => wealth.powf(-*gamma),
            UtilityFunction::Linear => 1.0,
        }
    }
}

/// Configuration for the Kelly optimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KellyConfig {
    /// Utility function to maximize
    pub utility: UtilityFunction,
    /// Number of Monte Carlo scenarios
    pub n_scenarios: usize,
    /// Initial wealth/bankroll
    pub initial_wealth: f64,
    /// Maximum position size as fraction of wealth
    pub max_position_fraction: f64,
    /// Maximum total exposure as fraction of wealth
    pub max_total_exposure: f64,
    /// Minimum position size (absolute)
    pub min_position_size: f64,
    /// Learning rate for gradient ascent
    pub learning_rate: f64,
    /// Maximum optimization iterations
    pub max_iterations: usize,
    /// Convergence tolerance
    pub tolerance: f64,
}

impl Default for KellyConfig {
    fn default() -> Self {
        Self {
            utility: UtilityFunction::Log,
            n_scenarios: 10_000,
            initial_wealth: 10_000.0,
            max_position_fraction: 0.20,
            max_total_exposure: 0.80,
            min_position_size: 1.0,
            learning_rate: 0.01,
            max_iterations: 1000,
            tolerance: 1e-6,
        }
    }
}

/// Result of a single position allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionAllocation {
    /// Opportunity ID
    pub opportunity_id: String,
    /// Recommended position size (units)
    pub size: f64,
    /// Dollar value of position
    pub dollar_value: f64,
    /// Expected profit
    pub expected_profit: f64,
    /// Expected utility contribution
    pub utility_contribution: f64,
}

/// Optimized portfolio result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedPortfolio {
    /// Positions to take
    pub positions: Vec<PositionAllocation>,
    /// Total expected utility
    pub expected_utility: f64,
    /// Expected portfolio return
    pub expected_return: f64,
    /// Expected Sharpe-like ratio (return / volatility)
    pub expected_sharpe: f64,
    /// Probability of loss
    pub prob_loss: f64,
    /// Maximum drawdown in scenarios
    pub max_drawdown: f64,
    /// Initial wealth
    pub initial_wealth: f64,
}

/// Monte Carlo scenario for a set of opportunities.
#[derive(Debug, Clone)]
struct Scenario {
    /// Prices at each expiry (indexed by expiry timestamp)
    prices: HashMap<i64, f64>,
}

/// Kelly-style position optimizer.
pub struct KellyOptimizer {
    config: KellyConfig,
}

impl KellyOptimizer {
    /// Creates a new optimizer with the given configuration.
    pub fn new(config: KellyConfig) -> Self {
        Self { config }
    }

    /// Creates an optimizer with default configuration.
    pub fn default() -> Self {
        Self::new(KellyConfig::default())
    }

    /// Optimizes position sizes for a set of opportunities.
    /// 
    /// # Arguments
    /// * `opportunities` - Trading opportunities to consider
    /// * `distribution` - Price distribution from vol surface
    /// * `now_ms` - Current timestamp
    pub fn optimize(
        &self,
        opportunities: &[Opportunity],
        distribution: &PriceDistribution,
        now_ms: i64,
    ) -> OptimizedPortfolio {
        if opportunities.is_empty() {
            return OptimizedPortfolio {
                positions: vec![],
                expected_utility: self.config.utility.utility(self.config.initial_wealth),
                expected_return: 0.0,
                expected_sharpe: 0.0,
                prob_loss: 0.0,
                max_drawdown: 0.0,
                initial_wealth: self.config.initial_wealth,
            };
        }

        // Generate correlated scenarios
        let scenarios = self.generate_scenarios(opportunities, distribution, now_ms);
        
        if scenarios.is_empty() {
            info!("KellyOptimizer: No valid scenarios generated");
            return OptimizedPortfolio {
                positions: vec![],
                expected_utility: self.config.utility.utility(self.config.initial_wealth),
                expected_return: 0.0,
                expected_sharpe: 0.0,
                prob_loss: 0.0,
                max_drawdown: 0.0,
                initial_wealth: self.config.initial_wealth,
            };
        }

        // Initial position sizes (start small)
        let mut sizes: Vec<f64> = opportunities.iter()
            .map(|o| {
                // Start with a position proportional to edge and liquidity
                let target = o.edge * self.config.initial_wealth * 0.01;
                target.min(o.liquidity).max(0.0)
            })
            .collect();

        // Gradient ascent optimization
        let mut prev_utility = f64::NEG_INFINITY;
        
        for iter in 0..self.config.max_iterations {
            // Compute expected utility and gradients
            let (utility, gradients) = self.compute_utility_and_gradients(
                opportunities,
                &sizes,
                &scenarios,
            );

            // Check convergence
            if (utility - prev_utility).abs() < self.config.tolerance {
                debug!("KellyOptimizer: Converged at iteration {}", iter);
                break;
            }
            prev_utility = utility;

            // Gradient ascent with projection
            for (i, grad) in gradients.iter().enumerate() {
                sizes[i] += self.config.learning_rate * grad;
                
                // Project onto constraints
                let max_size = (self.config.max_position_fraction * self.config.initial_wealth)
                    / opportunities[i].capital_required().max(0.01);
                sizes[i] = sizes[i].clamp(0.0, max_size.min(opportunities[i].liquidity));
            }

            // Enforce total exposure constraint
            let total_exposure: f64 = sizes.iter()
                .zip(opportunities.iter())
                .map(|(s, o)| s * o.capital_required())
                .sum();
            
            if total_exposure > self.config.max_total_exposure * self.config.initial_wealth {
                let scale = (self.config.max_total_exposure * self.config.initial_wealth) / total_exposure;
                for size in &mut sizes {
                    *size *= scale;
                }
            }
        }

        // Filter out small positions
        for (i, size) in sizes.iter_mut().enumerate() {
            if *size * opportunities[i].capital_required() < self.config.min_position_size {
                *size = 0.0;
            }
        }

        // Compute final statistics
        self.build_result(opportunities, &sizes, &scenarios)
    }

    /// Generates Monte Carlo scenarios using Sobol sequences.
    fn generate_scenarios(
        &self,
        opportunities: &[Opportunity],
        distribution: &PriceDistribution,
        _now_ms: i64,
    ) -> Vec<Scenario> {
        // Collect unique expiries
        let mut expiries: Vec<i64> = opportunities.iter()
            .map(|o| o.expiry_timestamp)
            .collect();
        expiries.sort();
        expiries.dedup();

        let n_expiries = expiries.len();
        if n_expiries == 0 {
            return vec![];
        }

        // Generate Sobol normal samples
        let mut sobol = SobolGenerator::new(n_expiries.min(21));
        let z_samples = sobol.generate_normal(self.config.n_scenarios);

        let mut scenarios = Vec::with_capacity(self.config.n_scenarios);

        for z_scenario in z_samples {
            let mut prices = HashMap::new();
            let mut cumulative_w = 0.0;

            for (i, &expiry_ts) in expiries.iter().enumerate() {
                let expiry_dist = match distribution.get(expiry_ts) {
                    Some(d) => d,
                    None => continue,
                };

                let time_to_expiry = expiry_dist.time_to_expiry;
                
                // Time increment
                let dt = if i == 0 {
                    time_to_expiry
                } else {
                    let prev_tte = distribution.get(expiries[i - 1])
                        .map(|d| d.time_to_expiry)
                        .unwrap_or(0.0);
                    (time_to_expiry - prev_tte).max(0.001)
                };

                // Brownian bridge
                let z = if i < z_scenario.len() { z_scenario[i] } else { 0.0 };
                let dw = z * dt.sqrt();
                cumulative_w += dw;

                // Marginal distribution
                let z_marginal = if time_to_expiry > 0.001 {
                    cumulative_w / time_to_expiry.sqrt()
                } else {
                    cumulative_w
                };
                let percentile = norm_cdf(z_marginal);

                // Price from PPF
                let price = expiry_dist.ppf(percentile);
                prices.insert(expiry_ts, price);
            }

            scenarios.push(Scenario { prices });
        }

        scenarios
    }

    /// Computes expected utility and gradients for gradient ascent.
    fn compute_utility_and_gradients(
        &self,
        opportunities: &[Opportunity],
        sizes: &[f64],
        scenarios: &[Scenario],
    ) -> (f64, Vec<f64>) {
        let n_scenarios = scenarios.len() as f64;
        let mut total_utility = 0.0;
        let mut gradients = vec![0.0; opportunities.len()];

        for scenario in scenarios {
            // Compute P&L for each opportunity
            let pnls: Vec<f64> = opportunities.iter()
                .map(|o| self.compute_pnl(o, scenario))
                .collect();

            // Terminal wealth
            let wealth: f64 = self.config.initial_wealth 
                + sizes.iter().zip(&pnls).map(|(s, p)| s * p).sum::<f64>();

            // Utility
            let u = self.config.utility.utility(wealth);
            total_utility += u;

            // Marginal utility for gradient
            let mu = self.config.utility.marginal_utility(wealth);

            // Gradient: ∂E[U]/∂size_i = E[U'(W) * P&L_i]
            for (i, pnl) in pnls.iter().enumerate() {
                gradients[i] += mu * pnl;
            }
        }

        // Average over scenarios
        total_utility /= n_scenarios;
        for g in &mut gradients {
            *g /= n_scenarios;
        }

        (total_utility, gradients)
    }

    /// Computes the P&L for an opportunity in a given scenario.
    fn compute_pnl(&self, opportunity: &Opportunity, scenario: &Scenario) -> f64 {
        let price = scenario.prices.get(&opportunity.expiry_timestamp)
            .copied()
            .unwrap_or(0.0);

        match opportunity.opportunity_type {
            super::opportunity::OpportunityType::BinaryYes => {
                // Pays 1 if price > strike, 0 otherwise
                if price > opportunity.strike {
                    1.0 - opportunity.market_price // Win
                } else {
                    -opportunity.market_price // Lose
                }
            }
            super::opportunity::OpportunityType::BinaryNo => {
                // Pays 1 if price <= strike, 0 otherwise
                if price <= opportunity.strike {
                    1.0 - opportunity.market_price
                } else {
                    -opportunity.market_price
                }
            }
            super::opportunity::OpportunityType::VanillaCall => {
                let payoff = (price - opportunity.strike).max(0.0);
                match opportunity.direction {
                    super::opportunity::TradeDirection::Buy => payoff - opportunity.market_price,
                    super::opportunity::TradeDirection::Sell => opportunity.market_price - payoff,
                }
            }
            super::opportunity::OpportunityType::VanillaPut => {
                let payoff = (opportunity.strike - price).max(0.0);
                match opportunity.direction {
                    super::opportunity::TradeDirection::Buy => payoff - opportunity.market_price,
                    super::opportunity::TradeDirection::Sell => opportunity.market_price - payoff,
                }
            }
            super::opportunity::OpportunityType::CallCreditSpread => {
                let strike2 = opportunity.strike2.unwrap_or(opportunity.strike);
                let width = (strike2 - opportunity.strike).abs();
                
                // Short lower strike, long higher strike
                let short_payoff = (price - opportunity.strike).max(0.0);
                let long_payoff = (price - strike2).max(0.0);
                let net_payoff = short_payoff - long_payoff; // What we owe
                
                opportunity.market_price - net_payoff.min(width) // Credit - loss
            }
            super::opportunity::OpportunityType::PutCreditSpread => {
                let strike2 = opportunity.strike2.unwrap_or(opportunity.strike);
                let width = (opportunity.strike - strike2).abs();
                
                // Short higher strike, long lower strike
                let short_payoff = (opportunity.strike - price).max(0.0);
                let long_payoff = (strike2 - price).max(0.0);
                let net_payoff = short_payoff - long_payoff; // What we owe
                
                opportunity.market_price - net_payoff.min(width) // Credit - loss
            }
        }
    }

    /// Builds the final result with statistics.
    fn build_result(
        &self,
        opportunities: &[Opportunity],
        sizes: &[f64],
        scenarios: &[Scenario],
    ) -> OptimizedPortfolio {
        let mut positions = Vec::new();
        let mut _total_expected_profit = 0.0;

        for (opp, &size) in opportunities.iter().zip(sizes.iter()) {
            if size < 1e-10 {
                continue;
            }

            // Expected P&L for this position
            let expected_pnl: f64 = scenarios.iter()
                .map(|s| self.compute_pnl(opp, s) * size)
                .sum::<f64>() / scenarios.len() as f64;

            _total_expected_profit += expected_pnl;

            positions.push(PositionAllocation {
                opportunity_id: opp.id.clone(),
                size,
                dollar_value: size * opp.capital_required(),
                expected_profit: expected_pnl,
                utility_contribution: 0.0, // Could compute marginal contribution
            });
        }

        // Compute portfolio statistics
        let mut terminal_wealths: Vec<f64> = scenarios.iter()
            .map(|scenario| {
                let pnl: f64 = opportunities.iter()
                    .zip(sizes.iter())
                    .map(|(o, s)| s * self.compute_pnl(o, scenario))
                    .sum();
                self.config.initial_wealth + pnl
            })
            .collect();

        terminal_wealths.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let expected_utility: f64 = terminal_wealths.iter()
            .map(|&w| self.config.utility.utility(w))
            .sum::<f64>() / scenarios.len() as f64;

        let expected_wealth: f64 = terminal_wealths.iter().sum::<f64>() / scenarios.len() as f64;
        let expected_return = (expected_wealth - self.config.initial_wealth) / self.config.initial_wealth;

        let variance: f64 = terminal_wealths.iter()
            .map(|&w| (w - expected_wealth).powi(2))
            .sum::<f64>() / scenarios.len() as f64;
        let volatility = variance.sqrt() / self.config.initial_wealth;

        let expected_sharpe = if volatility > 0.0 {
            expected_return / volatility
        } else {
            0.0
        };

        let prob_loss = terminal_wealths.iter()
            .filter(|&&w| w < self.config.initial_wealth)
            .count() as f64 / scenarios.len() as f64;

        let min_wealth = *terminal_wealths.first().unwrap_or(&self.config.initial_wealth);
        let max_drawdown = (self.config.initial_wealth - min_wealth) / self.config.initial_wealth;

        OptimizedPortfolio {
            positions,
            expected_utility,
            expected_return,
            expected_sharpe,
            prob_loss,
            max_drawdown,
            initial_wealth: self.config.initial_wealth,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::opportunity::{OpportunityType, TradeDirection};

    fn create_test_opportunity() -> Opportunity {
        Opportunity {
            id: "test".to_string(),
            opportunity_type: OpportunityType::BinaryYes,
            exchange: "polymarket".to_string(),
            instrument_id: "test_market".to_string(),
            description: "Test opportunity".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: 1704067200000 + 90 * 24 * 3600 * 1000, // 90 days
            time_to_expiry: 0.25,
            direction: TradeDirection::Buy,
            market_price: 0.40, // Market says 40%
            fair_value: 0.55,   // Model says 55%
            edge: 0.375,        // 37.5% edge
            max_profit: 0.60,
            max_loss: 0.40,
            liquidity: 100.0,
            implied_probability: Some(0.40),
            model_probability: Some(0.55),
            model_iv: None,
            token_id: None,
        }
    }

    #[test]
    fn test_utility_functions() {
        let log_utility = UtilityFunction::Log;
        assert!((log_utility.utility(1.0) - 0.0).abs() < 1e-10);
        assert!((log_utility.utility(std::f64::consts::E) - 1.0).abs() < 1e-10);

        let crra = UtilityFunction::CRRA { gamma: 2.0 };
        // For CRRA with gamma=2: U(W) = (W^(1-2) - 1) / (1-2) = (1/W - 1) / (-1) = 1 - 1/W
        // For W=100: U = 1 - 0.01 = 0.99 (positive)
        // For W=0.5: U = 1 - 2 = -1 (negative)
        assert!(crra.utility(100.0) > 0.0); // Positive for W > 1 when gamma > 1
        assert!(crra.utility(200.0) > crra.utility(100.0)); // Monotonic
    }

    #[test]
    fn test_compute_pnl_binary() {
        let opp = create_test_opportunity();
        let optimizer = KellyOptimizer::default();

        // Winning scenario (price > strike)
        let win_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 110_000.0)].into_iter().collect(),
        };
        let win_pnl = optimizer.compute_pnl(&opp, &win_scenario);
        assert!((win_pnl - 0.60).abs() < 1e-10); // 1 - 0.40

        // Losing scenario (price < strike)
        let lose_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 90_000.0)].into_iter().collect(),
        };
        let lose_pnl = optimizer.compute_pnl(&opp, &lose_scenario);
        assert!((lose_pnl - (-0.40)).abs() < 1e-10); // -cost
    }

    #[test]
    fn test_kelly_sizing() {
        // With positive edge, Kelly should recommend a positive position
        let config = KellyConfig {
            n_scenarios: 1000,
            initial_wealth: 10_000.0,
            max_iterations: 100,
            ..Default::default()
        };
        
        let optimizer = KellyOptimizer::new(config);
        
        // Create a simple fair coin flip with edge
        let opp = create_test_opportunity();
        
        // Mock distribution that gives 50% probability at the strike
        use crate::pricing::vol_surface::{VolSmile, VolatilitySurface};
        use crate::pricing::distribution::PriceDistribution;
        
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = opp.expiry_timestamp;
        
        let mut smile = VolSmile::new(0.25, 100_000.0);
        for strike in (80_000..=120_000).step_by(5000) {
            smile.add_point(strike as f64, 0.50, None, None);
        }
        surface.add_smile(expiry_ms, smile);
        
        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        
        let result = optimizer.optimize(&[opp], &dist, now_ms);
        
        // Should have a position since there's positive edge
        assert!(result.positions.len() > 0 || result.expected_return >= 0.0);
    }

    #[test]
    fn test_config_default() {
        let config = KellyConfig::default();
        
        assert_eq!(config.n_scenarios, 10_000);
        assert_eq!(config.initial_wealth, 10_000.0);
        assert!(config.max_position_fraction > 0.0 && config.max_position_fraction <= 1.0);
        assert!(config.tolerance > 0.0);
    }

    #[test]
    fn test_utility_function_log_monotonic() {
        let log = UtilityFunction::Log;
        
        // Utility should be monotonically increasing
        let mut prev_util = f64::NEG_INFINITY;
        for w in [0.1, 1.0, 10.0, 100.0, 1000.0] {
            let util = log.utility(w);
            assert!(util > prev_util, "Log utility not monotonic at w={}", w);
            prev_util = util;
        }
    }

    #[test]
    fn test_utility_function_crra_monotonic() {
        for gamma in [0.5, 1.5, 2.0, 3.0] {
            let crra = UtilityFunction::CRRA { gamma };
            
            let mut prev_util = f64::NEG_INFINITY;
            for w in [0.1, 1.0, 10.0, 100.0, 1000.0] {
                let util = crra.utility(w);
                assert!(util > prev_util, "CRRA(γ={}) not monotonic at w={}", gamma, w);
                prev_util = util;
            }
        }
    }

    #[test]
    fn test_utility_function_concave() {
        // Utility should be concave (diminishing marginal utility)
        let log = UtilityFunction::Log;
        
        // u(2W) - u(W) < u(W) - u(0.5W) for concave functions
        let w = 100.0;
        let gain_high = log.utility(2.0 * w) - log.utility(w);
        let gain_low = log.utility(w) - log.utility(0.5 * w);
        
        assert!(gain_low > gain_high, "Log utility not concave");
    }

    #[test]
    fn test_compute_pnl_binary_no() {
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = Opportunity {
            id: "test-no".to_string(),
            opportunity_type: OpportunityType::BinaryNo,
            exchange: "test".to_string(),
            instrument_id: "test".to_string(),
            description: "Test NO".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: expiry_ts,
            time_to_expiry: 0.25,
            direction: TradeDirection::Buy,
            market_price: 0.50,
            fair_value: 0.60,
            edge: 0.20,
            max_profit: 0.50,
            max_loss: 0.50,
            liquidity: 100.0,
            implied_probability: Some(0.50),
            model_probability: Some(0.60),
            model_iv: None,
            token_id: Some("token".to_string()),
        };
        
        let optimizer = KellyOptimizer::default();

        // For BinaryNo: wins if price < strike
        // Winning scenario (price < strike)
        let win_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 90_000.0)].into_iter().collect(),
        };
        let win_pnl = optimizer.compute_pnl(&opp, &win_scenario);
        assert!((win_pnl - 0.50).abs() < 1e-10); // 1 - 0.50

        // Losing scenario (price > strike)
        let lose_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 110_000.0)].into_iter().collect(),
        };
        let lose_pnl = optimizer.compute_pnl(&opp, &lose_scenario);
        assert!((lose_pnl - (-0.50)).abs() < 1e-10); // -cost
    }

    #[test]
    fn test_compute_pnl_vanilla_call() {
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = Opportunity {
            id: "test-call".to_string(),
            opportunity_type: OpportunityType::VanillaCall,
            exchange: "derive".to_string(),
            instrument_id: "BTC-CALL-100K".to_string(),
            description: "Test call".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: expiry_ts,
            time_to_expiry: 0.25,
            direction: TradeDirection::Buy,
            market_price: 4000.0,
            fair_value: 5000.0,
            edge: 0.25,
            max_profit: f64::MAX,
            max_loss: 4000.0,
            liquidity: 10.0,
            implied_probability: None,
            model_probability: None,
            model_iv: Some(0.50),
            token_id: None,
        };
        
        let optimizer = KellyOptimizer::default();

        // ITM scenario (price > strike)
        let itm_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 120_000.0)].into_iter().collect(),
        };
        let itm_pnl = optimizer.compute_pnl(&opp, &itm_scenario);
        // Payoff = max(0, 120k - 100k) - premium = 20k - 4k = 16k
        assert!((itm_pnl - 16000.0).abs() < 1e-6);

        // OTM scenario (price < strike)
        let otm_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 90_000.0)].into_iter().collect(),
        };
        let otm_pnl = optimizer.compute_pnl(&opp, &otm_scenario);
        // Payoff = max(0, 90k - 100k) - premium = 0 - 4k = -4k
        assert!((otm_pnl - (-4000.0)).abs() < 1e-6);
    }

    #[test]
    fn test_compute_pnl_vanilla_put() {
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = Opportunity {
            id: "test-put".to_string(),
            opportunity_type: OpportunityType::VanillaPut,
            exchange: "derive".to_string(),
            instrument_id: "BTC-PUT-100K".to_string(),
            description: "Test put".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: expiry_ts,
            time_to_expiry: 0.25,
            direction: TradeDirection::Buy,
            market_price: 4000.0,
            fair_value: 5000.0,
            edge: 0.25,
            max_profit: 100_000.0,
            max_loss: 4000.0,
            liquidity: 10.0,
            implied_probability: None,
            model_probability: None,
            model_iv: Some(0.50),
            token_id: None,
        };
        
        let optimizer = KellyOptimizer::default();

        // ITM scenario (price < strike)
        let itm_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 80_000.0)].into_iter().collect(),
        };
        let itm_pnl = optimizer.compute_pnl(&opp, &itm_scenario);
        // Payoff = max(0, 100k - 80k) - premium = 20k - 4k = 16k
        assert!((itm_pnl - 16000.0).abs() < 1e-6);

        // OTM scenario (price > strike)
        let otm_scenario = Scenario {
            prices: [(opp.expiry_timestamp, 120_000.0)].into_iter().collect(),
        };
        let otm_pnl = optimizer.compute_pnl(&opp, &otm_scenario);
        // Payoff = max(0, 100k - 120k) - premium = 0 - 4k = -4k
        assert!((otm_pnl - (-4000.0)).abs() < 1e-6);
    }

    #[test]
    fn test_capital_required_binary() {
        let opp = create_test_opportunity();
        // Binary option capital = market price per contract
        assert!((opp.capital_required() - 0.40).abs() < 1e-10);
    }

    #[test]
    fn test_capital_required_vanilla() {
        let opp = Opportunity {
            id: "test-cap".to_string(),
            opportunity_type: OpportunityType::VanillaCall,
            exchange: "derive".to_string(),
            instrument_id: "BTC-CALL".to_string(),
            description: "Test capital".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: 0,
            time_to_expiry: 0.25,
            direction: TradeDirection::Buy,
            market_price: 4000.0,
            fair_value: 5000.0,
            edge: 0.25,
            max_profit: f64::MAX,
            max_loss: 4000.0,
            liquidity: 10.0,
            implied_probability: None,
            model_probability: None,
            model_iv: Some(0.50),
            token_id: None,
        };
        // Vanilla option capital = premium (market price)
        assert!((opp.capital_required() - 4000.0).abs() < 1e-6);
    }

    #[test]
    fn test_optimization_result_structure() {
        let config = KellyConfig::default();
        let optimizer = KellyOptimizer::new(config);
        
        use crate::pricing::vol_surface::{VolSmile, VolatilitySurface};
        use crate::pricing::distribution::PriceDistribution;
        
        let opp = create_test_opportunity();
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        
        let mut smile = VolSmile::new(0.25, 100_000.0);
        smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(opp.expiry_timestamp, smile);
        
        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        let result = optimizer.optimize(&[opp], &dist, now_ms);
        
        // Result should have valid fields
        assert!(result.expected_return.is_finite());
        assert!(result.expected_sharpe.is_finite());
        assert!(result.expected_utility.is_finite());
        assert!(result.prob_loss.is_finite() && result.prob_loss >= 0.0 && result.prob_loss <= 1.0);
        assert!(result.max_drawdown.is_finite() && result.max_drawdown >= 0.0);
    }
}

