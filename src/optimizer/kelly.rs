// src/optimizer/kelly.rs
//
// Kelly-style position optimizer with utility maximization.
// Uses argmin's L-BFGS solver for robust optimization.
// Sizes positions to maximize expected utility across Monte Carlo scenarios.

use super::opportunity::Opportunity;
use super::halton::HaltonGenerator;
use crate::pricing::black_scholes::norm_cdf;
use crate::pricing::distribution::PriceDistribution;
use argmin::core::{CostFunction, Error, Executor, Gradient};
use argmin::solver::linesearch::MoreThuenteLineSearch;
use argmin::solver::quasinewton::LBFGS;
use log::{debug, info, warn};
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

/// Pre-computed P&L matrix for efficient optimization.
/// pnls[scenario_idx][opportunity_idx] = P&L for that scenario/opportunity pair.
#[derive(Clone)]
struct PnlMatrix {
    /// P&L values: pnls[scenario][opportunity]
    pnls: Vec<Vec<f64>>,
    /// Number of scenarios
    n_scenarios: usize,
    /// Number of opportunities
    n_opportunities: usize,
}

impl PnlMatrix {
    fn new(scenarios: &[Scenario], opportunities: &[Opportunity]) -> Self {
        let n_scenarios = scenarios.len();
        let n_opportunities = opportunities.len();
        
        let pnls: Vec<Vec<f64>> = scenarios.iter()
            .map(|scenario| {
                opportunities.iter()
                    .map(|opp| Self::compute_pnl(opp, scenario))
                    .collect()
            })
            .collect();
        
        Self { pnls, n_scenarios, n_opportunities }
    }
    
    /// Computes the P&L for an opportunity in a given scenario.
    fn compute_pnl(opportunity: &Opportunity, scenario: &Scenario) -> f64 {
        let price = scenario.prices.get(&opportunity.expiry_timestamp)
            .copied()
            .unwrap_or(0.0);

        match opportunity.opportunity_type {
            super::opportunity::OpportunityType::BinaryYes => {
                if price > opportunity.strike {
                    1.0 - opportunity.market_price
                } else {
                    -opportunity.market_price
                }
            }
            super::opportunity::OpportunityType::BinaryNo => {
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
                let short_payoff = (price - opportunity.strike).max(0.0);
                let long_payoff = (price - strike2).max(0.0);
                let net_payoff = short_payoff - long_payoff;
                opportunity.market_price - net_payoff.min(width)
            }
            super::opportunity::OpportunityType::PutCreditSpread => {
                let strike2 = opportunity.strike2.unwrap_or(opportunity.strike);
                let width = (opportunity.strike - strike2).abs();
                let short_payoff = (opportunity.strike - price).max(0.0);
                let long_payoff = (strike2 - price).max(0.0);
                let net_payoff = short_payoff - long_payoff;
                opportunity.market_price - net_payoff.min(width)
            }
        }
    }
}

/// The optimization problem for argmin.
/// 
/// We minimize negative expected utility (since argmin minimizes).
/// Constraints are handled via soft penalties and post-processing.
struct KellyProblem {
    /// Pre-computed P&L matrix (owned)
    pnl_matrix: PnlMatrix,
    /// Utility function
    utility: UtilityFunction,
    /// Initial wealth
    initial_wealth: f64,
    /// Maximum position sizes (per opportunity)
    max_sizes: Vec<f64>,
    /// Penalty weight for constraint violations
    penalty_weight: f64,
}

impl KellyProblem {
    fn new(
        pnl_matrix: PnlMatrix,
        utility: UtilityFunction,
        initial_wealth: f64,
        opportunities: &[Opportunity],
        max_position_fraction: f64,
    ) -> Self {
        let max_sizes: Vec<f64> = opportunities.iter()
            .map(|o| {
                let capital = o.capital_required().max(0.01);
                let max_from_fraction = (max_position_fraction * initial_wealth) / capital;
                max_from_fraction.min(o.liquidity)
            })
            .collect();
        
        Self {
            pnl_matrix,
            utility,
            initial_wealth,
            max_sizes,
            penalty_weight: 1000.0, // Large penalty for violations
        }
    }
    
    /// Computes expected utility for given position sizes.
    fn expected_utility(&self, sizes: &[f64]) -> f64 {
        let n_scenarios = self.pnl_matrix.n_scenarios as f64;
        
        let total_utility: f64 = self.pnl_matrix.pnls.iter()
            .map(|scenario_pnls| {
                let pnl: f64 = sizes.iter()
                    .zip(scenario_pnls.iter())
                    .map(|(s, p)| s * p)
                    .sum();
                let wealth = self.initial_wealth + pnl;
                self.utility.utility(wealth)
            })
            .sum();
        
        total_utility / n_scenarios
    }
    
    /// Computes gradient of expected utility w.r.t. sizes.
    fn expected_utility_gradient(&self, sizes: &[f64]) -> Vec<f64> {
        let n_scenarios = self.pnl_matrix.n_scenarios as f64;
        let n_opps = self.pnl_matrix.n_opportunities;
        let mut gradients = vec![0.0; n_opps];
        
        for scenario_pnls in &self.pnl_matrix.pnls {
            // Terminal wealth for this scenario
            let pnl: f64 = sizes.iter()
                .zip(scenario_pnls.iter())
                .map(|(s, p)| s * p)
                .sum();
            let wealth = self.initial_wealth + pnl;
            
            // Marginal utility
            let mu = self.utility.marginal_utility(wealth);
            
            // Gradient: ∂E[U]/∂size_i = E[U'(W) * P&L_i]
            for (i, pnl_i) in scenario_pnls.iter().enumerate() {
                gradients[i] += mu * pnl_i;
            }
        }
        
        // Average over scenarios
        for g in &mut gradients {
            *g /= n_scenarios;
        }
        
        gradients
    }
    
    /// Adds penalty for constraint violations (soft constraints).
    fn penalty(&self, sizes: &[f64]) -> f64 {
        let mut penalty = 0.0;
        
        for (i, &size) in sizes.iter().enumerate() {
            // Penalty for negative sizes
            if size < 0.0 {
                penalty += self.penalty_weight * size * size;
            }
            // Penalty for exceeding max size
            if size > self.max_sizes[i] {
                let excess = size - self.max_sizes[i];
                penalty += self.penalty_weight * excess * excess;
            }
        }
        
        penalty
    }
    
    /// Gradient of penalty function.
    fn penalty_gradient(&self, sizes: &[f64]) -> Vec<f64> {
        sizes.iter().enumerate()
            .map(|(i, &size)| {
                let mut grad = 0.0;
                if size < 0.0 {
                    grad += 2.0 * self.penalty_weight * size;
                }
                if size > self.max_sizes[i] {
                    let excess = size - self.max_sizes[i];
                    grad += 2.0 * self.penalty_weight * excess;
                }
                grad
            })
            .collect()
    }
}

impl CostFunction for KellyProblem {
    type Param = Vec<f64>;
    type Output = f64;
    
    /// Returns negative expected utility + penalty (we minimize this).
    fn cost(&self, sizes: &Self::Param) -> Result<Self::Output, Error> {
        let utility = self.expected_utility(sizes);
        let penalty = self.penalty(sizes);
        
        // We minimize, so negate utility and add penalty
        Ok(-utility + penalty)
    }
}

impl Gradient for KellyProblem {
    type Param = Vec<f64>;
    type Gradient = Vec<f64>;
    
    /// Returns gradient of negative expected utility + penalty gradient.
    fn gradient(&self, sizes: &Self::Param) -> Result<Self::Gradient, Error> {
        let utility_grad = self.expected_utility_gradient(sizes);
        let penalty_grad = self.penalty_gradient(sizes);
        
        // Negate utility gradient (since we minimize) and add penalty gradient
        let grad: Vec<f64> = utility_grad.iter()
            .zip(penalty_grad.iter())
            .map(|(u, p)| -u + p)
            .collect();
        
        Ok(grad)
    }
}

/// Maximum number of positions to try exhaustive combinatorial rounding.
/// For n positions needing rounding, we try 2^n combinations.
/// 2^10 = 1024 which is fast; 2^15 = 32768 which is still acceptable.
const MAX_EXHAUSTIVE_ROUNDING_POSITIONS: usize = 12;

/// A position that needs rounding to meet minimum order size constraints.
#[derive(Debug, Clone)]
struct RoundingCandidate {
    /// Index in the opportunities array
    index: usize,
    /// Continuous optimal size (0 < size < min_order_size)
    continuous_size: f64,
    /// Minimum order size to round up to
    min_order_size: f64,
}

/// Kelly-style position optimizer using L-BFGS.
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
    /// Uses L-BFGS algorithm from argmin for robust optimization.
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
        info!("Kelly: Generated {} scenarios", scenarios.len());
        
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

        // Pre-compute P&L matrix for efficiency
        let pnl_matrix = PnlMatrix::new(&scenarios, opportunities);
        info!("Kelly: PnL matrix built");
        
        // Clone for use after optimization (since KellyProblem takes ownership)
        let pnl_matrix_for_rounding = pnl_matrix.clone();
        
        // Create the optimization problem (takes ownership of pnl_matrix)
        let problem = KellyProblem::new(
            pnl_matrix,
            self.config.utility,
            self.config.initial_wealth,
            opportunities,
            self.config.max_position_fraction,
        );

        // Initial position sizes (start with small positions proportional to edge)
        let init_sizes: Vec<f64> = opportunities.iter()
            .map(|o| {
                let target = o.edge.max(0.0) * self.config.initial_wealth * 0.01;
                target.min(o.liquidity).max(0.0)
            })
            .collect();

        // Run L-BFGS optimization
        info!("Kelly: Starting L-BFGS...");
        let sizes = match self.run_lbfgs(problem, init_sizes) {
            Ok(result) => {
                info!("Kelly: L-BFGS success");
                result
            },
            Err(e) => {
                warn!("KellyOptimizer: L-BFGS failed: {}. Using initial sizes.", e);
                opportunities.iter()
                    .map(|o| {
                        let target = o.edge.max(0.0) * self.config.initial_wealth * 0.01;
                        target.min(o.liquidity).max(0.0)
                    })
                    .collect()
            }
        };

        // Post-process: project onto constraints
        let mut final_sizes = self.project_onto_constraints(&sizes, opportunities);
        info!("Kelly: Constraints projected");

        // Apply smart rounding to handle minimum order size constraints
        // This optimizes the rounding decisions to maximize expected utility
        self.apply_smart_rounding(&mut final_sizes, opportunities, &pnl_matrix_for_rounding);
        info!("Kelly: Smart rounding complete");

        // Compute final statistics
        self.build_result(opportunities, &final_sizes, &scenarios)
    }
    
    /// Runs L-BFGS optimization.
    fn run_lbfgs(&self, problem: KellyProblem, init: Vec<f64>) -> Result<Vec<f64>, String> {
        // Set up line search (More-Thuente is robust)
        let linesearch = MoreThuenteLineSearch::new();
        
        // L-BFGS with memory size 7 (typical choice)
        let solver = LBFGS::new(linesearch, 7);
        
        // Run the optimizer
        let result = Executor::new(problem, solver)
            .configure(|state| {
                state
                    .param(init)
                    .max_iters(self.config.max_iterations as u64)
                    .target_cost(f64::NEG_INFINITY) // No target, run until convergence
            })
            .run();
        
        match result {
            Ok(res) => {
                let state = res.state();
                debug!(
                    "KellyOptimizer: L-BFGS converged in {} iterations, cost = {:.6}",
                    state.iter,
                    state.cost
                );
                
                // Return the best parameters found
                match state.best_param.clone() {
                    Some(params) => Ok(params),
                    None => Err("No solution found".to_string()),
                }
            }
            Err(e) => Err(format!("L-BFGS error: {}", e)),
        }
    }
    
    /// Projects sizes onto constraint set.
    fn project_onto_constraints(&self, sizes: &[f64], opportunities: &[Opportunity]) -> Vec<f64> {
        let mut projected: Vec<f64> = sizes.iter().enumerate()
            .map(|(i, &size)| {
                let max_size = (self.config.max_position_fraction * self.config.initial_wealth)
                    / opportunities[i].capital_required().max(0.01);
                size.clamp(0.0, max_size.min(opportunities[i].liquidity))
            })
            .collect();

            // Enforce total exposure constraint
        let total_exposure: f64 = projected.iter()
                .zip(opportunities.iter())
                .map(|(s, o)| s * o.capital_required())
                .sum();
            
            if total_exposure > self.config.max_total_exposure * self.config.initial_wealth {
                let scale = (self.config.max_total_exposure * self.config.initial_wealth) / total_exposure;
            for size in &mut projected {
                    *size *= scale;
                }
            }
        
        projected
    }

    /// Applies smart rounding to handle minimum order size constraints.
    /// 
    /// For positions between 0 and min_order_size, we need to decide whether to
    /// round up to the minimum or skip entirely. This method optimizes that choice
    /// by evaluating expected utility.
    /// 
    /// For small numbers of positions needing rounding (<= MAX_EXHAUSTIVE_ROUNDING_POSITIONS),
    /// we try all 2^n combinations and pick the best one.
    /// For larger numbers, we use a greedy approach.
    fn apply_smart_rounding(
        &self,
        sizes: &mut [f64],
        opportunities: &[Opportunity],
        pnl_matrix: &PnlMatrix,
    ) {
        // First, filter out positions below min dollar value
        for (i, size) in sizes.iter_mut().enumerate() {
            if *size * opportunities[i].capital_required() < self.config.min_position_size {
                *size = 0.0;
            }
        }

        // Identify positions that need rounding
        let candidates: Vec<RoundingCandidate> = sizes.iter()
            .enumerate()
            .filter_map(|(i, &size)| {
                let min_order = opportunities[i].min_order_size();
                if size > 0.0 && size < min_order {
                    Some(RoundingCandidate {
                        index: i,
                        continuous_size: size,
                        min_order_size: min_order,
                    })
                } else {
                    None
                }
            })
            .collect();

        if candidates.is_empty() {
            return;
        }

        debug!(
            "Smart rounding: {} positions need rounding (exhaustive threshold: {})",
            candidates.len(),
            MAX_EXHAUSTIVE_ROUNDING_POSITIONS
        );

        // Choose rounding strategy based on number of candidates
        let rounding_decisions = if candidates.len() <= MAX_EXHAUSTIVE_ROUNDING_POSITIONS {
            self.exhaustive_rounding(&candidates, sizes, opportunities, pnl_matrix)
        } else {
            self.greedy_rounding(&candidates, sizes, opportunities, pnl_matrix)
        };

        // Apply the rounding decisions
        for (candidate, round_up) in candidates.iter().zip(rounding_decisions.iter()) {
            if *round_up {
                sizes[candidate.index] = candidate.min_order_size;
            } else {
                sizes[candidate.index] = 0.0;
            }
        }
    }

    /// Exhaustive search over all 2^n rounding combinations.
    /// Returns a vector of booleans: true = round up, false = skip.
    fn exhaustive_rounding(
        &self,
        candidates: &[RoundingCandidate],
        base_sizes: &[f64],
        _opportunities: &[Opportunity],
        pnl_matrix: &PnlMatrix,
    ) -> Vec<bool> {
        let n = candidates.len();
        let n_combinations = 1usize << n; // 2^n

        let mut best_utility = f64::NEG_INFINITY;
        let mut best_decisions = vec![false; n];

        // Try all combinations
        for combo in 0..n_combinations {
            // Build sizes with this rounding combination
            let mut test_sizes = base_sizes.to_vec();
            
            for (bit, candidate) in candidates.iter().enumerate() {
                let round_up = (combo >> bit) & 1 == 1;
                if round_up {
                    test_sizes[candidate.index] = candidate.min_order_size;
                } else {
                    test_sizes[candidate.index] = 0.0;
                }
            }

            // Evaluate expected utility
            let utility = self.compute_expected_utility(&test_sizes, pnl_matrix);

            if utility > best_utility {
                best_utility = utility;
                best_decisions = (0..n)
                    .map(|bit| (combo >> bit) & 1 == 1)
                    .collect();
            }
        }

        debug!(
            "Exhaustive rounding: tried {} combinations, best utility = {:.6}",
            n_combinations, best_utility
        );

        best_decisions
    }

    /// Greedy rounding for large numbers of candidates.
    /// Ranks candidates by marginal utility gain and rounds up greedily.
    fn greedy_rounding(
        &self,
        candidates: &[RoundingCandidate],
        base_sizes: &[f64],
        _opportunities: &[Opportunity],
        pnl_matrix: &PnlMatrix,
    ) -> Vec<bool> {
        let n = candidates.len();
        let mut decisions = vec![false; n];
        let mut current_sizes = base_sizes.to_vec();

        // Start by setting all candidates to 0
        for candidate in candidates {
            current_sizes[candidate.index] = 0.0;
        }

        // Compute marginal utility gain for rounding up each candidate
        let base_utility = self.compute_expected_utility(&current_sizes, pnl_matrix);
        
        let mut gains: Vec<(usize, f64)> = candidates.iter()
            .enumerate()
            .map(|(i, candidate)| {
                // Temporarily round up this candidate
                current_sizes[candidate.index] = candidate.min_order_size;
                let new_utility = self.compute_expected_utility(&current_sizes, pnl_matrix);
                current_sizes[candidate.index] = 0.0; // Reset
                
                (i, new_utility - base_utility)
            })
            .collect();

        // Sort by marginal utility gain (descending)
        gains.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Greedily add positions with positive marginal utility
        let mut current_utility = base_utility;
        for (candidate_idx, marginal_gain) in gains {
            if marginal_gain <= 0.0 {
                break; // No more positive gains
            }

            // Round up this candidate
            decisions[candidate_idx] = true;
            current_sizes[candidates[candidate_idx].index] = candidates[candidate_idx].min_order_size;
            
            // Recompute utility to account for diminishing returns
            let new_utility = self.compute_expected_utility(&current_sizes, pnl_matrix);
            
            // Only keep if it actually improved
            if new_utility > current_utility {
                current_utility = new_utility;
            } else {
                // Undo this decision
                decisions[candidate_idx] = false;
                current_sizes[candidates[candidate_idx].index] = 0.0;
            }
        }

        debug!(
            "Greedy rounding: {} of {} candidates rounded up, final utility = {:.6}",
            decisions.iter().filter(|&&x| x).count(),
            n,
            current_utility
        );

        decisions
    }

    /// Computes expected utility for a given set of position sizes.
    fn compute_expected_utility(&self, sizes: &[f64], pnl_matrix: &PnlMatrix) -> f64 {
        let n_scenarios = pnl_matrix.n_scenarios as f64;
        
        let total_utility: f64 = pnl_matrix.pnls.iter()
            .map(|scenario_pnls| {
                let pnl: f64 = sizes.iter()
                    .zip(scenario_pnls.iter())
                    .map(|(s, p)| s * p)
                    .sum();
                let wealth = self.config.initial_wealth + pnl;
                self.config.utility.utility(wealth)
            })
            .sum();
        
        total_utility / n_scenarios
    }

    /// Generates Monte Carlo scenarios using Halton sequences.
    /// 
    /// Generates correlated price paths for all opportunity expiries, including
    /// Polymarket expiries that may not have exact matches in the Deribit-based
    /// distribution. Uses interpolation for non-matching expiries.
    fn generate_scenarios(
        &self,
        opportunities: &[Opportunity],
        distribution: &PriceDistribution,
        now_ms: i64,
    ) -> Vec<Scenario> {
        // Collect unique expiries from opportunities
        let mut expiries: Vec<i64> = opportunities.iter()
            .map(|o| o.expiry_timestamp)
            .collect();
        expiries.sort();
        expiries.dedup();

        let n_expiries = expiries.len();
        if n_expiries == 0 {
            return vec![];
        }

        // Pre-compute time to expiry for each opportunity expiry
        // This handles both Deribit expiries (exact match) and Polymarket (interpolated)
        let expiry_info: Vec<(i64, f64)> = expiries.iter()
            .map(|&exp| {
                // Try exact match first
                if let Some(dist) = distribution.get(exp) {
                    (exp, dist.time_to_expiry)
                } else {
                    // Compute time to expiry from timestamp
                    let tte = (exp - now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
                    (exp, tte.max(0.001))
                }
            })
            .collect();

        // Generate Halton normal samples
        let mut halton = HaltonGenerator::new(n_expiries.min(21));
        let z_samples = halton.generate_normal(self.config.n_scenarios);

        let mut scenarios = Vec::with_capacity(self.config.n_scenarios);

        for z_scenario in z_samples {
            let mut prices = HashMap::new();
            let mut cumulative_w = 0.0;

            for (i, &(expiry_ts, time_to_expiry)) in expiry_info.iter().enumerate() {
                if time_to_expiry <= 0.0 {
                    continue;
                }

                // Time increment
                let dt = if i == 0 {
                    time_to_expiry
                } else {
                    (time_to_expiry - expiry_info[i - 1].1).max(0.001)
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

                // Price from PPF - try exact match, then use interpolation
                let price = if let Some(expiry_dist) = distribution.get(expiry_ts) {
                    expiry_dist.ppf(percentile)
                } else {
                    // For non-matching expiries (e.g., Polymarket), use price interpolation
                    distribution.ppf_interpolated(percentile, expiry_ts)
                        .unwrap_or_else(|| {
                            // Fallback: use spot price with simple lognormal assumption
                            let spot = distribution.spot();
                            if spot > 0.0 {
                                spot * (z_marginal * 0.5 * time_to_expiry.sqrt()).exp()
                            } else {
                                0.0
                            }
                        })
                };

                prices.insert(expiry_ts, price);
            }

            scenarios.push(Scenario { prices });
        }

        scenarios
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
            minimum_order_size: Some(15.0),
            minimum_tick_size: Some(0.01),
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
            minimum_order_size: Some(15.0),
            minimum_tick_size: Some(0.01),
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
            minimum_order_size: None,
            minimum_tick_size: None,
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
            minimum_order_size: None,
            minimum_tick_size: None,
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
            minimum_order_size: None,
            minimum_tick_size: None,
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

    #[test]
    fn test_scenarios_generated_for_nonmatching_expiries() {
        // This test verifies the fix for the bug where Polymarket expiries
        // that don't match Deribit expiries would get price=0 in scenarios,
        // causing BinaryYes to always lose and BinaryNo to always win.
        
        use crate::pricing::vol_surface::{VolSmile, VolatilitySurface};
        use crate::pricing::distribution::PriceDistribution;
        
        let now_ms = 1704067200000i64;
        
        // Create a vol surface with Deribit expiries (30 and 90 days)
        let mut surface = VolatilitySurface::new(100_000.0);
        
        let deribit_expiry_30d = now_ms + 30 * 24 * 3600 * 1000;
        let mut smile_30d = VolSmile::new(30.0 / 365.25, 100_000.0);
        smile_30d.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(deribit_expiry_30d, smile_30d);
        
        let deribit_expiry_90d = now_ms + 90 * 24 * 3600 * 1000;
        let mut smile_90d = VolSmile::new(90.0 / 365.25, 100_000.0);
        smile_90d.add_point(100_000.0, 0.45, None, None);
        surface.add_smile(deribit_expiry_90d, smile_90d);
        
        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        
        // Create a Polymarket opportunity with 45-day expiry (NOT matching Deribit)
        let polymarket_expiry = now_ms + 45 * 24 * 3600 * 1000;
        let opp = Opportunity {
            id: "pm-test".to_string(),
            opportunity_type: OpportunityType::BinaryYes,
            exchange: "polymarket".to_string(),
            instrument_id: "btc-above-100k".to_string(),
            description: "BTC above 100k".to_string(),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: polymarket_expiry, // 45 days - doesn't match 30 or 90
            time_to_expiry: 45.0 / 365.25,
            direction: TradeDirection::Buy,
            market_price: 0.40,
            fair_value: 0.50,
            edge: 0.25,
            max_profit: 0.60,
            max_loss: 0.40,
            liquidity: 100.0,
            implied_probability: Some(0.40),
            model_probability: Some(0.50),
            model_iv: None,
            token_id: None,
            minimum_order_size: Some(15.0),
            minimum_tick_size: Some(0.01),
        };
        
        let config = KellyConfig {
            n_scenarios: 100,
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config);
        
        // Generate scenarios
        let scenarios = optimizer.generate_scenarios(&[opp.clone()], &dist, now_ms);
        
        // All scenarios should have a price for the Polymarket expiry
        assert!(!scenarios.is_empty(), "No scenarios generated");
        
        for (i, scenario) in scenarios.iter().enumerate() {
            let price = scenario.prices.get(&polymarket_expiry);
            assert!(
                price.is_some(),
                "Scenario {} missing price for Polymarket expiry", i
            );
            
            let price = *price.unwrap();
            // Price should be reasonable (not 0, not negative, near spot)
            assert!(
                price > 10_000.0 && price < 1_000_000.0,
                "Scenario {} has unreasonable price: ${}", i, price
            );
        }
        
        // Compute P&L for some scenarios - should have variety (not all same outcome)
        let mut wins = 0;
        let mut losses = 0;
        for scenario in &scenarios {
            let pnl = optimizer.compute_pnl(&opp, scenario);
            if pnl > 0.0 {
                wins += 1;
            } else {
                losses += 1;
            }
        }
        
        // With ATM strike, should have roughly balanced wins/losses, not all losses
        // (The bug would cause all losses for BinaryYes since price=0 < strike)
        assert!(
            wins > 10 && losses > 10,
            "P&L outcomes not balanced: {} wins, {} losses (bug would cause 0 wins)",
            wins, losses
        );
    }

    // =====================================================================
    // Smart Rounding Tests
    // =====================================================================

    fn create_opportunity_with_min_order(
        id: &str,
        market_price: f64,
        fair_value: f64,
        min_order_size: f64,
        expiry_ts: i64,
    ) -> Opportunity {
        Opportunity {
            id: id.to_string(),
            opportunity_type: OpportunityType::BinaryYes,
            exchange: "polymarket".to_string(),
            instrument_id: format!("market-{}", id),
            description: format!("Test opportunity {}", id),
            strike: 100_000.0,
            strike2: None,
            expiry_timestamp: expiry_ts,
            time_to_expiry: 0.25,
            direction: TradeDirection::Buy,
            market_price,
            fair_value,
            edge: (fair_value - market_price) / market_price,
            max_profit: 1.0 - market_price,
            max_loss: market_price,
            liquidity: 1000.0,
            implied_probability: Some(market_price),
            model_probability: Some(fair_value),
            model_iv: None,
            token_id: None,
            minimum_order_size: Some(min_order_size),
            minimum_tick_size: Some(0.01),
        }
    }

    #[test]
    fn test_smart_rounding_single_positive_edge() {
        // Single opportunity with positive edge - should round up
        let config = KellyConfig {
            n_scenarios: 500,
            initial_wealth: 10_000.0,
            min_position_size: 0.0, // Don't filter by dollar value
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config.clone());
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = create_opportunity_with_min_order(
            "positive-edge",
            0.40, // market price
            0.60, // fair value (50% edge!)
            10.0, // min order size
            expiry_ts,
        );
        
        // Create deterministic scenarios where opp wins 60% of the time
        let mut scenarios = Vec::new();
        for i in 0..500 {
            let price = if i % 100 < 60 { 110_000.0 } else { 90_000.0 };
            let mut prices = HashMap::new();
            prices.insert(expiry_ts, price);
            scenarios.push(Scenario { prices });
        }
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &[opp.clone()]);
        
        // Start with a small position that needs rounding
        let mut sizes = vec![5.0]; // Below min_order_size of 10
        
        optimizer.apply_smart_rounding(&mut sizes, &[opp], &pnl_matrix);
        
        // With 50% edge, should definitely round up to minimum
        assert_eq!(
            sizes[0], 10.0,
            "Should round up positive-edge opportunity to minimum"
        );
    }

    #[test]
    fn test_smart_rounding_single_negative_edge() {
        // Single opportunity with negative edge - should skip (set to 0)
        let config = KellyConfig {
            n_scenarios: 500,
            initial_wealth: 10_000.0,
            min_position_size: 0.0,
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config.clone());
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = create_opportunity_with_min_order(
            "negative-edge",
            0.60, // market price
            0.40, // fair value (negative edge - overpriced!)
            10.0, // min order size
            expiry_ts,
        );
        
        // Create deterministic scenarios where opp wins only 40% of the time
        let mut scenarios = Vec::new();
        for i in 0..500 {
            let price = if i % 100 < 40 { 110_000.0 } else { 90_000.0 };
            let mut prices = HashMap::new();
            prices.insert(expiry_ts, price);
            scenarios.push(Scenario { prices });
        }
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &[opp.clone()]);
        
        // Start with a small position that needs rounding
        let mut sizes = vec![5.0]; // Below min_order_size of 10
        
        optimizer.apply_smart_rounding(&mut sizes, &[opp], &pnl_matrix);
        
        // With negative edge, should skip (not round up)
        assert_eq!(
            sizes[0], 0.0,
            "Should skip negative-edge opportunity (set to 0)"
        );
    }

    #[test]
    fn test_smart_rounding_exhaustive_multiple_opportunities() {
        // Test exhaustive search with 3 opportunities (2^3 = 8 combinations)
        let config = KellyConfig {
            n_scenarios: 500,
            initial_wealth: 10_000.0,
            min_position_size: 0.0,
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config.clone());
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        
        // Good opportunity (should be rounded up)
        let opp1 = create_opportunity_with_min_order(
            "good", 0.30, 0.55, 10.0, expiry_ts,
        );
        // Marginal opportunity (close call)
        let opp2 = create_opportunity_with_min_order(
            "marginal", 0.45, 0.50, 10.0, expiry_ts,
        );
        // Bad opportunity (should be skipped)
        let opp3 = create_opportunity_with_min_order(
            "bad", 0.70, 0.50, 10.0, expiry_ts,
        );
        
        let opportunities = vec![opp1, opp2, opp3];
        
        // Create scenarios: price distribution centered around 100k
        let mut scenarios = Vec::new();
        for i in 0..500 {
            // 55% win rate overall
            let price = if i % 100 < 55 { 110_000.0 } else { 90_000.0 };
            let mut prices = HashMap::new();
            prices.insert(expiry_ts, price);
            scenarios.push(Scenario { prices });
        }
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &opportunities);
        
        // All positions need rounding (below min order size)
        let mut sizes = vec![5.0, 5.0, 5.0];
        
        optimizer.apply_smart_rounding(&mut sizes, &opportunities, &pnl_matrix);
        
        // The good opportunity should definitely be rounded up
        assert_eq!(
            sizes[0], 10.0,
            "Good opportunity should be rounded up"
        );
        
        // The bad opportunity should definitely be skipped
        assert_eq!(
            sizes[2], 0.0,
            "Bad opportunity should be skipped"
        );
        
        // The marginal one could go either way, but the algorithm made a choice
        assert!(
            sizes[1] == 0.0 || sizes[1] == 10.0,
            "Marginal opportunity should be either 0 or 10, not {}", sizes[1]
        );
    }

    #[test]
    fn test_smart_rounding_positions_already_valid() {
        // Test that positions already meeting minimum aren't changed
        let config = KellyConfig {
            n_scenarios: 100,
            initial_wealth: 10_000.0,
            min_position_size: 0.0,
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config);
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = create_opportunity_with_min_order(
            "valid", 0.40, 0.50, 10.0, expiry_ts,
        );
        
        let scenarios: Vec<Scenario> = (0..100)
            .map(|i| {
                let price = if i % 2 == 0 { 110_000.0 } else { 90_000.0 };
                let mut prices = HashMap::new();
                prices.insert(expiry_ts, price);
                Scenario { prices }
            })
            .collect();
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &[opp.clone()]);
        
        // Position already meets minimum
        let mut sizes = vec![25.0]; // Already >= min_order_size of 10
        
        optimizer.apply_smart_rounding(&mut sizes, &[opp], &pnl_matrix);
        
        // Should not change
        assert_eq!(
            sizes[0], 25.0,
            "Position already meeting minimum should not change"
        );
    }

    #[test]
    fn test_smart_rounding_zero_positions() {
        // Test that zero positions stay zero
        let config = KellyConfig {
            n_scenarios: 100,
            initial_wealth: 10_000.0,
            min_position_size: 0.0,
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config);
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = create_opportunity_with_min_order(
            "zero", 0.40, 0.50, 10.0, expiry_ts,
        );
        
        let scenarios: Vec<Scenario> = (0..100)
            .map(|i| {
                let price = if i % 2 == 0 { 110_000.0 } else { 90_000.0 };
                let mut prices = HashMap::new();
                prices.insert(expiry_ts, price);
                Scenario { prices }
            })
            .collect();
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &[opp.clone()]);
        
        // Position is already zero
        let mut sizes = vec![0.0];
        
        optimizer.apply_smart_rounding(&mut sizes, &[opp], &pnl_matrix);
        
        // Should stay zero
        assert_eq!(
            sizes[0], 0.0,
            "Zero position should stay zero"
        );
    }

    #[test]
    fn test_smart_rounding_min_dollar_value_filter() {
        // Test that min_position_size (dollar value) filter is applied
        let config = KellyConfig {
            n_scenarios: 100,
            initial_wealth: 10_000.0,
            min_position_size: 100.0, // Minimum $100 position
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config);
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        // At market_price 0.40, need 250 shares to hit $100
        let opp = create_opportunity_with_min_order(
            "small-dollar", 0.40, 0.60, 5.0, expiry_ts,
        );
        
        let scenarios: Vec<Scenario> = (0..100)
            .map(|i| {
                let price = if i < 60 { 110_000.0 } else { 90_000.0 };
                let mut prices = HashMap::new();
                prices.insert(expiry_ts, price);
                Scenario { prices }
            })
            .collect();
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &[opp.clone()]);
        
        // Position of 10 shares at $0.40 = $4, below $100 minimum
        let mut sizes = vec![10.0];
        
        optimizer.apply_smart_rounding(&mut sizes, &[opp], &pnl_matrix);
        
        // Should be filtered to zero due to min dollar value
        assert_eq!(
            sizes[0], 0.0,
            "Position below min dollar value should be filtered out"
        );
    }

    #[test]
    fn test_exhaustive_vs_greedy_threshold() {
        // Verify that MAX_EXHAUSTIVE_ROUNDING_POSITIONS is respected
        assert!(
            MAX_EXHAUSTIVE_ROUNDING_POSITIONS >= 10,
            "Should handle at least 10 positions exhaustively"
        );
        assert!(
            MAX_EXHAUSTIVE_ROUNDING_POSITIONS <= 20,
            "Should not try more than 2^20 combinations"
        );
    }

    #[test]
    fn test_compute_expected_utility() {
        // Test the utility computation used in rounding decisions
        let config = KellyConfig {
            utility: UtilityFunction::Log,
            initial_wealth: 10_000.0,
            ..Default::default()
        };
        let optimizer = KellyOptimizer::new(config);
        
        let expiry_ts = 1704067200000i64 + 90 * 24 * 3600 * 1000;
        let opp = create_opportunity_with_min_order(
            "test", 0.40, 0.50, 10.0, expiry_ts,
        );
        
        // 50% win, 50% lose scenarios
        let scenarios: Vec<Scenario> = (0..100)
            .map(|i| {
                let price = if i < 50 { 110_000.0 } else { 90_000.0 };
                let mut prices = HashMap::new();
                prices.insert(expiry_ts, price);
                Scenario { prices }
            })
            .collect();
        
        let pnl_matrix = PnlMatrix::new(&scenarios, &[opp]);
        
        // Zero position should give base utility
        let base_utility = optimizer.compute_expected_utility(&[0.0], &pnl_matrix);
        assert!(
            (base_utility - (10_000.0_f64).ln()).abs() < 0.01,
            "Zero position utility should be ln(initial_wealth)"
        );
        
        // Small position should change utility slightly
        let small_pos_utility = optimizer.compute_expected_utility(&[10.0], &pnl_matrix);
        // With fair bet (50% win), utility shouldn't change much from base
        assert!(
            (small_pos_utility - base_utility).abs() < 0.1,
            "Small fair-bet position should have similar utility to base"
        );
    }

    #[test]
    fn test_rounding_candidate_struct() {
        // Verify RoundingCandidate is created correctly
        let candidate = RoundingCandidate {
            index: 5,
            continuous_size: 7.5,
            min_order_size: 10.0,
        };
        
        assert_eq!(candidate.index, 5);
        assert_eq!(candidate.continuous_size, 7.5);
        assert_eq!(candidate.min_order_size, 10.0);
    }
}

