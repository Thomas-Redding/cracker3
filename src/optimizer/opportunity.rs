// src/optimizer/opportunity.rs
//
// Opportunity identification for binary options, vanilla options, and spreads.
// Scans markets for mispricings relative to the calibrated volatility surface.

use crate::pricing::{BlackScholes, OptionType, PriceDistribution, VolatilitySurface};
use serde::{Deserialize, Serialize};

/// Type of trading opportunity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpportunityType {
    /// Binary option: pays $1 if condition met
    BinaryYes,
    /// Binary option: pays $1 if condition NOT met
    BinaryNo,
    /// Vanilla call option
    VanillaCall,
    /// Vanilla put option
    VanillaPut,
    /// Call credit spread (sell lower strike, buy higher strike)
    CallCreditSpread,
    /// Put credit spread (sell higher strike, buy lower strike)
    PutCreditSpread,
}

/// Direction of the trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeDirection {
    Buy,
    Sell,
}

/// A single trading opportunity with pricing edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opportunity {
    /// Unique identifier
    pub id: String,
    /// Type of opportunity
    pub opportunity_type: OpportunityType,
    /// Exchange (e.g., "polymarket", "derive")
    pub exchange: String,
    /// Instrument/market identifier
    pub instrument_id: String,
    /// Human-readable description
    pub description: String,
    /// Strike price
    pub strike: f64,
    /// Second strike for spreads
    pub strike2: Option<f64>,
    /// Expiry timestamp (ms)
    pub expiry_timestamp: i64,
    /// Time to expiry (years)
    pub time_to_expiry: f64,
    /// Direction: buy or sell
    pub direction: TradeDirection,
    /// Market price (what we pay/receive)
    pub market_price: f64,
    /// Fair value from model
    pub fair_value: f64,
    /// Edge = (fair - market) / market for buys, (market - fair) / max_loss for sells
    pub edge: f64,
    /// Maximum profit per unit
    pub max_profit: f64,
    /// Maximum loss per unit
    pub max_loss: f64,
    /// Available liquidity (units)
    pub liquidity: f64,
    /// Implied probability (for binary options)
    pub implied_probability: Option<f64>,
    /// Model probability (for binary options)
    pub model_probability: Option<f64>,
    /// Model IV used
    pub model_iv: Option<f64>,
    /// Token ID for order execution
    pub token_id: Option<String>,
}

impl Opportunity {
    /// Returns the expected value per unit.
    pub fn expected_value(&self) -> f64 {
        self.fair_value - self.market_price
    }

    /// Returns true if this is a buy opportunity.
    pub fn is_buy(&self) -> bool {
        matches!(self.direction, TradeDirection::Buy)
    }

    /// Returns the required capital per unit.
    pub fn capital_required(&self) -> f64 {
        match self.direction {
            TradeDirection::Buy => self.market_price,
            TradeDirection::Sell => self.max_loss,
        }
    }
}

/// Configuration for the opportunity scanner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerConfig {
    /// Minimum edge to consider (e.g., 0.02 for 2%)
    pub min_edge: f64,
    /// Maximum spread width for credit spreads
    pub max_spread_width: f64,
    /// Minimum liquidity (units available)
    pub min_liquidity: f64,
    /// Maximum time to expiry (years)
    pub max_time_to_expiry: f64,
    /// Risk-free rate for pricing
    pub rate: f64,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            min_edge: 0.02,
            max_spread_width: 5000.0,
            min_liquidity: 0.01,
            max_time_to_expiry: 1.0,
            rate: 0.0,
        }
    }
}

/// Scanner for finding trading opportunities.
pub struct OpportunityScanner {
    config: ScannerConfig,
}

impl OpportunityScanner {
    /// Creates a new scanner with the given configuration.
    pub fn new(config: ScannerConfig) -> Self {
        Self { config }
    }

    /// Creates a scanner with default configuration.
    pub fn default() -> Self {
        Self::new(ScannerConfig::default())
    }

    /// Scans a binary option market for opportunities.
    /// 
    /// # Arguments
    /// * `market_id` - Unique identifier for the market
    /// * `description` - Human-readable description
    /// * `strike` - Strike price threshold
    /// * `expiry_timestamp` - Expiry time in milliseconds
    /// * `yes_price` - Market price for YES token (0-1)
    /// * `no_price` - Market price for NO token (0-1)
    /// * `yes_liquidity` - Available YES liquidity
    /// * `no_liquidity` - Available NO liquidity
    /// * `distribution` - Price distribution from model
    /// * `now_ms` - Current timestamp
    /// * `yes_token_id` - Token ID for YES
    /// * `no_token_id` - Token ID for NO
    pub fn scan_binary_option(
        &self,
        market_id: &str,
        description: &str,
        strike: f64,
        expiry_timestamp: i64,
        yes_price: f64,
        no_price: f64,
        yes_liquidity: f64,
        no_liquidity: f64,
        distribution: &PriceDistribution,
        now_ms: i64,
        yes_token_id: Option<&str>,
        no_token_id: Option<&str>,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();

        // Calculate time to expiry
        let time_to_expiry = (expiry_timestamp - now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
        
        if time_to_expiry <= 0.0 || time_to_expiry > self.config.max_time_to_expiry {
            return opportunities;
        }

        // Get model probability from distribution
        let model_prob = match distribution.probability_above(strike, expiry_timestamp) {
            Some(p) => p,
            None => return opportunities,
        };

        // Check YES opportunity (buy if underpriced, i.e., market < fair)
        if yes_price > 0.0 && yes_price < 1.0 && yes_liquidity >= self.config.min_liquidity {
            let edge = (model_prob - yes_price) / yes_price;
            
            if edge >= self.config.min_edge {
                opportunities.push(Opportunity {
                    id: format!("{}_yes", market_id),
                    opportunity_type: OpportunityType::BinaryYes,
                    exchange: "polymarket".to_string(),
                    instrument_id: market_id.to_string(),
                    description: format!("{} - YES", description),
                    strike,
                    strike2: None,
                    expiry_timestamp,
                    time_to_expiry,
                    direction: TradeDirection::Buy,
                    market_price: yes_price,
                    fair_value: model_prob,
                    edge,
                    max_profit: 1.0 - yes_price,
                    max_loss: yes_price,
                    liquidity: yes_liquidity,
                    implied_probability: Some(yes_price),
                    model_probability: Some(model_prob),
                    model_iv: None,
                    token_id: yes_token_id.map(|s| s.to_string()),
                });
            }
        }

        // Check NO opportunity (buy if underpriced)
        let model_prob_no = 1.0 - model_prob;
        if no_price > 0.0 && no_price < 1.0 && no_liquidity >= self.config.min_liquidity {
            let edge = (model_prob_no - no_price) / no_price;
            
            if edge >= self.config.min_edge {
                opportunities.push(Opportunity {
                    id: format!("{}_no", market_id),
                    opportunity_type: OpportunityType::BinaryNo,
                    exchange: "polymarket".to_string(),
                    instrument_id: market_id.to_string(),
                    description: format!("{} - NO", description),
                    strike,
                    strike2: None,
                    expiry_timestamp,
                    time_to_expiry,
                    direction: TradeDirection::Buy,
                    market_price: no_price,
                    fair_value: model_prob_no,
                    edge,
                    max_profit: 1.0 - no_price,
                    max_loss: no_price,
                    liquidity: no_liquidity,
                    implied_probability: Some(no_price),
                    model_probability: Some(model_prob_no),
                    model_iv: None,
                    token_id: no_token_id.map(|s| s.to_string()),
                });
            }
        }

        opportunities
    }

    /// Scans a vanilla option for opportunities.
    pub fn scan_vanilla_option(
        &self,
        instrument_id: &str,
        option_type: OptionType,
        strike: f64,
        expiry_timestamp: i64,
        market_bid: f64,
        market_ask: f64,
        liquidity: f64,
        surface: &VolatilitySurface,
        now_ms: i64,
    ) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();

        let time_to_expiry = (expiry_timestamp - now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
        
        if time_to_expiry <= 0.0 || time_to_expiry > self.config.max_time_to_expiry {
            return opportunities;
        }

        if liquidity < self.config.min_liquidity {
            return opportunities;
        }

        // Get model IV
        let model_iv = match surface.get_iv_interpolated(strike, time_to_expiry, now_ms) {
            Some(iv) => iv,
            None => return opportunities,
        };

        let spot = surface.spot();
        let bs = BlackScholes::new(spot, strike, time_to_expiry, self.config.rate, model_iv, option_type);
        let fair_value = bs.price();

        let opportunity_type = match option_type {
            OptionType::Call => OpportunityType::VanillaCall,
            OptionType::Put => OpportunityType::VanillaPut,
        };

        // Buy opportunity: market ask < fair value
        if market_ask > 0.0 && market_ask < fair_value {
            let edge = (fair_value - market_ask) / market_ask;
            
            if edge >= self.config.min_edge {
                opportunities.push(Opportunity {
                    id: format!("{}_buy", instrument_id),
                    opportunity_type,
                    exchange: "derive".to_string(),
                    instrument_id: instrument_id.to_string(),
                    description: format!("Buy {}", instrument_id),
                    strike,
                    strike2: None,
                    expiry_timestamp,
                    time_to_expiry,
                    direction: TradeDirection::Buy,
                    market_price: market_ask,
                    fair_value,
                    edge,
                    max_profit: f64::INFINITY, // Unlimited for long options
                    max_loss: market_ask,
                    liquidity,
                    implied_probability: None,
                    model_probability: None,
                    model_iv: Some(model_iv),
                    token_id: None,
                });
            }
        }

        // Sell opportunity: market bid > fair value
        if market_bid > 0.0 && market_bid > fair_value {
            let edge = (market_bid - fair_value) / market_bid;
            
            if edge >= self.config.min_edge {
                opportunities.push(Opportunity {
                    id: format!("{}_sell", instrument_id),
                    opportunity_type,
                    exchange: "derive".to_string(),
                    instrument_id: instrument_id.to_string(),
                    description: format!("Sell {}", instrument_id),
                    strike,
                    strike2: None,
                    expiry_timestamp,
                    time_to_expiry,
                    direction: TradeDirection::Sell,
                    market_price: market_bid,
                    fair_value,
                    edge,
                    max_profit: market_bid,
                    max_loss: f64::INFINITY, // Unlimited for short options
                    liquidity,
                    implied_probability: None,
                    model_probability: None,
                    model_iv: Some(model_iv),
                    token_id: None,
                });
            }
        }

        opportunities
    }

    /// Scans for credit spread opportunities.
    /// 
    /// Call credit spread: Sell lower strike call, buy higher strike call.
    /// Put credit spread: Sell higher strike put, buy lower strike put.
    pub fn scan_credit_spread(
        &self,
        short_instrument: &str,
        long_instrument: &str,
        option_type: OptionType,
        short_strike: f64,
        long_strike: f64,
        expiry_timestamp: i64,
        short_bid: f64,
        long_ask: f64,
        liquidity: f64,
        surface: &VolatilitySurface,
        now_ms: i64,
    ) -> Option<Opportunity> {
        let time_to_expiry = (expiry_timestamp - now_ms) as f64 / (365.25 * 24.0 * 3600.0 * 1000.0);
        
        if time_to_expiry <= 0.0 || time_to_expiry > self.config.max_time_to_expiry {
            return None;
        }

        if liquidity < self.config.min_liquidity {
            return None;
        }

        let width = (long_strike - short_strike).abs();
        if width > self.config.max_spread_width {
            return None;
        }

        // Market credit received
        let market_credit = short_bid - long_ask;
        if market_credit <= 0.0 {
            return None; // Must be a credit spread
        }

        // Model fair values
        let short_iv = surface.get_iv_interpolated(short_strike, time_to_expiry, now_ms)?;
        let long_iv = surface.get_iv_interpolated(long_strike, time_to_expiry, now_ms)?;

        let spot = surface.spot();
        let short_bs = BlackScholes::new(spot, short_strike, time_to_expiry, self.config.rate, short_iv, option_type);
        let long_bs = BlackScholes::new(spot, long_strike, time_to_expiry, self.config.rate, long_iv, option_type);

        let fair_credit = short_bs.price() - long_bs.price();

        // Max loss = width - credit received
        let max_loss = width - market_credit;
        if max_loss <= 0.0 {
            return None; // Guaranteed profit, unlikely
        }

        // Edge = (market_credit - fair_credit) / width
        let edge = (market_credit - fair_credit) / width;

        if edge < self.config.min_edge {
            return None;
        }

        let opportunity_type = match option_type {
            OptionType::Call => OpportunityType::CallCreditSpread,
            OptionType::Put => OpportunityType::PutCreditSpread,
        };

        Some(Opportunity {
            id: format!("{}_{}_spread", short_instrument, long_instrument),
            opportunity_type,
            exchange: "derive".to_string(),
            instrument_id: format!("{}/{}", short_instrument, long_instrument),
            description: format!(
                "{:?} spread {:.0}/{:.0}",
                option_type, short_strike, long_strike
            ),
            strike: short_strike,
            strike2: Some(long_strike),
            expiry_timestamp,
            time_to_expiry,
            direction: TradeDirection::Sell, // Selling the spread
            market_price: market_credit,
            fair_value: fair_credit,
            edge,
            max_profit: market_credit,
            max_loss,
            liquidity,
            implied_probability: None,
            model_probability: None,
            model_iv: Some((short_iv + long_iv) / 2.0),
            token_id: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::vol_surface::{VolSmile, VolatilitySurface};

    fn create_test_distribution() -> (PriceDistribution, i64, i64) {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.25 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.25, 100_000.0);
        for strike in (80_000..=120_000).step_by(5000) {
            smile.add_point(strike as f64, 0.50, None, None);
        }
        surface.add_smile(expiry_ms, smile);

        let dist = PriceDistribution::from_vol_surface(&surface, now_ms, 0.0);
        (dist, now_ms, expiry_ms)
    }

    #[test]
    fn test_binary_option_scan() {
        let (dist, now_ms, expiry_ms) = create_test_distribution();
        let scanner = OpportunityScanner::new(ScannerConfig {
            min_edge: 0.05,
            ..Default::default()
        });

        // Underpriced YES (model says ~50% but market is 40%)
        let opps = scanner.scan_binary_option(
            "test_market",
            "BTC above 100k",
            100_000.0,
            expiry_ms,
            0.40, // YES price underpriced
            0.60, // NO price
            100.0,
            100.0,
            &dist,
            now_ms,
            Some("yes_token"),
            Some("no_token"),
        );

        assert!(!opps.is_empty());
        assert_eq!(opps[0].opportunity_type, OpportunityType::BinaryYes);
        assert!(opps[0].edge > 0.05);
    }

    #[test]
    fn test_vanilla_option_scan() {
        let mut surface = VolatilitySurface::new(100_000.0);
        let now_ms = 1704067200000i64;
        let expiry_ms = now_ms + (0.25 * 365.25 * 24.0 * 3600.0 * 1000.0) as i64;

        let mut smile = VolSmile::new(0.25, 100_000.0);
        smile.add_point(100_000.0, 0.50, None, None);
        surface.add_smile(expiry_ms, smile);

        let scanner = OpportunityScanner::new(ScannerConfig {
            min_edge: 0.05,
            ..Default::default()
        });

        // Get fair value first
        let bs = BlackScholes::new(100_000.0, 100_000.0, 0.25, 0.0, 0.50, OptionType::Call);
        let fair = bs.price();

        // Market ask is 20% below fair value
        let market_ask = fair * 0.75;

        let opps = scanner.scan_vanilla_option(
            "BTC-20240401-100000-C",
            OptionType::Call,
            100_000.0,
            expiry_ms,
            market_ask * 0.95, // bid
            market_ask,        // ask
            10.0,
            &surface,
            now_ms,
        );

        assert!(!opps.is_empty());
        assert_eq!(opps[0].direction, TradeDirection::Buy);
    }
}

