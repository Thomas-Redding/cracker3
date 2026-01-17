// src/traits.rs

use async_trait::async_trait;
use crate::models::{Exchange, Instrument, MarketEvent, Order, OrderId, Position};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

/// A stream of market events from an exchange.
/// 
/// Supports dynamic subscription management for adding/removing instruments
/// at runtime without reconnecting.
/// 
/// All methods take `&self` to allow concurrent access - implementations should
/// use interior mutability (e.g., channels, mutexes) as needed.
#[async_trait]
pub trait MarketStream: Send + Sync {
    /// Receive the next market event from the stream.
    /// 
    /// Implementations should use interior mutability (e.g., `Mutex<Receiver>`)
    /// to allow this to be called concurrently with subscribe/unsubscribe.
    async fn next(&self) -> Option<MarketEvent>;

    /// Subscribe to additional instruments.
    /// 
    /// The stream will start receiving events for these instruments.
    /// Instruments not matching this stream's exchange are ignored.
    async fn subscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        // Default implementation does nothing
        let _ = instruments;
        Ok(())
    }

    /// Unsubscribe from instruments.
    /// 
    /// The stream will stop receiving events for these instruments.
    /// Instruments not matching this stream's exchange are ignored.
    async fn unsubscribe(&self, instruments: &[Instrument]) -> Result<(), String> {
        // Default implementation does nothing
        let _ = instruments;
        Ok(())
    }
}

/// Execution client for placing orders.
/// Implementations should be Clone + Send + Sync so they can be shared across strategies.
#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn place_order(&self, order: Order) -> Result<OrderId, String>;
    async fn cancel_order(&self, order_id: &OrderId, instrument: &Instrument) -> Result<(), String>;
    async fn get_position(&self, instrument: &Instrument) -> Result<Position, String>;
    async fn get_positions(&self) -> Result<Vec<Position>, String>;
    async fn get_balance(&self) -> Result<f64, String>;
}

/// Wrapper to make any ExecutionClient shareable across strategies.
/// Strategies receive an Arc<dyn ExecutionClient> so multiple can share one connection.
pub type SharedExecutionClient = Arc<dyn ExecutionClient>;

// =============================================================================
// Execution Router
// =============================================================================

/// Routes orders to the appropriate exchange based on instrument type.
/// 
/// Strategies hold an `Arc<ExecutionRouter>` instead of a single `SharedExecutionClient`,
/// allowing them to place orders on any exchange they've declared via `required_exchanges()`.
pub struct ExecutionRouter {
    clients: std::collections::HashMap<Exchange, SharedExecutionClient>,
}

impl ExecutionRouter {
    /// Creates a new ExecutionRouter with the given clients.
    pub fn new(clients: std::collections::HashMap<Exchange, SharedExecutionClient>) -> Self {
        Self { clients }
    }

    /// Creates an empty ExecutionRouter (useful for backtesting with MockExec).
    pub fn empty() -> Self {
        Self {
            clients: std::collections::HashMap::new(),
        }
    }

    /// Adds a client for an exchange.
    pub fn with_client(mut self, exchange: Exchange, client: SharedExecutionClient) -> Self {
        self.clients.insert(exchange, client);
        self
    }

    /// Places an order, routing to the appropriate exchange based on instrument.
    pub async fn place_order(&self, order: Order) -> Result<OrderId, String> {
        let exchange = order.instrument.exchange();
        self.clients
            .get(&exchange)
            .ok_or_else(|| format!("No execution client for {:?}", exchange))?
            .place_order(order)
            .await
    }

    /// Returns true if this router has a client for the given exchange.
    pub fn has_exchange(&self, exchange: Exchange) -> bool {
        self.clients.contains_key(&exchange)
    }

    /// Returns the set of exchanges this router can handle.
    pub fn exchanges(&self) -> HashSet<Exchange> {
        self.clients.keys().copied().collect()
    }

    /// Cancels an order.
    pub async fn cancel_order(&self, order_id: &OrderId, instrument: &Instrument) -> Result<(), String> {
        let exchange = instrument.exchange();
        self.clients
            .get(&exchange)
            .ok_or_else(|| format!("No execution client for {:?}", exchange))?
            .cancel_order(order_id, instrument)
            .await
    }

    /// Gets current position for an instrument.
    pub async fn get_position(&self, instrument: &Instrument) -> Result<Position, String> {
        let exchange = instrument.exchange();
        self.clients
            .get(&exchange)
            .ok_or_else(|| format!("No execution client for {:?}", exchange))?
            .get_position(instrument)
            .await
    }

    /// Gets account balance for an exchange.
    pub async fn get_balance(&self, exchange: Exchange) -> Result<f64, String> {
        self.clients
            .get(&exchange)
            .ok_or_else(|| format!("No execution client for {:?}", exchange))?
            .get_balance()
            .await
    }

    /// Gets all positions across all exchanges.
    pub async fn get_positions(&self) -> Result<Vec<Position>, String> {
        let mut all_positions = Vec::new();
        for client in self.clients.values() {
            let positions = client.get_positions().await?;
            all_positions.extend(positions);
        }
        Ok(all_positions)
    }
}

/// Shared ExecutionRouter for use across strategies.
pub type SharedExecutionRouter = Arc<ExecutionRouter>;

// =============================================================================
// Strategy Trait
// =============================================================================

/// Portfolio metrics that strategies can optionally expose for tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioMetrics {
    pub expected_utility: f64,
    pub expected_return: f64,
    pub prob_loss: f64,
}

/// The core Strategy trait for the multi-strategy engine.
/// 
/// Strategies declare which exchanges they need (statically) and which instruments
/// they want to subscribe to (dynamically via catalog queries).
#[async_trait]
pub trait Strategy: Dashboard + Send + Sync {
    /// Returns the name of this strategy (for logging and identification).
    fn name(&self) -> &str;

    /// Static: which exchanges this strategy needs connections to.
    /// 
    /// This doesn't change at runtime. The engine uses this to determine
    /// which exchange connections to establish.
    fn required_exchanges(&self) -> HashSet<Exchange>;

    /// Dynamic: discover instruments to subscribe to.
    /// 
    /// Called periodically by the engine to allow strategies to update
    /// their subscriptions based on catalog queries, market conditions, etc.
    /// 
    /// The default implementation returns an empty set (no subscriptions).
    async fn discover_subscriptions(&self) -> Vec<Instrument> {
        Vec::new()
    }

    /// Called by the engine when a market event arrives for an instrument
    /// this strategy is subscribed to.
    async fn on_event(&self, event: MarketEvent);

    /// Optional initialization step.
    /// Called by the engine/runner before trading starts.
    /// Useful for initial catalog discovery or pre-computation.
    async fn initialize(&self) {}

    /// Optional: Get current portfolio metrics for tracking/visualization.
    /// Returns None if the strategy doesn't calculate these metrics.
    async fn get_portfolio_metrics(&self) -> Option<PortfolioMetrics> {
        None
    }
}

// =============================================================================
// Dashboard Support
// =============================================================================

/// Dashboard trait that all strategies must implement for web UI support.
/// Each strategy exposes its current state and defines how it should be rendered.
#[async_trait]
pub trait Dashboard: Send + Sync {
    /// Display name for the dashboard tab
    fn dashboard_name(&self) -> &str;

    /// Returns the current state as JSON for the frontend to render.
    /// This is called periodically or on-demand by the dashboard server.
    async fn dashboard_state(&self) -> Value;

    /// Optional: Define the dashboard layout schema.
    /// If not overridden, returns a default schema that renders the state as key-value pairs.
    fn dashboard_schema(&self) -> DashboardSchema {
        DashboardSchema::default()
    }
}

/// Schema describing the dashboard layout and widgets to render.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct DashboardSchema {
    /// List of widgets to render in the dashboard
    pub widgets: Vec<Widget>,
}

/// Widget types supported by the dashboard frontend.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Widget {
    /// Simple key-value display (label: value)
    KeyValue {
        label: String,
        /// JSON path to the value in dashboard_state()
        key: String,
        /// Optional format string (e.g., "{:.2}%" for percentages)
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<String>,
    },
    /// Time-series chart
    Chart {
        title: String,
        /// JSON path to the array of data points
        data_key: String,
        /// Chart type: "line", "bar", "area"
        #[serde(default = "default_chart_type")]
        chart_type: String,
    },
    /// Data table
    Table {
        title: String,
        /// Column definitions
        columns: Vec<TableColumn>,
        /// JSON path to the array of row data
        data_key: String,
    },
    /// Scrolling log of recent events
    Log {
        title: String,
        /// JSON path to the log entries array
        data_key: String,
        /// Maximum lines to display
        #[serde(default = "default_log_lines")]
        max_lines: usize,
    },
    /// Horizontal divider for layout
    Divider,
}

fn default_chart_type() -> String {
    "line".to_string()
}

fn default_log_lines() -> usize {
    50
}

/// Column definition for table widgets.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableColumn {
    /// Display header
    pub header: String,
    /// JSON key in each row object
    pub key: String,
    /// Optional format string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}
