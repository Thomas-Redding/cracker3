// src/traits.rs

use async_trait::async_trait;
use crate::models::{MarketEvent, Order, OrderId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// A stream of market events from an exchange.
/// This is still used internally by the Engine to receive raw events.
#[async_trait]
pub trait MarketStream: Send + Unpin {
    async fn next(&mut self) -> Option<MarketEvent>;
}

/// Execution client for placing orders.
/// Implementations should be Clone + Send + Sync so they can be shared across strategies.
#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn place_order(&self, order: Order) -> Result<OrderId, String>;
    // ... cancel, get_positions, etc.
}

/// The core Strategy trait for the multi-strategy engine.
/// Each strategy declares its interests and reacts to market events.
#[async_trait]
pub trait Strategy: Dashboard + Send + Sync {
    /// Returns the name of this strategy (for logging).
    fn name(&self) -> &str;

    /// Returns the list of instruments/tokens this strategy needs to receive events for.
    /// The engine will aggregate these across all strategies to build the subscription list.
    fn required_subscriptions(&self) -> Vec<String>;

    /// Called by the engine when a market event arrives for an instrument this strategy watches.
    /// The strategy should process the event and optionally place orders via the exec client.
    async fn on_event(&self, event: MarketEvent);
}

/// Wrapper to make any ExecutionClient shareable across strategies.
/// Strategies receive an Arc<dyn ExecutionClient> so multiple can share one connection.
pub type SharedExecutionClient = Arc<dyn ExecutionClient>;

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
