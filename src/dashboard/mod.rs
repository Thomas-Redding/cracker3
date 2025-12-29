// src/dashboard/mod.rs

//! Dashboard server module for strategy web UIs.
//!
//! Provides a web server that:
//! - Serves a single-page frontend with tabs for each strategy
//! - Exposes REST endpoints for strategy state
//! - Supports WebSocket connections for real-time updates

use crate::traits::Strategy;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use log::{error, info};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

/// Message broadcast to WebSocket clients when strategy state updates.
#[derive(Clone, Debug, Serialize)]
pub struct DashboardUpdate {
    pub strategy_name: String,
    pub state: serde_json::Value,
    pub timestamp: u64,
}

/// Shared state for the dashboard server.
pub struct DashboardState {
    strategies: Vec<Arc<dyn Strategy>>,
    update_tx: broadcast::Sender<DashboardUpdate>,
}

/// The dashboard server that serves the web UI and API endpoints.
pub struct DashboardServer {
    state: Arc<DashboardState>,
    port: u16,
}

impl DashboardServer {
    /// Creates a new dashboard server.
    ///
    /// # Arguments
    /// * `strategies` - List of strategies to expose in the dashboard
    /// * `port` - Port to listen on (default: 8080)
    pub fn new(strategies: Vec<Arc<dyn Strategy>>, port: u16) -> Self {
        let (update_tx, _) = broadcast::channel(100);

        Self {
            state: Arc::new(DashboardState {
                strategies,
                update_tx,
            }),
            port,
        }
    }

    /// Returns a sender that can be used to push updates to connected clients.
    pub fn update_sender(&self) -> broadcast::Sender<DashboardUpdate> {
        self.state.update_tx.clone()
    }

    /// Starts the dashboard web server.
    /// This method runs until the server is shut down.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = self.state.clone();

        // Spawn background task to periodically poll strategy states
        let poll_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                for strategy in &poll_state.strategies {
                    let state_value = strategy.dashboard_state().await;
                    let update = DashboardUpdate {
                        strategy_name: strategy.dashboard_name().to_string(),
                        state: state_value,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    };
                    // Ignore send errors (no receivers)
                    let _ = poll_state.update_tx.send(update);
                }
            }
        });

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        let app = Router::new()
            .route("/", get(serve_frontend))
            .route("/api/strategies", get(list_strategies))
            .route("/api/strategies/{name}", get(get_strategy_state))
            .route("/api/strategies/{name}/schema", get(get_strategy_schema))
            .route("/ws", get(websocket_handler))
            .layer(cors)
            .with_state(state);

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.port));
        info!("Dashboard server starting on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

/// GET / - Serves the main dashboard frontend
async fn serve_frontend() -> Html<&'static str> {
    Html(FRONTEND_HTML)
}

/// Strategy info returned by the API
#[derive(Serialize)]
struct StrategyInfo {
    name: String,
    instruments: Vec<String>,
}

/// GET /api/strategies - Lists all strategies
async fn list_strategies(State(state): State<Arc<DashboardState>>) -> Json<Vec<StrategyInfo>> {
    let strategies: Vec<StrategyInfo> = state
        .strategies
        .iter()
        .map(|s| StrategyInfo {
            name: s.dashboard_name().to_string(),
            instruments: s.required_subscriptions(),
        })
        .collect();

    Json(strategies)
}

/// GET /api/strategies/:name - Gets a strategy's current state
async fn get_strategy_state(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
) -> Response {
    for strategy in &state.strategies {
        if strategy.dashboard_name() == name {
            let state_value = strategy.dashboard_state().await;
            return Json(state_value).into_response();
        }
    }

    (StatusCode::NOT_FOUND, "Strategy not found").into_response()
}

/// GET /api/strategies/:name/schema - Gets a strategy's dashboard schema
async fn get_strategy_schema(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
) -> Response {
    for strategy in &state.strategies {
        if strategy.dashboard_name() == name {
            let schema = strategy.dashboard_schema();
            return Json(schema).into_response();
        }
    }

    (StatusCode::NOT_FOUND, "Strategy not found").into_response()
}

/// GET /ws - WebSocket endpoint for real-time updates
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<DashboardState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

async fn handle_websocket(mut socket: WebSocket, state: Arc<DashboardState>) {
    info!("WebSocket client connected");

    // Send initial state for all strategies
    for strategy in &state.strategies {
        let state_value = strategy.dashboard_state().await;
        let update = DashboardUpdate {
            strategy_name: strategy.dashboard_name().to_string(),
            state: state_value,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        if let Ok(json) = serde_json::to_string(&update) {
            if socket.send(Message::Text(json.into())).await.is_err() {
                return;
            }
        }
    }

    // Subscribe to updates
    let mut rx = state.update_tx.subscribe();

    loop {
        tokio::select! {
            // Forward broadcast updates to the WebSocket client
            result = rx.recv() => {
                match result {
                    Ok(update) => {
                        if let Ok(json) = serde_json::to_string(&update) {
                            if socket.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        info!("WebSocket client lagged, skipped {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // Handle incoming messages from client (ping/pong, close)
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Err(e)) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    info!("WebSocket client disconnected");
}

/// Embedded frontend HTML with CSS and JavaScript
const FRONTEND_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0a0f;
            --bg-secondary: #12121a;
            --bg-tertiary: #1a1a26;
            --bg-card: #16161f;
            --border: #2a2a3d;
            --border-active: #4f46e5;
            --text-primary: #f0f0f5;
            --text-secondary: #8888a0;
            --text-muted: #555566;
            --accent: #6366f1;
            --accent-glow: rgba(99, 102, 241, 0.3);
            --success: #22c55e;
            --warning: #f59e0b;
            --danger: #ef4444;
            --chart-line: #818cf8;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: 
                radial-gradient(ellipse at 20% 0%, rgba(99, 102, 241, 0.08) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 100%, rgba(139, 92, 246, 0.06) 0%, transparent 50%);
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 32px;
            padding-bottom: 24px;
            border-bottom: 1px solid var(--border);
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .logo-icon {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, var(--accent), #8b5cf6);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
        }

        h1 {
            font-size: 24px;
            font-weight: 600;
            letter-spacing: -0.5px;
        }

        .status {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 14px;
            color: var(--text-secondary);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--success);
            box-shadow: 0 0 8px var(--success);
            animation: pulse 2s infinite;
        }

        .status-dot.disconnected {
            background: var(--danger);
            box-shadow: 0 0 8px var(--danger);
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 24px;
            background: var(--bg-secondary);
            padding: 6px;
            border-radius: 12px;
            border: 1px solid var(--border);
        }

        .tab {
            padding: 12px 24px;
            border: none;
            background: transparent;
            color: var(--text-secondary);
            font-family: inherit;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            border-radius: 8px;
            transition: all 0.2s ease;
        }

        .tab:hover {
            color: var(--text-primary);
            background: var(--bg-tertiary);
        }

        .tab.active {
            background: var(--accent);
            color: white;
            box-shadow: 0 4px 12px var(--accent-glow);
        }

        .dashboard {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 24px;
            transition: border-color 0.2s ease, box-shadow 0.2s ease;
        }

        .card:hover {
            border-color: var(--border-active);
            box-shadow: 0 0 20px var(--accent-glow);
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .card-title {
            font-size: 14px;
            font-weight: 500;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .card-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 28px;
            font-weight: 600;
            color: var(--text-primary);
        }

        .card-value.positive { color: var(--success); }
        .card-value.negative { color: var(--danger); }

        .metric-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }

        .metric {
            padding: 16px;
            background: var(--bg-tertiary);
            border-radius: 10px;
        }

        .metric-label {
            font-size: 12px;
            color: var(--text-muted);
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .metric-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 18px;
            font-weight: 500;
        }

        .log-container {
            grid-column: 1 / -1;
        }

        .log {
            font-family: 'JetBrains Mono', monospace;
            font-size: 13px;
            line-height: 1.8;
            max-height: 300px;
            overflow-y: auto;
            background: var(--bg-tertiary);
            border-radius: 10px;
            padding: 16px;
        }

        .log-entry {
            padding: 4px 0;
            border-bottom: 1px solid var(--border);
            display: flex;
            gap: 12px;
        }

        .log-entry:last-child {
            border-bottom: none;
        }

        .log-time {
            color: var(--text-muted);
            flex-shrink: 0;
        }

        .log-message {
            color: var(--text-secondary);
        }

        .log-message.signal {
            color: var(--warning);
        }

        .instruments {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 12px;
        }

        .instrument-tag {
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            padding: 6px 10px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--text-secondary);
        }

        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: var(--text-muted);
        }

        .empty-state h2 {
            font-size: 18px;
            margin-bottom: 8px;
            color: var(--text-secondary);
        }

        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }

        ::-webkit-scrollbar-track {
            background: var(--bg-tertiary);
            border-radius: 4px;
        }

        ::-webkit-scrollbar-thumb {
            background: var(--border);
            border-radius: 4px;
        }

        ::-webkit-scrollbar-thumb:hover {
            background: var(--text-muted);
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo">
                <div class="logo-icon">📈</div>
                <h1>Trading Dashboard</h1>
            </div>
            <div class="status">
                <div class="status-dot" id="statusDot"></div>
                <span id="statusText">Connecting...</span>
            </div>
        </header>

        <div class="tabs" id="tabs"></div>

        <div class="dashboard" id="dashboard">
            <div class="empty-state">
                <h2>Loading strategies...</h2>
                <p>Connecting to trading engine</p>
            </div>
        </div>
    </div>

    <script>
        // State
        let strategies = [];
        let strategyStates = {};
        let activeStrategy = null;
        let ws = null;

        // Initialize
        async function init() {
            await loadStrategies();
            connectWebSocket();
        }

        async function loadStrategies() {
            try {
                const res = await fetch('/api/strategies');
                strategies = await res.json();
                
                if (strategies.length > 0) {
                    activeStrategy = strategies[0].name;
                    renderTabs();
                }
            } catch (err) {
                console.error('Failed to load strategies:', err);
            }
        }

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

            ws.onopen = () => {
                document.getElementById('statusDot').classList.remove('disconnected');
                document.getElementById('statusText').textContent = 'Connected';
            };

            ws.onclose = () => {
                document.getElementById('statusDot').classList.add('disconnected');
                document.getElementById('statusText').textContent = 'Disconnected';
                // Reconnect after 2 seconds
                setTimeout(connectWebSocket, 2000);
            };

            ws.onmessage = (event) => {
                try {
                    const update = JSON.parse(event.data);
                    strategyStates[update.strategy_name] = update.state;
                    
                    if (update.strategy_name === activeStrategy) {
                        renderDashboard(update.state);
                    }
                } catch (err) {
                    console.error('Failed to parse update:', err);
                }
            };
        }

        function renderTabs() {
            const tabsEl = document.getElementById('tabs');
            tabsEl.innerHTML = strategies.map(s => `
                <button class="tab ${s.name === activeStrategy ? 'active' : ''}" 
                        onclick="selectStrategy('${s.name}')">
                    ${s.name}
                </button>
            `).join('');
        }

        function selectStrategy(name) {
            activeStrategy = name;
            renderTabs();
            
            if (strategyStates[name]) {
                renderDashboard(strategyStates[name]);
            }
        }

        function renderDashboard(state) {
            const dashboard = document.getElementById('dashboard');
            
            if (!state) {
                dashboard.innerHTML = '<div class="empty-state"><h2>No data available</h2></div>';
                return;
            }

            // Build cards from state
            let html = '';

            // Key metrics
            const metrics = [];
            for (const [key, value] of Object.entries(state)) {
                if (typeof value === 'number' || typeof value === 'string') {
                    metrics.push({ key, value });
                }
            }

            if (metrics.length > 0) {
                html += `
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Metrics</span>
                        </div>
                        <div class="metric-grid">
                            ${metrics.map(m => `
                                <div class="metric">
                                    <div class="metric-label">${formatLabel(m.key)}</div>
                                    <div class="metric-value">${formatValue(m.value)}</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Instruments
            const strategy = strategies.find(s => s.name === activeStrategy);
            if (strategy && strategy.instruments.length > 0) {
                html += `
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Instruments</span>
                        </div>
                        <div class="instruments">
                            ${strategy.instruments.map(i => `
                                <span class="instrument-tag">${i}</span>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Log entries
            if (state.log && Array.isArray(state.log)) {
                html += `
                    <div class="card log-container">
                        <div class="card-header">
                            <span class="card-title">Activity Log</span>
                        </div>
                        <div class="log">
                            ${state.log.slice(-50).reverse().map(entry => `
                                <div class="log-entry">
                                    <span class="log-time">${entry.time || ''}</span>
                                    <span class="log-message ${entry.type === 'signal' ? 'signal' : ''}">${entry.message}</span>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Price history chart placeholder
            if (state.price_history && Array.isArray(state.price_history)) {
                html += `
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Price History</span>
                        </div>
                        <div class="metric-grid">
                            ${state.price_history.slice(-4).map((p, i) => `
                                <div class="metric">
                                    <div class="metric-label">Tick ${i + 1}</div>
                                    <div class="metric-value">${formatValue(p)}</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            dashboard.innerHTML = html || '<div class="empty-state"><h2>No data available</h2></div>';
        }

        function formatLabel(key) {
            return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        }

        function formatValue(value) {
            if (typeof value === 'number') {
                if (Math.abs(value) < 0.01) return value.toFixed(6);
                if (Math.abs(value) < 1) return value.toFixed(4);
                if (Math.abs(value) < 100) return value.toFixed(2);
                return value.toLocaleString();
            }
            return value;
        }

        // Start
        init();
    </script>
</body>
</html>
"##;

