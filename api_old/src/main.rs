use axum::{extract::State, http::StatusCode, response::IntoResponse};
use thiserror::Error;
use tower_http::trace::TraceLayer;
use duckdb::Connection;
use anyhow::{Result, Context};
use std::sync::Arc;

mod repo;

#[derive(Clone)]
struct AppState {
    warehouse_path: Arc<String>,
}

impl AppState {
    async fn new() -> Result<Self> {
        let warehouse_path = std::env::var("WAREHOUSE_PATH")
            .unwrap_or_else(|_| "/opt/warehouse".to_string());
            
        let setup_conn = Connection::open("api_warehouse.db")
            .context("failed to open DuckDB database file")?;
        
        setup_conn.execute_batch("
            INSTALL iceberg;
        ").context("failed to install Iceberg extension")?;
        
        log::info!("DuckDB warehouse database initialized at: {}", warehouse_path);
        
        Ok(AppState {
            warehouse_path: Arc::new(warehouse_path),
        })
    }
    
    fn create_connection(&self) -> Result<Connection> {
        let conn = Connection::open("api_warehouse.db")
            .context("failed to open database connection")?;
        
        conn.execute_batch("LOAD iceberg;")
            .context("failed to load Iceberg extension")?;
        
        Ok(conn)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let port: u16 = std::env::var("API_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .unwrap_or(3000);

    let state = AppState::new()
        .await
        .context("failed to initialize application state")?;

    let app = axum::Router::new()
        .layer(TraceLayer::new_for_http())
        .route("/speed", axum::routing::get(get_speed_aggregations))
        .with_state(state);
        
    let addr = format!("0.0.0.0:{}", port);
    log::info!("listening at: {}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind to address: {}", addr))?;
        
    axum::serve(listener, app)
        .await
        .context("server failed to run")?;
    
    Ok(())
}

async fn get_speed_aggregations(State(state): State<AppState>) -> impl IntoResponse {
    match query_speed_aggregations(&state).await {
        Ok(()) => {
            (StatusCode::OK, "Speed aggregations data available").into_response()
        },
        Err(e) => {
            log::error!("failed to query speed aggregations: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Query failed").into_response()
        }
    }
}

async fn query_speed_aggregations(state: &AppState) -> Result<(), GetAggregationError> {
    let conn = state.create_connection()
        .map_err(|e| GetAggregationError::SpeedAggregationError(e.to_string()))?;
    
    let iceberg_path = format!("{}/speed_aggregations", state.warehouse_path);
    let query = format!("SELECT * FROM iceberg_scan('{}') LIMIT 10", iceberg_path);
    
    conn.prepare(&query)
        .map_err(|e| GetAggregationError::SpeedAggregationError(e.to_string()))?;
    
    Ok(())
}

#[derive(Error, Debug)]
enum GetAggregationError {
    #[error("failed to query for speed aggregation: {0}")]
    SpeedAggregationError(String),
}
