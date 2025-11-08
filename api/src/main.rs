use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    extract::State,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::get,
    Router,
};
use duckdb::Connection;
use futures_util::{stream::Stream, StreamExt as _};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use thiserror::Error;
use tokio::sync::broadcast;

use crate::{
    aggregation::{rpm_aggregation::RPMAggregation, speed_aggregation::SpeedAggregation},
    sse_message::SSEMessage,
};

mod aggregation;
mod sse_message;

#[derive(Clone)]
struct AppState {
    broadcast_tx: broadcast::Sender<String>,
    warehouse_path: Arc<String>,
}

impl AppState {
    async fn new(broadcast_tx: broadcast::Sender<String>) -> Result<Self, InitializationError> {
        let warehouse_path = std::env::var("WAREHOUSE_PATH")
            .unwrap_or_else(|_| "/opt/warehouse".to_string());
            
        let setup_conn = Connection::open("api_warehouse.db")
            .map_err(|e| InitializationError::DatabaseOpen(e.to_string()))?;
        
        setup_conn.execute_batch("
            INSTALL iceberg;
        ").map_err(|e| InitializationError::IcebergInstall(e.to_string()))?;
        
        log::info!("DuckDB warehouse database initialized at: {}", warehouse_path);
        
        Ok(AppState {
            broadcast_tx,
            warehouse_path: Arc::new(warehouse_path),
        })
    }
    
    fn create_connection(&self) -> Result<Connection, InitializationError> {
        let conn = Connection::open("api_warehouse.db")
            .map_err(|e| InitializationError::DatabaseOpen(e.to_string()))?;
        
        conn.execute_batch("LOAD iceberg;")
            .map_err(|e| InitializationError::IcebergLoad(e.to_string()))?;
        
        Ok(conn)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .expect("KAFKA_BOOTSTRAP_SERVERS environment variable required");
    let speed_aggregation_topic = std::env::var("SPEED_AGGREGATION_TOPIC")
        .expect("SPEED_AGGREGATION_TOPIC environment variable required");
    let rpm_aggregation_topic = std::env::var("RPM_AGGREGATION_TOPIC")
        .expect("RPM_AGGREGATION_TOPIC environment variable required");

    let api_url = std::env::var("API_URL").unwrap();
    let api_port = std::env::var("API_PORT").unwrap();

    let (tx, _) = broadcast::channel::<String>(1000);

    let state = AppState::new(tx.clone()).await.unwrap();

    let bsc = bootstrap_servers.clone();
    let satc = speed_aggregation_topic.clone();
    let ratc = rpm_aggregation_topic.clone();
    let txc = tx.clone();
    let consume_handle = tokio::spawn(async move {
        let speed_task = consume_speed(&bsc, &satc, txc.clone());
        let rpm_task = consume_rpm(&bsc, &ratc, txc.clone());

        tokio::select! {
            result = speed_task => {
                log::error!("speed consumer terminated: {:?}", result);
                result
            }
            result = rpm_task => {
                log::error!("rpm consumer terminated: {:?}", result);
                result
            }
        }
    });

    let app = Router::new()
        .route("/api/events", get(sse_handler))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state);

    let addr = format!("{}:{}", api_url, api_port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    log::info!("SSE server listening on {}", addr);
    log::info!(
        "subscribed to: {}, {} and {}",
        &bootstrap_servers,
        &speed_aggregation_topic,
        &rpm_aggregation_topic
    );

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::select! {
        consume_result = consume_handle => {
            match consume_result {
                Ok(Ok(())) => {
                    log::info!("Kafka consumer finished");
                }
                Ok(Err(e)) => {
                    log::error!("Kafka consumer failed: {}", e);
                    std::process::exit(1);
                }
                Err(e) => {
                    log::error!("Kafka consumer panicked: {}", e);
                    std::process::exit(1);
                }
            }
        }
        server_result = server_handle => {
            match server_result {
                Ok(()) => {
                    log::info!("SSE server finished");
                }
                Err(e) => {
                    log::error!("SSE server failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast_tx.subscribe();
    
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(message) => Some((Ok(Event::default().data(message)), rx)),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                log::warn!("SSE client lagged behind, skipped {} messages", n);
                Some((Err(()), rx))
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    })
    .filter_map(|result| async move {
        match result {
            Ok(event) => Some(Ok(event)),
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn healthz() -> impl IntoResponse {
    "OK"
}

async fn readyz() -> impl IntoResponse {
    "OK"
}

async fn consume_speed(
    bootstrap_servers: &str,
    topic: &str,
    broadcast_tx: broadcast::Sender<String>,
) -> Result<(), KafkaConsumptionError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "f1-api-server-speed")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "latest")
        .create()
        .map_err(|e| KafkaConsumptionError::ConsumerCreation(e.to_string()))?;

    consumer
        .subscribe(&[&topic])
        .map_err(|e| KafkaConsumptionError::TopicSubscription(e.to_string()))?;

    loop {
        match consumer.recv().await {
            Err(e) => {
                log::error!("speed consumer error: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(message) => {
                if let Some(payload) = message.payload() {
                    log::debug!("received speed message from kafka");
                    match std::str::from_utf8(payload) {
                        Err(e) => {
                            log::error!("failed to decode speed message payload as UTF-8: {}", e);
                        }
                        Ok(payload_str) => {
                            match serde_json::from_str::<SpeedAggregation>(payload_str) {
                                Err(e) => {
                                    log::error!(
                                        "failed to deserialize speed aggregation from kafka: {}",
                                        e
                                    );
                                }
                                Ok(speed_data) => {
                                    let telemetry_message = SSEMessage::Speed(speed_data);
                                    match serde_json::to_string(&telemetry_message) {
                                        Err(e) => {
                                            log::error!(
                                                "failed to serialize speed telemetry message: {}",
                                                e
                                            );
                                        }
                                        Ok(json_str) => {
                                            if let Err(e) = broadcast_tx.send(json_str) {
                                                log::debug!(
                                                    "no active SSE clients to receive speed message: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    log::warn!("received speed message with no payload");
                }
            }
        }
    }
}

async fn consume_rpm(
    bootstrap_servers: &str,
    topic: &str,
    broadcast_tx: broadcast::Sender<String>,
) -> Result<(), KafkaConsumptionError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "f1-api-server-rpm")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "latest")
        .create()
        .map_err(|e| KafkaConsumptionError::ConsumerCreation(e.to_string()))?;

    consumer
        .subscribe(&[&topic])
        .map_err(|e| KafkaConsumptionError::TopicSubscription(e.to_string()))?;

    loop {
        match consumer.recv().await {
            Err(e) => {
                log::error!("rpm consumer error: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(message) => {
                if let Some(payload) = message.payload() {
                    log::debug!("received rpm message from kafka");
                    match std::str::from_utf8(payload) {
                        Err(e) => {
                            log::error!("failed to decode rpm message payload as UTF-8: {}", e);
                        }
                        Ok(payload_str) => {
                            match serde_json::from_str::<RPMAggregation>(payload_str) {
                                Err(e) => {
                                    log::error!(
                                        "failed to deserialize rpm aggregation from kafka: {}",
                                        e
                                    );
                                }
                                Ok(rpm_data) => {
                                    let telemetry_message = SSEMessage::Rpm(rpm_data);
                                    match serde_json::to_string(&telemetry_message) {
                                        Err(e) => {
                                            log::error!(
                                                "failed to serialize rpm telemetry message: {}",
                                                e
                                            );
                                        }
                                        Ok(json_str) => {
                                            if let Err(e) = broadcast_tx.send(json_str) {
                                                log::debug!(
                                                    "no active SSE clients to receive rpm message: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    log::warn!("received rpm message with no payload");
                }
            }
        }
    }
}

#[derive(Error, Debug)]
enum KafkaConsumptionError {
    #[error("failed to create consumer: {0}")]
    ConsumerCreation(String),

    #[error("failed to subscribe to topic: {0}")]
    TopicSubscription(String),
}

#[derive(Error, Debug)]
enum InitializationError {
    #[error("failed to open DuckDB database: {0}")]
    DatabaseOpen(String),
    
    #[error("failed to install Iceberg extension: {0}")]
    IcebergInstall(String),
    
    #[error("failed to load Iceberg extension: {0}")]
    IcebergLoad(String),
}
