use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{RwLock, broadcast},
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{Message as WsMessage, Result as WsResult},
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpeedAggregation {
    window_start: String,
    window_end: String,
    session_uid: i64,
    avg_speed: f64,
    min_speed: i32,
    max_speed: i32,
    sample_count: i64,
}

type Clients = Arc<RwLock<HashMap<Uuid, broadcast::Sender<String>>>>;

#[tokio::main]
async fn main() {
    env_logger::init();

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .expect("KAFKA_BOOTSTRAP_SERVERS environment variable required");
    let speed_aggregation_topic = std::env::var("SPEED_AGGREGATION_TOPIC")
        .expect("SPEED_AGGREGATION_TOPIC environment variable required");

    let socket_url = std::env::var("SOCKET_URL").unwrap();
    let socket_port = std::env::var("SOCKET_PORT").unwrap();

    let connections: Clients = Arc::new(RwLock::new(HashMap::new()));

    let (tx, _) = broadcast::channel::<String>(1000);

    let bsc = bootstrap_servers.clone();
    let satc = speed_aggregation_topic.clone();
    let cc = connections.clone();
    let txc = tx.clone();
    let consume_handle = tokio::spawn(async move { consume(&bsc, &satc, cc, txc).await });

    let listener = TcpListener::bind(&format!("{}:{}", socket_url, socket_port))
        .await
        .unwrap();

    let server_handle = tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            log::debug!("accepted connection from: {}", addr);
            let cc = connections.clone();
            let tx_sub = tx.subscribe();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, cc, tx_sub).await {
                    log::error!("failed handling socket connection from {}: {}", addr, e);
                }
            });
        }
    });

    log::info!("socket accepting on {}:{}", socket_url, socket_port);
    log::info!(
        "subscribed to: {}, {}",
        &bootstrap_servers,
        &speed_aggregation_topic
    );

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
                    log::info!("socket server finished");
                }
                Err(e) => {
                    log::error!("socket server failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}

async fn consume(
    bootstrap_servers: &str,
    topic: &str,
    connections: Clients,
    broadcast_tx: broadcast::Sender<String>,
) -> Result<(), KafkaConsumptionException> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "f1-socket-server")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|e| KafkaConsumptionException::ConsumerCreationError(e.to_string()))?;

    consumer
        .subscribe(&[&topic])
        .map_err(|e| KafkaConsumptionException::TopicSubscriptionError(e.to_string()))?;

    loop {
        match consumer.recv().await {
            Err(e) => {
                log::error!("failed to recv from Kafka consumer consumer: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(message) => {
                log::info!("Received Kafka message from partition: {}, offset: {}", message.partition(), message.offset());
                if let Some(payload) = message.payload_view::<str>() {
                    match payload {
                        Err(e) => {
                            log::error!("failed to parse Kafka message: {}", e);
                        }
                        Ok(json_str) => {
                            log::debug!("received message: {}", json_str);

                            if let Err(e) = broadcast_tx.send(json_str.to_string()) {
                                log::error!("failed to broadcast message: {}", e);
                            }

                            let mut conn_guard = connections.write().await;
                            conn_guard.retain(|client_id, sender| {
                                if sender.receiver_count() == 0 {
                                    log::debug!("removing disconnected client: {}", client_id);
                                    false
                                } else {
                                    true
                                }
                            });
                        }
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    raw_stream: TcpStream,
    connections: Clients,
    mut tx_sub: broadcast::Receiver<String>,
) -> WsResult<()> {
    let socket_stream = accept_async(raw_stream).await?;
    let client_id = Uuid::new_v4();

    log::info!("socket connection established for client: {}", client_id);

    let (mut socket_sender, mut socket_receiver) = socket_stream.split();

    {
        let mut clients_guard = connections.write().await;
        clients_guard.insert(client_id, broadcast::channel::<String>(100).0);
    }

    let client_id_for_broadcast = client_id;
    let broadcast_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg_result = tx_sub.recv() => {
                    match msg_result {
                        Ok(message) => {
                            if let Err(e) = socket_sender.send(WsMessage::Text(message)).await {
                                log::error!("failed to send message to client {}: {}", client_id_for_broadcast, e);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            log::info!("broadcast channel closed for client: {}", client_id_for_broadcast);
                            break;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            log::warn!("client {} lagged behind, skipping messages", client_id_for_broadcast);
                        }
                    }
                }
                ws_msg = socket_receiver.next() => {
                    match ws_msg {
                        Some(Ok(msg)) => {
                            match msg {
                                WsMessage::Close(_) => {
                                    log::info!("client {} requested close", client_id_for_broadcast);
                                    break;
                                }
                                WsMessage::Ping(payload) => {
                                    if let Err(e) = socket_sender.send(WsMessage::Pong(payload)).await {
                                        log::error!("failed to send pong to client {}: {}", client_id_for_broadcast, e);
                                        break;
                                    }
                                }
                                WsMessage::Text(text) => {
                                    log::info!("received text from client {}: {}", client_id_for_broadcast, text);
                                }
                                _ => {}
                            }
                        }
                        Some(Err(e)) => {
                            log::error!("socket error for client {}: {}", client_id_for_broadcast, e);
                            break;
                        }
                        None => {
                            log::info!("socket stream ended for client: {}", client_id_for_broadcast);
                            break;
                        }
                    }
                }
            }
        }
    });

    if let Err(e) = broadcast_task.await {
        log::error!("broadcast task error for client {}: {}", client_id, e);
    }

    {
        let mut clients_guard = connections.write().await;
        clients_guard.remove(&client_id);
    }

    log::info!("client {} disconnected", client_id);

    Ok(())
}

#[derive(Debug)]
enum KafkaConsumptionException {
    ConsumerCreationError(String),
    TopicSubscriptionError(String),
}

impl std::fmt::Display for KafkaConsumptionException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaConsumptionException::ConsumerCreationError(msg) => {
                write!(f, "failed to create consumer: {}", msg)
            }
            KafkaConsumptionException::TopicSubscriptionError(msg) => {
                write!(f, "failed to subscribe to topic: {}", msg)
            }
        }
    }
}

impl std::error::Error for KafkaConsumptionException {}
