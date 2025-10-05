use std::{env, net::UdpSocket, time::Duration};

use opentelemetry::{global::{self}, propagation::TextMapCompositePropagator, trace::{Span, Tracer}, KeyValue};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{propagation::{BaggagePropagator, TraceContextPropagator}, trace::SdkTracerProvider, Resource};
use rdkafka::{ClientConfig, producer::FutureProducer};
use thiserror::Error;
use tokio::sync::mpsc;

mod packet;

#[tokio::main]
async fn main() {
    env_logger::init();
    
    init_telemetry().await.unwrap();
    let tracer = global::tracer("f1-telemetry-producer");

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap();
    let lap_topic = std::env::var("LAP_TOPIC").unwrap();
    let participants_topic = std::env::var("PARTICIPANTS_TOPIC").unwrap();
    let lobby_info_topic = std::env::var("LOBBY_INFO_TOPIC").unwrap();
    let car_telemetry_topic = std::env::var("CAR_TELEMETRY_TOPIC").unwrap();

    let udp_port = std::env::var("UDP_PORT").unwrap();

    let channel_size: usize = std::env::var("CHANNEL_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, String, String, String)>(channel_size);

    tokio::spawn(async move {
        while let Some((topic, key, payload, traceparent)) = rx.recv().await {
            let record = rdkafka::producer::FutureRecord::to(&topic)
                .key(&key)
                .payload(&payload)
                .headers(rdkafka::message::OwnedHeaders::new().insert(rdkafka::message::Header {
                    key: "traceparent",
                    value: Some(&traceparent),
                }));

            if let Err(e) = producer.send(record, Duration::from_secs(0)).await {
                log::error!("failed to send message to Kafka: {:?}", e);
            }
        }
    });

    let socket = UdpSocket::bind(&format!("0.0.0.0:{}", udp_port)).unwrap();

    log::info!("recv from: 0.0.0.0:{}", udp_port);
    log::info!(
        "publishing to: {}, [{}, {}, {}, {}]",
        &bootstrap_servers,
        &car_telemetry_topic,
        &lap_topic,
        &lobby_info_topic,
        &participants_topic
    );

    loop {
        let mut buf = [0u8; 2048];
        let (amt, _src) = socket.recv_from(&mut buf).unwrap();

        log::debug!("recv {} bytes", amt);
        let header: &packet::header::PacketHeader = match bytemuck::try_from_bytes(
            &buf[..std::mem::size_of::<packet::header::PacketHeader>()],
        ) {
            Ok(header) => header,
            Err(e) => {
                log::error!("failed to parse packet header: {:?}", e);
                continue;
            }
        };

        match packet::header::PacketId::try_from(header.m_packet_id) {
            Ok(packet::header::PacketId::CarDamage) => {
                if amt >= std::mem::size_of::<packet::payload::car_damage::PacketCarDamage>() {
                    match bytemuck::try_from_bytes::<packet::payload::car_damage::PacketCarDamage>(
                        &buf[..std::mem::size_of::<packet::payload::car_damage::PacketCarDamage>()],
                    ) {
                        Ok(_damage) => {
                            log::debug!("car damage packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse car damage packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::CarSetups) => {
                if amt >= std::mem::size_of::<packet::payload::car_setups::PacketCarSetups>() {
                    match bytemuck::try_from_bytes::<packet::payload::car_setups::PacketCarSetups>(
                        &buf[..std::mem::size_of::<packet::payload::car_setups::PacketCarSetups>()],
                    ) {
                        Ok(_car_setups) => {
                            log::debug!("car setups packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse car setups packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::CarStatus) => {
                if amt >= std::mem::size_of::<packet::payload::car_status::PacketCarStatus>() {
                    match bytemuck::try_from_bytes::<packet::payload::car_status::PacketCarStatus>(
                        &buf[..std::mem::size_of::<packet::payload::car_status::PacketCarStatus>()],
                    ) {
                        Ok(_car_status) => {
                            log::debug!("car status packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse car status packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::CarTelemetry) => {
                if amt >= std::mem::size_of::<packet::payload::car_telemetry::PacketCarTelemetry>()
                {
                    match bytemuck::try_from_bytes::<
                        packet::payload::car_telemetry::PacketCarTelemetry,
                    >(
                        &buf[..std::mem::size_of::<
                            packet::payload::car_telemetry::PacketCarTelemetry,
                        >()],
                    ) {
                        Ok(telemetry_packet) => {
                            log::debug!("car telemetry packet received");
                            let session_uid = telemetry_packet.m_header.m_session_uid;
                            let frame_identifier = telemetry_packet.m_header.m_frame_identifier;
                            
                            let mut span = tracer.start("process_car_telemetry_packet");
                            span.set_attribute(KeyValue::new("packet_id", header.m_packet_id.to_string()));
                            span.set_attribute(KeyValue::new("session_uid", session_uid.to_string()));
                            span.set_attribute(KeyValue::new("frame_identifier", frame_identifier.to_string()));
                            span.set_attribute(KeyValue::new("packet_type", "car_telemetry"));

                            let span_context = span.span_context();
                            let trace_id = span_context.trace_id().to_string();
                            let span_id = span_context.span_id().to_string();
                            let traceparent = format!("00-{}-{}-01", trace_id, span_id);

                            span.add_event(
                                "processing car telemetry packet",
                                vec![KeyValue::new("log.level", "INFO")]
                            );

                            match serde_json::to_string(telemetry_packet) {
                                Ok(json_data) => {
                                    let session_uid = telemetry_packet.m_header.m_session_uid;
                                    let key = format!("session_{}", session_uid);

                                    if let Err(e) =
                                        tx.try_send((car_telemetry_topic.clone(), key, json_data, traceparent.clone()))
                                    {
                                        log::error!(
                                            "failed to queue car telemetry data for Kafka: {:?}",
                                            e
                                        );
                                        span.add_event(
                                            "failed to queue car telemetry for Kafka",
                                            vec![KeyValue::new("log.level", "ERROR")]
                                        );
                                    } else {
                                        span.add_event(
                                            "successfully queued car telemetry for Kafka",
                                            vec![KeyValue::new("log.level", "INFO")]
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!("failed to serialize car telemetry data: {:?}", e);
                                    span.add_event(
                                            "failed to serialize car telemetry data",
                                            vec![KeyValue::new("log.level", "ERROR")]
                                    );
                                }
                            }

                            span.end();
                        }
                        Err(e) => {
                            log::error!("failed to parse car telemetry packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::Event) => {
                if amt >= std::mem::size_of::<packet::payload::event::PacketEvent>() {
                    match bytemuck::try_from_bytes::<packet::payload::event::PacketEvent>(
                        &buf[..std::mem::size_of::<packet::payload::event::PacketEvent>()],
                    ) {
                        Ok(_event) => {
                            log::debug!("event packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse event packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::FinalClassification) => {
                if amt
                    >= std::mem::size_of::<
                        packet::payload::final_classification::PacketFinalClassification,
                    >()
                {
                    match bytemuck::try_from_bytes::<
                        packet::payload::final_classification::PacketFinalClassification,
                    >(
                        &buf[..std::mem::size_of::<
                            packet::payload::final_classification::PacketFinalClassification,
                        >()],
                    ) {
                        Ok(_final_classification) => {
                            log::debug!("final classification packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse final classification packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::Lap) => {
                if amt >= std::mem::size_of::<packet::payload::lap::PacketLap>() {
                    match bytemuck::try_from_bytes::<packet::payload::lap::PacketLap>(
                        &buf[..std::mem::size_of::<packet::payload::lap::PacketLap>()],
                    ) {
                        Ok(lap_packet) => {
                            log::debug!("lap data packet received");
                            let session_uid = lap_packet.m_header.m_session_uid;
                            let frame_identifier = lap_packet.m_header.m_frame_identifier;

                            let mut span = tracer.start("process_lap_packet");
                            span.set_attribute(KeyValue::new("packet_id", header.m_packet_id.to_string()));
                            span.set_attribute(KeyValue::new("session_uid", session_uid.to_string()));
                            span.set_attribute(KeyValue::new("frame_identifier", frame_identifier.to_string()));
                            span.set_attribute(KeyValue::new("packet_type", "lap"));

                            // Extract OpenTelemetry trace context for W3C traceparent header
                            let span_context = span.span_context();
                            let trace_id = span_context.trace_id().to_string();
                            let span_id = span_context.span_id().to_string();
                            let traceparent = format!("00-{}-{}-01", trace_id, span_id);

                            span.add_event(
                                "processing lap packet",
                                vec![KeyValue::new("log.level", "INFO")]
                            );

                            match serde_json::to_string(lap_packet) {
                                Ok(json_data) => {
                                    let session_uid = lap_packet.m_header.m_session_uid;
                                    let key = format!("session_{}", session_uid);

                                    if let Err(e) = tx.try_send((lap_topic.clone(), key, json_data, traceparent.clone()))
                                    {
                                        log::error!("failed to queue lap data for Kafka: {:?}", e);
                                        span.add_event(
                                            "failed to queue lap data for Kafka",
                                            vec![KeyValue::new("log.level", "ERROR")]
                                        );
                                    } else {
                                        span.add_event(
                                            "successfully queued lap data for Kafka",
                                            vec![KeyValue::new("log.level", "INFO")]
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!("failed to serialize lap data: {:?}", e);
                                    span.add_event(
                                        "failed to serialize lap data",
                                        vec![KeyValue::new("log.level", "ERROR")]
                                    );
                                }
                            }

                            span.end();
                        }
                        Err(e) => {
                            log::error!("failed to parse lap packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::LapPositions) => {
                if amt >= std::mem::size_of::<packet::payload::lap_positions::PacketLapPositions>()
                {
                    match bytemuck::try_from_bytes::<
                        packet::payload::lap_positions::PacketLapPositions,
                    >(
                        &buf[..std::mem::size_of::<
                            packet::payload::lap_positions::PacketLapPositions,
                        >()],
                    ) {
                        Ok(_lap_positions) => {
                            log::debug!("lap positions packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse lap positions packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::LobbyInfo) => {
                if amt >= std::mem::size_of::<packet::payload::lobby_info::PacketLobbyInfo>() {
                    match bytemuck::try_from_bytes::<packet::payload::lobby_info::PacketLobbyInfo>(
                        &buf[..std::mem::size_of::<packet::payload::lobby_info::PacketLobbyInfo>()],
                    ) {
                        Ok(lobby_info_packet) => {
                            log::debug!("lobby info packet received");
                            let session_uid = lobby_info_packet.m_header.m_session_uid;
                            let frame_identifier = lobby_info_packet.m_header.m_frame_identifier;

                            let mut span = tracer.start("process_lobby_info_packet");
                            span.set_attribute(KeyValue::new("packet_id", header.m_packet_id.to_string()));
                            span.set_attribute(KeyValue::new("session_uid", session_uid.to_string()));
                            span.set_attribute(KeyValue::new("frame_identifier", frame_identifier.to_string()));
                            span.set_attribute(KeyValue::new("packet_type", "lobby_info"));

                            // Extract OpenTelemetry trace context for W3C traceparent header
                            let span_context = span.span_context();
                            let trace_id = span_context.trace_id().to_string();
                            let span_id = span_context.span_id().to_string();
                            let traceparent = format!("00-{}-{}-01", trace_id, span_id);

                            span.add_event(
                                "processing lobby info packet",
                                vec![KeyValue::new("log.level", "INFO")]
                            );

                            match serde_json::to_string(lobby_info_packet) {
                                Ok(json_data) => {
                                    let session_uid = lobby_info_packet.m_header.m_session_uid;
                                    let key = format!("session_{}", session_uid);

                                    if let Err(e) =
                                        tx.try_send((lobby_info_topic.clone(), key, json_data, traceparent.clone()))
                                    {
                                        log::error!(
                                            "failed to queue lobby info data for Kafka: {:?}",
                                            e
                                        );
                                        span.add_event(
                                            "failed to queue lobby info data for Kafka",
                                            vec![KeyValue::new("log.level", "ERROR")]
                                        );
                                    } else {
                                        span.add_event(
                                            "successfully queued lobby info data for Kafka",
                                            vec![KeyValue::new("log.level", "INFO")]
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!("failed to serialize lobby info data: {:?}", e);
                                    span.add_event(
                                        "failed to serialize lobby info data",
                                        vec![KeyValue::new("log.level", "ERROR")]
                                    );
                                }
                            }
                            
                            span.end();
                        }
                        Err(e) => {
                            log::error!("failed to parse lobby info packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::Motion) => {
                if amt >= std::mem::size_of::<packet::payload::motion::PacketMotion>() {
                    match bytemuck::try_from_bytes::<packet::payload::motion::PacketMotion>(
                        &buf[..std::mem::size_of::<packet::payload::motion::PacketMotion>()],
                    ) {
                        Ok(_motion) => {
                            log::debug!("motion packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse motion packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::MotionEx) => {
                if amt >= std::mem::size_of::<packet::payload::motion_ex::PacketMotionEx>() {
                    match bytemuck::try_from_bytes::<packet::payload::motion_ex::PacketMotionEx>(
                        &buf[..std::mem::size_of::<packet::payload::motion_ex::PacketMotionEx>()],
                    ) {
                        Ok(_motion_ex) => {
                            log::debug!("motion Ex packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse motion Ex packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::Participants) => {
                if amt >= std::mem::size_of::<packet::payload::participants::PacketParticipants>() {
                    match bytemuck::try_from_bytes::<
                        packet::payload::participants::PacketParticipants,
                    >(
                        &buf[..std::mem::size_of::<
                            packet::payload::participants::PacketParticipants,
                        >()],
                    ) {
                        Ok(participants_packet) => {
                            let session_uid = participants_packet.m_header.m_session_uid;
                            let frame_identifier = participants_packet.m_header.m_frame_identifier;

                            let mut span = tracer.start("process_participants_packet");
                            span.set_attribute(KeyValue::new("packet_id", header.m_packet_id.to_string()));
                            span.set_attribute(KeyValue::new("session_uid", session_uid.to_string()));
                            span.set_attribute(KeyValue::new("frame_identifier", frame_identifier.to_string()));
                            span.set_attribute(KeyValue::new("packet_type", "participants"));

                            // Extract OpenTelemetry trace context for W3C traceparent header
                            let span_context = span.span_context();
                            let trace_id = span_context.trace_id().to_string();
                            let span_id = span_context.span_id().to_string();
                            let traceparent = format!("00-{}-{}-01", trace_id, span_id);

                            span.add_event(
                                "processing participants packet",
                                vec![KeyValue::new("log.level", "INFO")]
                            );

                            match serde_json::to_string(participants_packet) {
                                Ok(json_data) => {
                                    let session_uid = participants_packet.m_header.m_session_uid;
                                    let key = format!("session_{}", session_uid);

                                    if let Err(e) =
                                        tx.try_send((participants_topic.clone(), key, json_data, traceparent.clone()))
                                    {
                                        log::error!(
                                            "failed to queue participants data for Kafka: {:?}",
                                            e
                                        );
                                        span.add_event(
                                            "failed to queue participants data for Kafka",
                                            vec![KeyValue::new("log.level", "ERROR")]
                                        );
                                    } else {
                                        span.add_event(
                                            "successfully queued participants data for Kafka",
                                            vec![KeyValue::new("log.level", "INFO")]
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!("failed to serialize participants data: {:?}", e);
                                    span.add_event(
                                        "failed to serialize participants data",
                                        vec![KeyValue::new("log.level", "ERROR")]
                                    );
                                }
                            }

                            span.end();
                        }
                        Err(e) => {
                            log::error!("failed to parse participants packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::Session) => {
                if amt >= std::mem::size_of::<packet::payload::session::PacketSession>() {
                    match bytemuck::try_from_bytes::<packet::payload::session::PacketSession>(
                        &buf[..std::mem::size_of::<packet::payload::session::PacketSession>()],
                    ) {
                        Ok(_session) => {
                            log::debug!("session packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse session packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::SessionHistory) => {
                if amt
                    >= std::mem::size_of::<packet::payload::session_history::PacketSessionHistory>()
                {
                    match bytemuck::try_from_bytes::<
                        packet::payload::session_history::PacketSessionHistory,
                    >(
                        &buf[..std::mem::size_of::<
                            packet::payload::session_history::PacketSessionHistory,
                        >()],
                    ) {
                        Ok(_session_history) => {
                            log::debug!("session history packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse session history packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::TimeTrial) => {
                if amt >= std::mem::size_of::<packet::payload::time_trial::PacketTimeTrial>() {
                    match bytemuck::try_from_bytes::<packet::payload::time_trial::PacketTimeTrial>(
                        &buf[..std::mem::size_of::<packet::payload::time_trial::PacketTimeTrial>()],
                    ) {
                        Ok(_time_trial) => {
                            log::debug!("time trial packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse time trial packet: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::TyreSets) => {
                if amt >= std::mem::size_of::<packet::payload::tyre_sets::PacketTyreSets>() {
                    match bytemuck::try_from_bytes::<packet::payload::tyre_sets::PacketTyreSets>(
                        &buf[..std::mem::size_of::<packet::payload::tyre_sets::PacketTyreSets>()],
                    ) {
                        Ok(_tyre_sets) => {
                            log::debug!("tyre sets packet received");
                        }
                        Err(e) => {
                            log::error!("failed to parse tyre sets packet: {:?}", e);
                        }
                    }
                }
            }
            Err(_) => {
                log::error!("unknown packet id: {}", header.m_packet_id);
            }
        }
    }
}

async fn init_telemetry() -> Result<(), InitTelemetryError> {
    let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .map_err(|e| InitTelemetryError::OTELConfig(e.to_string()))?;
    
    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", "f1-telemetry-producer"))
        .with_attribute(KeyValue::new("telemetry.sdk.language", "rust"))
        .build();

    let otlp_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| InitTelemetryError::ExporterBuild(e.to_string()))?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(otlp_exporter)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    Ok(())
}

#[derive(Error, Debug)]
enum InitTelemetryError {
    #[error("failed to get otel config: {0}")]
    OTELConfig(String),
    #[error("failed to build otlp exporter: {0}")]
    ExporterBuild(String),
}
