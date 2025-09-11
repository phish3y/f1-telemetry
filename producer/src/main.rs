use std::{net::UdpSocket, time::Duration};

use rdkafka::{ClientConfig, producer::FutureProducer};
use tokio::sync::mpsc;

mod packet;

#[tokio::main]
async fn main() {
    env_logger::init();

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap();
    let lap_topic = std::env::var("LAP_TOPIC").unwrap();
    let car_telemetry_topic = std::env::var("CAR_TELEMETRY_TOPIC").unwrap();

    let udp_url = std::env::var("UDP_URL").unwrap();
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

    let (tx, mut rx) = mpsc::channel::<(String, String, String)>(channel_size);

    tokio::spawn(async move {
        while let Some((topic, key, payload)) = rx.recv().await {
            let record = rdkafka::producer::FutureRecord::to(&topic)
                .key(&key)
                .payload(&payload);

            if let Err(e) = producer.send(record, Duration::from_secs(0)).await {
                log::error!("failed to send message to Kafka: {:?}", e);
            }
        }
    });

    let socket = UdpSocket::bind(&format!("{}:{}", udp_url, udp_port)).unwrap();

    log::info!("recv from: {}:{}", udp_url, udp_port);
    log::info!(
        "publishing to: {}, {}",
        &bootstrap_servers,
        &car_telemetry_topic
    );

    loop {
        let mut buf = [0u8; 2048];
        let (amt, _src) = socket.recv_from(&mut buf).unwrap();

        log::debug!("recv {} bytes", amt);
        let header: &packet::header::PacketHeader =
            bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::header::PacketHeader>()]);

        match packet::header::PacketId::try_from(header.m_packet_id) {
            Ok(packet::header::PacketId::CarDamage) => {
                if amt >= std::mem::size_of::<packet::payload::car_damage::PacketCarDamage>() {
                    let _damage: &packet::payload::car_damage::PacketCarDamage =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::car_damage::PacketCarDamage>(
                            )],
                        );
                    log::debug!("car damage packet received");
                }
            }
            Ok(packet::header::PacketId::CarSetups) => {
                if amt >= std::mem::size_of::<packet::payload::car_setups::PacketCarSetups>() {
                    let _car_setups: &packet::payload::car_setups::PacketCarSetups =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::car_setups::PacketCarSetups>(
                            )],
                        );
                    log::debug!("car setups packet received");
                }
            }
            Ok(packet::header::PacketId::CarStatus) => {
                if amt >= std::mem::size_of::<packet::payload::car_status::PacketCarStatus>() {
                    let _car_status: &packet::payload::car_status::PacketCarStatus =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::car_status::PacketCarStatus>(
                            )],
                        );
                    log::debug!("car status packet received");
                }
            }
            Ok(packet::header::PacketId::CarTelemetry) => {
                if amt >= std::mem::size_of::<packet::payload::car_telemetry::PacketCarTelemetry>()
                {
                    let telemetry_packet: &packet::payload::car_telemetry::PacketCarTelemetry =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<
                                packet::payload::car_telemetry::PacketCarTelemetry,
                            >()],
                        );
                    log::debug!("car telemetry packet received");

                    match serde_json::to_string(telemetry_packet) {
                        Ok(json_data) => {
                            let session_uid = telemetry_packet.m_header.m_session_uid;
                            let key = format!("session_{}", session_uid);

                            if let Err(e) =
                                tx.try_send((car_telemetry_topic.clone(), key, json_data))
                            {
                                log::error!(
                                    "failed to queue car telemetry data for Kafka: {:?}",
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            log::error!("failed to serialize car telemetry data: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::Event) => {
                if amt >= std::mem::size_of::<packet::payload::event::PacketEvent>() {
                    let _event: &packet::payload::event::PacketEvent = bytemuck::from_bytes(
                        &buf[..std::mem::size_of::<packet::payload::event::PacketEvent>()],
                    );
                    log::debug!("event packet received");
                }
            }
            Ok(packet::header::PacketId::FinalClassification) => {
                if amt
                    >= std::mem::size_of::<
                        packet::payload::final_classification::PacketFinalClassification,
                    >()
                {
                    let _final_classification: &packet::payload::final_classification::PacketFinalClassification =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::final_classification::PacketFinalClassification>()]);
                    log::debug!("final classification packet received");
                }
            }
            Ok(packet::header::PacketId::Lap) => {
                if amt >= std::mem::size_of::<packet::payload::lap::PacketLap>() {
                    let lap_packet: &packet::payload::lap::PacketLap = bytemuck::from_bytes(
                        &buf[..std::mem::size_of::<packet::payload::lap::PacketLap>()],
                    );
                    log::debug!("lap data packet received");

                    match serde_json::to_string(lap_packet) {
                        Ok(json_data) => {
                            let session_uid = lap_packet.m_header.m_session_uid;
                            let key = format!("session_{}", session_uid);

                            if let Err(e) = tx.try_send((lap_topic.clone(), key, json_data)) {
                                log::error!("failed to queue lap data for Kafka: {:?}", e);
                            }
                        }
                        Err(e) => {
                            log::error!("failed to serialize lap data: {:?}", e);
                        }
                    }
                }
            }
            Ok(packet::header::PacketId::LapPositions) => {
                if amt >= std::mem::size_of::<packet::payload::lap_positions::PacketLapPositions>()
                {
                    let _lap_positions: &packet::payload::lap_positions::PacketLapPositions =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<
                                packet::payload::lap_positions::PacketLapPositions,
                            >()],
                        );
                    log::debug!("lap positions packet received");
                }
            }
            Ok(packet::header::PacketId::LobbyInfo) => {
                if amt >= std::mem::size_of::<packet::payload::lobby_info::PacketLobbyInfo>() {
                    let _lobby_info: &packet::payload::lobby_info::PacketLobbyInfo =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::lobby_info::PacketLobbyInfo>(
                            )],
                        );
                    log::debug!("lobby info packet received");
                }
            }
            Ok(packet::header::PacketId::Motion) => {
                if amt >= std::mem::size_of::<packet::payload::motion::PacketMotion>() {
                    let _motion: &packet::payload::motion::PacketMotion = bytemuck::from_bytes(
                        &buf[..std::mem::size_of::<packet::payload::motion::PacketMotion>()],
                    );
                    log::debug!("motion packet received");
                }
            }
            Ok(packet::header::PacketId::MotionEx) => {
                if amt >= std::mem::size_of::<packet::payload::motion_ex::PacketMotionEx>() {
                    let _motion_ex: &packet::payload::motion_ex::PacketMotionEx =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::motion_ex::PacketMotionEx>(
                            )],
                        );
                    log::debug!("motion Ex packet received");
                }
            }
            Ok(packet::header::PacketId::Participants) => {
                if amt >= std::mem::size_of::<packet::payload::participants::PacketParticipants>() {
                    let _participants: &packet::payload::participants::PacketParticipants =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<
                                packet::payload::participants::PacketParticipants,
                            >()],
                        );
                    log::debug!("participants packet received");
                }
            }
            Ok(packet::header::PacketId::Session) => {
                if amt >= std::mem::size_of::<packet::payload::session::PacketSession>() {
                    let _session: &packet::payload::session::PacketSession = bytemuck::from_bytes(
                        &buf[..std::mem::size_of::<packet::payload::session::PacketSession>()],
                    );
                    log::debug!("session packet received");
                }
            }
            Ok(packet::header::PacketId::SessionHistory) => {
                if amt
                    >= std::mem::size_of::<packet::payload::session_history::PacketSessionHistory>()
                {
                    let _session_history: &packet::payload::session_history::PacketSessionHistory =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<
                                packet::payload::session_history::PacketSessionHistory,
                            >()],
                        );
                    log::debug!("session history packet received");
                }
            }
            Ok(packet::header::PacketId::TimeTrial) => {
                if amt >= std::mem::size_of::<packet::payload::time_trial::PacketTimeTrial>() {
                    let _time_trial: &packet::payload::time_trial::PacketTimeTrial =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::time_trial::PacketTimeTrial>(
                            )],
                        );
                    log::debug!("time trial packet received");
                }
            }
            Ok(packet::header::PacketId::TyreSets) => {
                if amt >= std::mem::size_of::<packet::payload::tyre_sets::PacketTyreSets>() {
                    let _tyre_sets: &packet::payload::tyre_sets::PacketTyreSets =
                        bytemuck::from_bytes(
                            &buf[..std::mem::size_of::<packet::payload::tyre_sets::PacketTyreSets>(
                            )],
                        );
                    log::debug!("tyre sets packet received");
                }
            }
            Err(_) => {
                log::error!("unknown packet id: {}", header.m_packet_id);
            }
        }
    }
}
