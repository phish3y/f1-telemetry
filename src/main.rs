use std::{net::UdpSocket, time::Duration};

mod packet;

fn main() {
    env_logger::init();

    let ip = "0.0.0.0";
    let port = "20777";
    let socket = UdpSocket::bind(format!("{}:{}", ip, port)).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(300))).unwrap();
    log::info!("recv from: {}:{}", ip, port);

    loop {
        let mut buf = [0u8; 2048];
        let (amt, _src) = socket.recv_from(&mut buf).unwrap();

        log::info!("recv {} bytes", amt);
        let header: &packet::header::PacketHeader = bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::header::PacketHeader>()]);

        match packet::header::PacketId::try_from(header.m_packet_id) {
            Ok(packet::header::PacketId::CarDamage) => {
                if amt >= std::mem::size_of::<packet::payload::car_damage::PacketCarDamage>() {
                    let _damage: &packet::payload::car_damage::PacketCarDamage =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::car_damage::PacketCarDamage>()]);
                    log::info!("Car damage packet received");
                }
            }
            Ok(packet::header::PacketId::CarSetups) => {
                if amt >= std::mem::size_of::<packet::payload::car_setups::PacketCarSetups>() {
                    let _car_setups: &packet::payload::car_setups::PacketCarSetups =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::car_setups::PacketCarSetups>()]);
                    log::info!("Car setups packet received");
                }
            }
            Ok(packet::header::PacketId::CarStatus) => {
                if amt >= std::mem::size_of::<packet::payload::car_status::PacketCarStatus>() {
                    let _car_status: &packet::payload::car_status::PacketCarStatus =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::car_status::PacketCarStatus>()]);
                    log::info!("Car status packet received");
                }
            }
            Ok(packet::header::PacketId::CarTelemetry) => {
                if amt >= std::mem::size_of::<packet::payload::car_telemetry::PacketCarTelemetry>() {
                    let _telemetry: &packet::payload::car_telemetry::PacketCarTelemetry =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::car_telemetry::PacketCarTelemetry>()]);
                    log::info!("Car telemetry packet received");
                }
            }
            Ok(packet::header::PacketId::Event) => {
                if amt >= std::mem::size_of::<packet::payload::event::PacketEvent>() {
                    let _event: &packet::payload::event::PacketEvent =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::event::PacketEvent>()]);
                    log::info!("Event packet received");
                }
            }
            Ok(packet::header::PacketId::FinalClassification) => {
                if amt >= std::mem::size_of::<packet::payload::final_classification::PacketFinalClassification>() {
                    let _final_classification: &packet::payload::final_classification::PacketFinalClassification =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::final_classification::PacketFinalClassification>()]);
                    log::info!("Final classification packet received");
                }
            }
            Ok(packet::header::PacketId::LapData) => {
                if amt >= std::mem::size_of::<packet::payload::lap::PacketLap>() {
                    let _lap: &packet::payload::lap::PacketLap =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::lap::PacketLap>()]);
                    log::info!("Lap data packet received");
                }
            }
            Ok(packet::header::PacketId::LapHistory) => {
                log::info!("Lap history packet received (placeholder - no struct defined)");
            }
            Ok(packet::header::PacketId::LapPositions) => {
                if amt >= std::mem::size_of::<packet::payload::lap_positions::PacketLapPositions>() {
                    let _lap_positions: &packet::payload::lap_positions::PacketLapPositions =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::lap_positions::PacketLapPositions>()]);
                    log::info!("Lap positions packet received");
                }
            }
            Ok(packet::header::PacketId::LobbyInfo) => {
                if amt >= std::mem::size_of::<packet::payload::lobby_info::PacketLobbyInfo>() {
                    let _lobby_info: &packet::payload::lobby_info::PacketLobbyInfo =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::lobby_info::PacketLobbyInfo>()]);
                    log::info!("Lobby info packet received");
                }
            }
            Ok(packet::header::PacketId::Motion) => {
                if amt >= std::mem::size_of::<packet::payload::motion::PacketMotion>() {
                    let motion: &packet::payload::motion::PacketMotion =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::motion::PacketMotion>()]);
                    log::info!("Motion packet received");
                }
            }
            Ok(packet::header::PacketId::MotionEx) => {
                if amt >= std::mem::size_of::<packet::payload::motion_ex::PacketMotionEx>() {
                    let _motion_ex: &packet::payload::motion_ex::PacketMotionEx =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::motion_ex::PacketMotionEx>()]);
                    log::info!("Motion Ex packet received");
                }
            }
            Ok(packet::header::PacketId::Participants) => {
                if amt >= std::mem::size_of::<packet::payload::participants::PacketParticipants>() {
                    let _participants: &packet::payload::participants::PacketParticipants =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::participants::PacketParticipants>()]);
                    log::info!("Participants packet received");
                }
            }
            Ok(packet::header::PacketId::Session) => {
                if amt >= std::mem::size_of::<packet::payload::session::PacketSession>() {
                    let _session: &packet::payload::session::PacketSession =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::session::PacketSession>()]);
                    log::info!("Session packet received");
                }
            }
            Ok(packet::header::PacketId::SessionHistory) => {
                if amt >= std::mem::size_of::<packet::payload::session_history::PacketSessionHistory>() {
                    let _session_history: &packet::payload::session_history::PacketSessionHistory =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::session_history::PacketSessionHistory>()]);
                    log::info!("Session history packet received");
                }
            }
            Ok(packet::header::PacketId::TimeTrial) => {
                if amt >= std::mem::size_of::<packet::payload::time_trial::PacketTimeTrial>() {
                    let _time_trial: &packet::payload::time_trial::PacketTimeTrial =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::time_trial::PacketTimeTrial>()]);
                    log::info!("Time trial packet received");
                }
            }
            Ok(packet::header::PacketId::TyreSets) => {
                if amt >= std::mem::size_of::<packet::payload::tyre_sets::PacketTyreSets>() {
                    let _tyre_sets: &packet::payload::tyre_sets::PacketTyreSets =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<packet::payload::tyre_sets::PacketTyreSets>()]);
                    log::info!("Tyre sets packet received");
                }
            }
            Err(_) => {
                log::error!("unknown packet id: {}", header.m_packet_id);
            }
        }
    }
}
