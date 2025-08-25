use std::{net::UdpSocket, time::Duration};

use f1_telemetry::packet::{header, motion, session};

fn main() {
    env_logger::init();

    let ip = "0.0.0.0";
    let port = "20777";
    let socket = UdpSocket::bind(format!("{}:{}", ip, port)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(300)))
        .unwrap();
    log::info!("recv udp from: {}:{}", ip, port);

    loop {
        let mut buf = [0u8; 2048];
        let (amt, _src) = socket.recv_from(&mut buf).unwrap();

        log::info!("recv {} bytes", amt);
        let header: &header::PacketHeader =
            bytemuck::from_bytes(&buf[..std::mem::size_of::<header::PacketHeader>()]);

        match header::PacketId::try_from(header.m_packet_id) {
            Ok(header::PacketId::Motion) => {
                if amt >= std::mem::size_of::<motion::PacketMotion>() {
                    let motion: &motion::PacketMotion = bytemuck::from_bytes(
                        &buf[..std::mem::size_of::<motion::PacketMotion>()],
                    );
                    let player_index = motion.m_header.m_player_car_index as usize;
                    let player_car_motion = &motion.m_car_motion_data[player_index];

                    let pos_x = player_car_motion.m_world_position_x;
                    let pos_y = player_car_motion.m_world_position_y;
                    let pos_z = player_car_motion.m_world_position_z;

                    log::info!("Motion: ({},{},{})", pos_x, pos_y, pos_z)
                }
            }
            Err(_) => {
                log::error!("failed to parse packet id: {}", header.m_packet_id)
            }
            _ => {}
        }
    }
}
