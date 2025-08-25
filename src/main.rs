use std::{net::UdpSocket, time::Duration};

use bytemuck::{Pod, Zeroable};

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketHeader {
    pub m_packet_format: u16,
    pub m_game_year: u8,
    pub m_game_major_version: u8,
    pub m_game_minor_version: u8,
    pub m_packet_version: u8,
    pub m_packet_id: u8,
    pub m_session_uid: u64,
    pub m_session_time: f32,
    pub m_frame_identifier: u32,
    pub m_overall_frame_identifier: u32,
    pub m_player_car_index: u8,
    pub m_secondary_player_car_index: u8,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum PacketId {
    Motion = 0,
    Session = 1,
    LapData = 2,
    Event = 3,
    Participants = 4,
    CarSetups = 5,
    CarTelemetry = 6,
    CarStatus = 7,
    FinalClassification = 8,
    LobbyInfo = 9,
    CarDamage = 10,
    SessionHistory = 11,
    TyreSets = 12,
    MotionEx = 13,
    TimeTrial = 14,
    LapHistory = 15,
    LapPositions = 16,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct CarMotionData {
    pub m_world_position_x: f32,
    pub m_world_position_y: f32,
    pub m_world_position_z: f32,

    pub m_world_velocity_x: f32,
    pub m_world_velocity_y: f32,
    pub m_world_velocity_z: f32,

    pub m_world_forward_dir_x: i16,
    pub m_world_forward_dir_y: i16,
    pub m_world_forward_dir_z: i16,

    pub m_world_right_dir_x: i16,
    pub m_world_right_dir_y: i16,
    pub m_world_right_dir_z: i16,

    pub m_g_force_lateral: f32,
    pub m_g_force_longitudinal: f32,
    pub m_g_force_vertical: f32,

    pub m_yaw: f32,
    pub m_pitch: f32,
    pub m_roll: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketMotionData {
    pub m_header: PacketHeader,
    pub m_car_motion_data: [CarMotionData; 22],
}

fn main() {
    env_logger::init();

    let socket = UdpSocket::bind("0.0.0.0:20777").unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(300))).unwrap();

    loop {
        let mut buf = [0u8; 2048];
        let (amt, _src) = socket.recv_from(&mut buf).unwrap();

        log::info!("recv {} bytes", amt);
        let header: &PacketHeader = bytemuck::from_bytes(&buf[..std::mem::size_of::<PacketHeader>()]);

        match header.m_packet_id {
            0 => {
                if amt >= std::mem::size_of::<PacketMotionData>() {
                    let motion: &PacketMotionData =
                        bytemuck::from_bytes(&buf[..std::mem::size_of::<PacketMotionData>()]);
                    let player_index = motion.m_header.m_player_car_index as usize;
                    let player_car_motion = &motion.m_car_motion_data[player_index];

                    let pos_x = player_car_motion.m_world_position_x;
                    let pos_y = player_car_motion.m_world_position_y;
                    let pos_z = player_car_motion.m_world_position_z;

                    log::info!("({},{},{})", pos_x, pos_y, pos_z)
                }
            }
            _ => {}
        }
    }
}
